package gtc_processor

// gtc_processor.go

import (
	"context"
	_ "embed"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/common/parallel"
	"github.com/influxdata/telegraf/plugins/processors"
	"github.com/influxdata/telegraf/selfstat"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PodMap
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type PodMapGetter interface {
	Get(string) (string, DataStatus)
	GetWait(string) (string, DataStatus)
	StartRefreshLoop()
}

type DataStatus int

const (
	OK DataStatus = iota
	ABSENT
	DROP
)

type Data struct {
	Value     string
	Timestamp time.Time
	Status    DataStatus
}

type PodMap struct {
	mutexData sync.RWMutex
	data      map[string]Data

	refreshInterval time.Duration
	cacheDuration   time.Duration

	k8sClient *kubernetes.Clientset

	statRefreshSuccessCount selfstat.Stat
	statRefreshFailCount    selfstat.Stat
	statItemCount           selfstat.Stat
	statQueryErrors         selfstat.Stat

	log telegraf.Logger
}

func NewPodMap(refreshInterval int, cacheDuration int, log telegraf.Logger) *PodMap {
	return &PodMap{
		data:                    make(map[string]Data),
		refreshInterval:         time.Duration(refreshInterval) * time.Minute,
		cacheDuration:           time.Duration(cacheDuration) * time.Minute,
		k8sClient:               getK8sClient(),
		statRefreshSuccessCount: selfstat.Register("gtc_processor", "refresh_count", map[string]string{"result": "success"}),
		statRefreshFailCount:    selfstat.Register("gtc_processor", "refresh_count", map[string]string{"result": "fail"}),
		statItemCount:           selfstat.Register("gtc_processor", "cache_size", map[string]string{}),
		statQueryErrors:         selfstat.Register("gtc_processor", "query_errors", map[string]string{}),
		log:                     log,
	}
}

func (tm *PodMap) Get(key string) (string, DataStatus) {
	tm.mutexData.RLock()
	defer tm.mutexData.RUnlock()
	data, ok := tm.data[key]
	if !ok {
		return "", ABSENT
	}
	return data.Value, data.Status
}

func (tm *PodMap) GetWait(key string) (string, DataStatus) {
	// try getting value
	if data, status := tm.Get(key); status != ABSENT {
		return data, status
	}

	var service string
	var timestamp time.Time
	var status DataStatus
	namespace, podName := splitPodKey(key)
	pod, err := tm.downloadPod(namespace, podName)
	if err != nil {
		tm.statQueryErrors.Incr(1)
		// We get the following error when a pod no longer exists but it's metrics are still being collected:
		//   err = unable to retrieve pod <pod> in namespace <namespace> from k8s api: pods "<pod>" not found
		tm.log.Errorf("gtc_processor - error getting pod %s from api server: %v", key, err)
		// the metric should be dropped until the next cache refresh occurs
		service = ""
		timestamp = time.Now().Add(-tm.cacheDuration)
		status = DROP
	} else {
		service = createServiceLabel(pod.Annotations)
		timestamp = time.Now()
		status = OK
	}

	tm.mutexData.Lock()
	defer tm.mutexData.Unlock()

	tm.data[key] = Data{service, timestamp, status}
	tm.statItemCount.Set(int64(len(tm.data)))
	tm.log.Infof("gtc_processor - got service label %s for pod %s, pod-service map size = %d", service, key, len(tm.data))

	return service, status
}

func (tm *PodMap) StartRefreshLoop() {
	// blocking, crash if map initialization fails
	if err := tm.populateMap(); err != nil {
		panic(err)
	}
	tm.statRefreshSuccessCount.Incr(1)
	tm.statItemCount.Set(int64(len(tm.data)))
	tm.log.Infof("gtc_processor - initial pod-service map size = %d", len(tm.data))
	// populate every refreshInterval
	// - will automatically remove old pods
	go func() {
		for {
			time.Sleep(tm.refreshInterval)
			// populate
			if err := tm.populateMap(); err != nil {
				tm.statRefreshFailCount.Incr(1)
				tm.log.Warnf("gtc_processor - map refresh failed")
				continue
			}
			tm.statRefreshSuccessCount.Incr(1)
			// delete old pods (older than twice refreshInterval)
			tm.mutexData.Lock()
			for key, data := range tm.data {
				if time.Since(data.Timestamp) > tm.cacheDuration {
					delete(tm.data, key)
				}
			}
			tm.statItemCount.Set(int64(len(tm.data)))
			tm.log.Infof("gtc_processor - after cleanup pod-service map size = %d", len(tm.data))
			tm.mutexData.Unlock()
		}
	}()
}

func (tm *PodMap) populateMap() error {
	pods, err := downloadPods(tm.k8sClient)
	if err != nil {
		tm.statQueryErrors.Incr(1)
		tm.log.Errorf("gtc_processor - error populating pod-service map: %v", err)
		return err
	}

	now := time.Now()
	tmpMap := make(map[string]Data)
	for _, pod := range pods.Items {
		podKey := createPodKey(pod.Namespace, pod.Name)
		tmpMap[podKey] = Data{createServiceLabel(pod.Annotations), now, OK}
	}
	tm.log.Infof("gtc_processor - tmp map size = %d", len(tmpMap))
	if len(tmpMap) <= 0 {
		tm.log.Warn("gtc_processor - created pod-service map is empty")
	}

	tm.mutexData.Lock()
	defer tm.mutexData.Unlock()
	// merge tmpMap into main map (we don't want to delete old pods immediately to account for metric lag)
	for k, v := range tmpMap {
		tm.data[k] = v
	}
	tm.log.Infof("gtc_processor - new pod-service map size = %d", len(tm.data))

	return nil
}

func (tm *PodMap) downloadPod(namespace string, podName string) (*corev1.Pod, error) {
	var pod *corev1.Pod
	var err error

	pod, err = tm.k8sClient.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "connection reset by peer" {
		tm.k8sClient = getK8sClient()
		pod, err = tm.k8sClient.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	}
	if err != nil {
		err = fmt.Errorf("unable to retrieve pod %s in namespace %s from k8s api: %v", podName, namespace, err)
	}

	return pod, err
}

// This function causes program termination if an error occurs
func getK8sClient() *kubernetes.Clientset {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return client
}

func downloadPods(clientset *kubernetes.Clientset) (*corev1.PodList, error) {
	pods, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		err = fmt.Errorf("unable to retrieve pods from k8s api: %v", err)
	}
	return pods, err
}

func createServiceLabel(annotations map[string]string) string {
	service, ok := annotations["prometheus.io/label-service"]
	if !ok {
		if project, ok := annotations["app.tug.jive.com/project"]; ok {
			if app, ok := annotations["app.tug.jive.com/app"]; ok {
				service = project + "/" + app
			}
		}
	}
	return service
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Plugin
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func podKeyFromMetric(metric telegraf.Metric) string {
	if namespace, ok := metric.GetTag("namespace"); ok {
		if pod, ok := metric.GetTag("pod"); ok {
			return createPodKey(namespace, pod)
		}
	}
	return ""
}

func createPodKey(namespace string, pod string) string {
	return fmt.Sprintf("%s/%s", namespace, pod)
}

func splitPodKey(key string) (string, string) {
	splitKey := strings.Split(key, "/")
	return splitKey[0], splitKey[1]
}

type GtcProcessor struct {
	podServiceMap PodMapGetter
	parallel      parallel.Parallel

	LabelName string `toml:"label"`
	// MaxQueued       int    `toml:"max_queued"`
	RefreshInterval int `toml:"refresh_interval"`
	CacheDuration   int `toml:"cache_duration"`

	// statQueueLimit                selfstat.Stat
	statQueueSize                   selfstat.Stat
	statEnqueue                     selfstat.Stat
	statDequeue                     selfstat.Stat
	statMetricReceived              selfstat.Stat
	statMetricProcessedNone         selfstat.Stat
	statMetricProcessedAdded        selfstat.Stat
	statMetricProcessedDropped      selfstat.Stat
	statMetricProcessedNoneAsync    selfstat.Stat
	statMetricProcessedAddedAsync   selfstat.Stat
	statMetricProcessedDroppedAsync selfstat.Stat

	Log telegraf.Logger `toml:"-"`
}

//go:embed sample.conf
var sampleConfig string

func (*GtcProcessor) SampleConfig() string {
	return sampleConfig
}

func (p *GtcProcessor) Init() error {
	// p.statQueueLimit = selfstat.Register("gtc_processor", "queue_limit", map[string]string{})
	p.statQueueSize = selfstat.Register("gtc_processor", "queue_size", map[string]string{})
	p.statEnqueue = selfstat.Register("gtc_processor", "enqueue", map[string]string{})
	p.statDequeue = selfstat.Register("gtc_processor", "dequeue", map[string]string{})
	p.statMetricReceived = selfstat.Register("gtc_processor", "received", map[string]string{})
	p.statMetricProcessedNone = selfstat.Register("gtc_processor", "processed", map[string]string{"exec": "sync", "result": "nolabel"})
	p.statMetricProcessedAdded = selfstat.Register("gtc_processor", "processed", map[string]string{"exec": "sync", "result": "label_added"})
	p.statMetricProcessedDropped = selfstat.Register("gtc_processor", "processed", map[string]string{"exec": "sync", "result": "metric_dropped"})
	p.statMetricProcessedNoneAsync = selfstat.Register("gtc_processor", "processed", map[string]string{"exec": "async", "result": "nolabel"})
	p.statMetricProcessedAddedAsync = selfstat.Register("gtc_processor", "processed", map[string]string{"exec": "async", "result": "label_added"})
	p.statMetricProcessedDroppedAsync = selfstat.Register("gtc_processor", "processed", map[string]string{"exec": "async", "result": "metric_dropped"})

	// p.statQueueLimit.Set(int64(p.MaxQueued))

	return nil
}

// Start initalizes the podServiceMap global map
// we protect it with an if nil because podServiceMap is global and
// Start is called once per instance of the plugin
func (p *GtcProcessor) Start(acc telegraf.Accumulator) error {
	if p.podServiceMap == nil {
		p.podServiceMap = NewPodMap(
			p.RefreshInterval,
			p.CacheDuration,
			p.Log,
		)
		p.podServiceMap.StartRefreshLoop()
		p.Log.Info("gtc_processor - map initialized")
	}
	// Only 1 worker since there's no need to query api server multiple times
	p.parallel = parallel.NewOrdered(acc, p.asyncAdd, 10, 1) // removed p.MaxQueued as value since we block depending on number of workers anyway (10 will never be reached, double workers is max)
	p.Log.Info("gtc_processor - plugin started")
	return nil
}

func (p *GtcProcessor) Add(metric telegraf.Metric, acc telegraf.Accumulator) error {
	p.statMetricReceived.Incr(1)
	statMetricProcessed := p.statMetricProcessedNone

	if podKey := podKeyFromMetric(metric); len(podKey) > 0 {
		if service, status := (PodMapGetter)(p.podServiceMap).Get(podKey); len(service) > 0 {
			metric.RemoveTag(p.LabelName)
			metric.AddTag(p.LabelName, service)
			statMetricProcessed = p.statMetricProcessedAdded
		} else if status == ABSENT {
			p.statEnqueue.Incr(1)
			p.statQueueSize.Incr(1)
			// Add to parallel queue to retrieve 'service' label in background
			p.parallel.Enqueue(metric)
			return nil
		} else if status == DROP {
			p.statMetricProcessedDropped.Incr(1)
			metric.Drop()
			return nil
		}
	}
	acc.AddMetric(metric)
	statMetricProcessed.Incr(1)

	return nil
}

func (p *GtcProcessor) asyncAdd(metric telegraf.Metric) []telegraf.Metric {
	p.statDequeue.Incr(1)
	p.statQueueSize.Incr(-1)
	statMetricProcessed := p.statMetricProcessedNoneAsync

	podKey := podKeyFromMetric(metric)
	if service, status := (PodMapGetter)(p.podServiceMap).GetWait(podKey); len(service) > 0 {
		metric.RemoveTag(p.LabelName)
		metric.AddTag(p.LabelName, service)
		statMetricProcessed = p.statMetricProcessedAddedAsync
		p.Log.Infof("gtc_processor async - label added for %s", podKey)
	} else if status == DROP {
		p.statMetricProcessedDroppedAsync.Incr(1)
		metric.Drop()
		p.Log.Infof("gtc_processor async - metric dropped for %s", podKey)
		return []telegraf.Metric{}
	} else {
		p.Log.Infof("gtc_processor async - label not added for %s", podKey)
	}

	statMetricProcessed.Incr(1)
	return []telegraf.Metric{metric}
}

func (p *GtcProcessor) Stop() {
	p.parallel.Stop()
}

func init() {
	processors.AddStreaming("gtc_processor", func() telegraf.StreamingProcessor {
		return &GtcProcessor{
			LabelName: "service",
			// MaxQueued:       10000,
			RefreshInterval: 1,
			CacheDuration:   30,
		}
	})
}
