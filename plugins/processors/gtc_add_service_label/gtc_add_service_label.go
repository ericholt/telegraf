package gtc_join_service

// gtc_join_service.go

import (
	"context"
	_ "embed"
	"fmt"
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
// PodMap TODO: move to another file
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Data struct {
	Value     string
	Timestamp time.Time
}

type PodMap struct {
	mutexData sync.RWMutex
	data      map[string]Data

	refreshInterval time.Duration

	k8sClient *kubernetes.Clientset

	statRefreshCount selfstat.Stat
	statItemCount    selfstat.Stat
	statQueryErrors  selfstat.Stat

	log telegraf.Logger
}

func NewPodMap(refreshInterval int, log telegraf.Logger) *PodMap {
	return &PodMap{
		data:             make(map[string]Data),
		refreshInterval:  time.Duration(refreshInterval) * time.Minute,
		k8sClient:        getK8sClient(),
		statRefreshCount: selfstat.Register("gtc_processor", "refresh_count", map[string]string{}),
		statItemCount:    selfstat.Register("gtc_processor", "cache_size", map[string]string{}),
		statQueryErrors:  selfstat.Register("gtc_processor", "query_errors", map[string]string{}),
		log:              log,
	}
}

func (tm *PodMap) Set(key, value string) {
	tm.mutexData.Lock()
	defer tm.mutexData.Unlock()
	tm.data[key] = Data{Value: value, Timestamp: time.Now()}
}

func (tm *PodMap) Get(key string) (string, bool) {
	tm.mutexData.RLock()
	defer tm.mutexData.RUnlock()
	data, ok := tm.data[key]
	return data.Value, ok
}

func (tm *PodMap) Size() int {
	tm.mutexData.RLock()
	defer tm.mutexData.RUnlock()
	return len(tm.data)
}

func (tm *PodMap) GetWait(key string) (string, bool) {
	// try getting value
	if data, ok := tm.Get(key); ok {
		return data, ok
	}

	namespace, podName := splitPodKey(key)
	pod, err := downloadPod(tm.k8sClient, namespace, podName)
	if err != nil {
		tm.statQueryErrors.Incr(1)
		tm.log.Errorf("gtc_add_service_label - error getting pod %s from api server: %v", key, err)
		return "", false
	}

	service := createServiceLabel(pod)
	tm.mutexData.Lock()
	defer tm.mutexData.Unlock()
	tm.data[key] = Data{service, time.Now()}
	tm.statItemCount.Set(int64(len(tm.data)))
	tm.log.Infof("gtc_add_service_label - got service label %s for pod %s, pod-service map size = %d", service, key, len(tm.data))

	return service, true
}

func (tm *PodMap) StartRefreshLoop() {
	// blocking
	tm.populateMap()
	tm.statRefreshCount.Incr(1)
	tm.statItemCount.Set(int64(len(tm.data)))
	tm.log.Infof("gtc_add_service_label - initial pod-service map size = %d", len(tm.data))
	// populate every refreshInterval
	// - will automatically remove old pods
	go func() {
		for {
			time.Sleep(tm.refreshInterval)
			// populate
			tm.populateMap()
			tm.statRefreshCount.Incr(1)
			// delete old pods (older than twice refreshInterval)
			tm.mutexData.Lock()
			for key, data := range tm.data {
				if time.Since(data.Timestamp) > (2 * tm.refreshInterval) {
					delete(tm.data, key)
				}
			}
			tm.statItemCount.Set(int64(len(tm.data)))
			tm.log.Infof("gtc_add_service_label - after cleanup pod-service map size = %d", len(tm.data))
			tm.mutexData.Unlock()
		}
	}()
}

func (tm *PodMap) populateMap() {
	pods, err := downloadPods(tm.k8sClient)
	if err != nil {
		tm.statQueryErrors.Incr(1)
		tm.log.Errorf("gtc_add_service_label - error populating pod-service map: %v", err)
	}

	tmpMap := make(map[string]Data)
	now := time.Now()
	for _, pod := range pods.Items {
		podKey := createPodKey(pod.Namespace, pod.Name)
		tmpMap[podKey] = Data{createServiceLabel(&pod), now}
	}
	tm.log.Infof("gtc_add_service_label - tmp map size = %d", len(tmpMap))
	if len(tmpMap) <= 0 {
		tm.log.Warn("gtc_add_service_label - created pod-service map is empty")
	}

	tm.mutexData.Lock()
	defer tm.mutexData.Unlock()
	// merge tmpMap into main map (we don't want to delete old pods immediately to account for metric lag)
	for k, v := range tmpMap {
		tm.data[k] = v
	}
	tm.log.Infof("gtc_add_service_label - new pod-service map size = %d", len(tm.data))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API Server - TODO move to another file
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func getK8sClient() *kubernetes.Clientset {
	config, err := rest.InClusterConfig()
	if err != nil {
		// TODO
		panic(err.Error())
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		// TODO
		panic(err.Error())
	}

	return client
}

func downloadPod(clientset *kubernetes.Clientset, namespace string, podName string) (*corev1.Pod, error) {
	pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve pod %s in namespace %s from k8s api: %v", podName, namespace, err)
	}
	return pod, nil
}

func downloadPods(clientset *kubernetes.Clientset) (*corev1.PodList, error) {
	pods, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve pods from k8s api: %v", err)
	}
	return pods, nil
}

func createServiceLabel(pod *corev1.Pod) string {
	service, ok := pod.Annotations["prometheus.io/label-service"]
	if !ok {
		if project, ok := pod.Annotations["app.tug.jive.com/project"]; ok {
			if app, ok := pod.Annotations["app.tug.jive.com/app"]; ok {
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

type AddServiceLabel struct {
	podServiceMap *PodMap
	parallel      parallel.Parallel

	LabelName       string `toml:"label"`
	MaxQueued       int    `toml:"max_queued"`
	RefreshInterval int    `toml:"refresh_interval"`

	statQueueLimit selfstat.Stat
	statQueueSize  selfstat.Stat

	Log telegraf.Logger `toml:"-"`
}

//go:embed sample.conf
var sampleConfig string

func (*AddServiceLabel) SampleConfig() string {
	return sampleConfig
}

func (p *AddServiceLabel) Init() error {
	p.statQueueLimit = selfstat.Register("gtc_processor", "queue_limit", map[string]string{})
	p.statQueueSize = selfstat.Register("gtc_processor", "queue_size", map[string]string{})

	p.statQueueLimit.Set(int64(p.MaxQueued))

	return nil
}

// Start initalizes the podServiceMap global map
// we protect it with an if nil because podServiceMap is global and
// Start is called once per instance of the plugin
func (p *AddServiceLabel) Start(acc telegraf.Accumulator) error {
	if p.podServiceMap == nil {
		p.podServiceMap = NewPodMap(
			p.RefreshInterval,
			p.Log,
		)
		p.podServiceMap.StartRefreshLoop()
		p.Log.Info("gtc_add_service_label - map initialized")
	}
	// Only 1 worker since there's no need to query api server multiple times
	p.parallel = parallel.NewOrdered(acc, p.asyncAdd, p.MaxQueued, 1)
	p.Log.Info("gtc_add_service_label - plugin started")
	return nil
}

func (p *AddServiceLabel) Add(metric telegraf.Metric, acc telegraf.Accumulator) error {
	if podKey := podKeyFromMetric(metric); len(podKey) > 0 {
		if service, ok := p.podServiceMap.Get(podKey); ok {
			// 'service' can be empty string
			if len(service) > 0 {
				metric.RemoveTag(p.LabelName)
				metric.AddTag(p.LabelName, service)
			}
			acc.AddMetric(metric)
		} else {
			// Add to parallel queue to retrieve 'service' label in background
			p.statQueueSize.Incr(1)
			p.parallel.Enqueue(metric)
		}
	} else {
		acc.AddMetric(metric)
	}

	return nil
}

func (p *AddServiceLabel) asyncAdd(metric telegraf.Metric) []telegraf.Metric {
	if podKey := podKeyFromMetric(metric); len(podKey) > 0 {
		if service, ok := p.podServiceMap.GetWait(podKey); len(service) > 0 {
			metric.RemoveTag(p.LabelName)
			metric.AddTag(p.LabelName, service)
			p.Log.Infof("gtc_add_service_label - label added for %s", podKey)
		} else {
			p.Log.Infof("gtc_add_service_label - label not added for %s - service='%s' - ok='%t'", podKey, service, ok)
		}
	}
	p.statQueueSize.Incr(-1)
	return []telegraf.Metric{metric}
}

func (p *AddServiceLabel) Stop() {
	p.parallel.Stop()
}

func init() {
	processors.AddStreaming("gtc_add_service_label", func() telegraf.StreamingProcessor {
		return &AddServiceLabel{
			LabelName:       "service",
			MaxQueued:       10000,
			RefreshInterval: 15,
		}
	})
}
