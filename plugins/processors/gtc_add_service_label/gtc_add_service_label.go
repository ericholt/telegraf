package gtc_join_service

// gtc_join_service.go

import (
	"context"
	_ "embed"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/common/parallel"
	"github.com/influxdata/telegraf/plugins/processors"
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

	mutexRefresh    sync.Mutex
	wgRefreshing    sync.WaitGroup
	wgTriggered     sync.WaitGroup
	triggerRefresh  chan bool
	refreshInterval int

	log telegraf.Logger
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

func (tm *PodMap) GetWait(key string) (string, bool) {
	// try getting value
	if data, ok := tm.Get(key); ok {
		return data, ok
	}
	// prevent multiple threads from starting refresh simultaneously
	tm.mutexRefresh.Lock()
	{
		// protected by mutexRefresh (extra safety)
		// wait if refresh in progress
		tm.wgRefreshing.Wait()
		// try getting value following potential refresh
		if data, ok := tm.Get(key); ok {
			tm.mutexRefresh.Unlock()
			return data, ok
		}
		tm.log.Infof("gtc_add_service_label - trigger refresh for pod %s", key)
		// trigger refresh
		tm.wgTriggered.Add(1)
		tm.triggerRefresh <- true
		// wait for refresh to begin (we can't wait on wgRefreshing before Add(1) is called on it)
		tm.wgTriggered.Wait()
		// wait for refresh to end
		tm.wgRefreshing.Wait()
	}
	tm.mutexRefresh.Unlock()

	// return value
	return tm.Get(key)
}

func (tm *PodMap) StartCleanupLoop() {
	go func() {
		// runs every (5*refreshInterval) and removes pods that are more than (5*refreshInterval) old
		interval := 5 * tm.getRefreshInterval()
		for {
			time.Sleep(interval)
			tm.mutexData.Lock()
			for key, data := range tm.data {
				if time.Since(data.Timestamp) > interval {
					delete(tm.data, key)
				}
			}
			tm.log.Infof("gtc_add_service_label - after cleanup pod-service map size = %d", len(tm.data))
			tm.mutexData.Unlock()
		}
	}()
}

func (tm *PodMap) PopulateRefresh() {
	k8sClient := getK8sClient()
	// blocking
	tm.populateMap(k8sClient)
	tm.triggerRefresh = make(chan bool)
	// non-blocking
	// populate every x minutes
	// - will automatically remove old pods
	// TODO-nice-to-have: keep old pods for 1 extra cycle since some metrics might arrive right after pod termination
	go func() {
		interval := tm.getRefreshInterval()
		for {
			timer := time.After(interval)
			select {
			case <-timer:
				tm.mutexRefresh.Lock()
				tm.wgRefreshing.Add(1)
				tm.mutexRefresh.Unlock()
				tm.log.Info("gtc_add_service_label - start refresh after time interval reached")
			case <-tm.triggerRefresh:
				tm.wgRefreshing.Add(1)
				tm.wgTriggered.Done()
				tm.log.Info("gtc_add_service_label - start refresh after triggered")
			}
			tm.populateMap(k8sClient)
			tm.wgRefreshing.Done()
		}
	}()
}

func (tm *PodMap) getRefreshInterval() time.Duration {
	return time.Duration(tm.refreshInterval) * time.Minute
}

func (tm *PodMap) populateMap(k8sClient *kubernetes.Clientset) {
	pods, err := downloadPods(k8sClient)
	if err != nil {
		tm.log.Errorf("gtc_add_service_label - error populating pod-service map: %v", err)
	}

	tmpMap := make(map[string]Data)
	now := time.Now()
	for _, pod := range pods.Items {
		tmpMap[pod.Name] = Data{createServiceLabel(&pod), now}
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

func downloadPods(clientset *kubernetes.Clientset) (*corev1.PodList, error) {
	// Use Get("pod_name") instead (and namespace if possible)
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

// stored tags global var
var podServiceMap *PodMap

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Plugin
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type AddServiceLabel struct {
	parallel parallel.Parallel

	LabelName       string `toml:"label"`
	MaxQueued       int    `toml:"max_queued"`
	RefreshInterval int    `toml:"refresh_interval"`

	Log telegraf.Logger `toml:"-"`
}

//go:embed sample.conf
var sampleConfig string

func (*AddServiceLabel) SampleConfig() string {
	return sampleConfig
}

func (p *AddServiceLabel) Init() error {
	return nil
}

// Start initalizes the podServiceMap global map
// we protect it with an if nil because podServiceMap is global and
// Start is called once per instance of the plugin
func (p *AddServiceLabel) Start(acc telegraf.Accumulator) error {
	if podServiceMap == nil {
		podServiceMap = &PodMap{
			data:            make(map[string]Data),
			refreshInterval: p.RefreshInterval,
			log:             p.Log,
		}
		podServiceMap.PopulateRefresh()
		p.Log.Info("gtc_add_service_label - map initialized")
		podServiceMap.StartCleanupLoop()
	}
	// Only 1 worker since there's no need to query api server multiple times
	p.parallel = parallel.NewOrdered(acc, p.asyncAdd, p.MaxQueued, 1)
	p.Log.Info("gtc_add_service_label - plugin started")
	return nil
}

func (p *AddServiceLabel) Add(metric telegraf.Metric, acc telegraf.Accumulator) error {
	if pod, ok := metric.GetTag("pod"); ok {
		if service, ok := podServiceMap.Get(pod); ok {
			// 'service' can be empty string
			if len(service) > 0 {
				metric.RemoveTag(p.LabelName)
				metric.AddTag(p.LabelName, service)
			}
			acc.AddMetric(metric)
		} else {
			// Add to parallel queue to retrieve 'service' label in background
			p.parallel.Enqueue(metric)
		}
	} else {
		acc.AddMetric(metric)
	}

	return nil
}

func (p *AddServiceLabel) asyncAdd(metric telegraf.Metric) []telegraf.Metric {
	if pod, ok := metric.GetTag("pod"); ok {
		if service, ok := podServiceMap.GetWait(pod); len(service) > 0 {
			metric.RemoveTag(p.LabelName)
			metric.AddTag(p.LabelName, service)
			p.Log.Infof("gtc_add_service_label - label added for %s", pod)
		} else {
			p.Log.Infof("gtc_add_service_label - label not added for %s - service='%s' - ok='%t'", pod, service, ok)
		}
	}
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
			RefreshInterval: 5,
		}
	})
}
