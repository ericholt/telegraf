package gtc_join_service

// gtc_join_service.go

import (
	_ "embed"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"
)

//go:embed sample.conf
var sampleConfig string

type Data struct {
	Value     string
	Timestamp time.Time
}

type TagMap struct {
	mutex sync.RWMutex
	data  map[string]Data
}

func (tm *TagMap) Set(key, value string) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.data[key] = Data{Value: value, Timestamp: time.Now()}
}

func (tm *TagMap) Get(key string) (string, bool) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	data, ok := tm.data[key]
	return data.Value, ok
}

func (sm *TagMap) Remove(key string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	delete(sm.data, key)
}

func (tm *TagMap) Cleanup() {
	go func() {
		interval := 5 * time.Minute
		for {
			time.Sleep(interval)
			tm.mutex.Lock()
			for key, data := range tm.data {
				if time.Since(data.Timestamp) > interval {
					delete(tm.data, key)
					fmt.Println("Deleted key", key)
				}
			}
			tm.mutex.Unlock()
		}
	}()
}

// stored tags global var
var joinTags *TagMap

type JoinService struct {
	Store bool `toml:"store"`
	Copy  bool `toml:"copy"`

	Log telegraf.Logger `toml:"-"`
}

func (*JoinService) SampleConfig() string {
	return sampleConfig
}

// Init is used here to initalize the joinTags global map
// we protect it with an if nil because the Init function
// is called twice by default and can be called more depending
// on config.
func (p *JoinService) Init() error {
	if joinTags == nil {
		fmt.Println("Init - make joinTags")
		joinTags = &TagMap{
			data: make(map[string]Data),
		}
		joinTags.Cleanup()
	}
	return nil
}

func (p *JoinService) Apply(in ...telegraf.Metric) []telegraf.Metric {
	// fmt.Println("Apply metric count -", len(in))
	if p.Store {
		for _, metric := range in {
			// fmt.Println("store metric - ", metric)
			if pod, ok := metric.GetTag("pod"); ok {
				if service, ok := metric.GetTag("annotation_prometheus_io_label_service"); ok {
					joinTags.Set(pod, service)
				} else if project, ok := metric.GetTag("annotation_app_tug_jive_com_project"); ok {
					if app, ok := metric.GetTag("annotation_app_tug_jive_com_app"); ok {
						service = project + "/" + app
						joinTags.Set(pod, service)
					}
				}
			}
		}
		// fmt.Println("store joinTags -", joinTags)
	}

	if p.Copy {
		// fmt.Println("copy joinTags -", joinTags)
		for _, metric := range in {
			// fmt.Println("copy metric before - ", metric)
			if pod, ok := metric.GetTag("pod"); ok {
				if service, ok := joinTags.Get(pod); ok {
					metric.RemoveTag("service")
					metric.AddTag("service", service)
				}
			}
			// fmt.Println("copy metric after - ", metric)
		}
	}

	return in
}

func init() {
	processors.Add("gtc_join_service", func() telegraf.Processor {
		return &JoinService{}
	})
}
