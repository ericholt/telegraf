package gtc_processor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/testutil"
)

type PodMapMock struct {
	data map[string]Data
}

func (pm *PodMapMock) Get(key string) (string, bool) {
	data, ok := pm.data[key]
	return data.Value, ok
}

func (pm *PodMapMock) GetWait(key string) (string, bool) {
	time.Sleep(5 * time.Hour)
	return "val", true
}

func (pm *PodMapMock) StartRefreshLoop() {}

func newMetric(name string, tags map[string]string, fields map[string]interface{}) telegraf.Metric {
	if tags == nil {
		tags = map[string]string{}
	}
	if fields == nil {
		fields = map[string]interface{}{}
	}
	m := metric.New(name, tags, fields, time.Now())
	return m
}

func TestMultiplePodDownloads(t *testing.T) {
	log := testutil.Logger{}
	plugin := GtcProcessor{
		LabelName:       "service",
		MaxQueued:       5,
		RefreshInterval: 15,
		Log:             log,
		podServiceMap:   &PodMapMock{},
	}
	plugin.Init()

	// Create metrics
	metrics := make([]telegraf.Metric, 10)
	for idx := range 10 {
		metrics[idx] = newMetric(fmt.Sprintf("metric_%d", idx), map[string]string{"namespace": "ns", "pod": "pd"}, nil)
	}

	// Process expected metrics and compare with resulting metrics
	acc := &testutil.Accumulator{}
	plugin.Start(acc)
	for _, m := range metrics {
		log.Infof("Queue size - %d", plugin.statQueueSize.Get())
		log.Infof("Add - %s", m.Name())
		plugin.Add(m, acc)
	}
	require.Equal(t, 10, plugin.statQueueSize.Get())

	plugin.Stop()
}
