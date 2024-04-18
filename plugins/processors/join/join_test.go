package join

import (
	_ "embed"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/stretchr/testify/assert"
)

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

func TestStore(t *testing.T) {
	r := Join{
		Stores: []Store{
			{Measurement: "kube_pod_annotations", MatchTag: "pod", CopyTag: "annotation_prometheus_io_label_service"},
		},
	}
	m1 := newMetric("kube_pod_annotations", map[string]string{"annotation_prometheus_io_label_service": "myservice1", "pod": "myservice1-asdf"}, nil)
	m2 := newMetric("kube_pod_info", map[string]string{"annotation_prometheus_io_label_service": "myservice2", "pod": "myservice2-asdf"}, nil)
	m3 := newMetric("kube_pod_annotations", map[string]string{"pod": "myservice3-asdf"}, nil)
	m4 := newMetric("kube_pod_annotations", map[string]string{"annotation_prometheus_io_label_service": "myservice4"}, nil)
	m5 := newMetric("kube_pod_annotations", map[string]string{"annotation_prometheus_io_label_service": "myservice5", "pod": "myservice5-asdf"}, nil)
	r.Init()
	r.Apply(m1, m2, m3, m4, m5)

	assert.Equal(t, joinTags, map[string]string{
		"myservice1-asdf": "myservice1",
		"myservice5-asdf": "myservice5",
	})

	// require.Equal(t, map[string]string{"host": "localhost", "region": "east-1"}, results[0].Tags(), "should change tag 'hostname' to 'host'")
}
