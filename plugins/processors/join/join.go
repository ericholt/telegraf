package join

// join.go

import (
	_ "embed"
	"fmt"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"
)

//go:embed sample.conf
var sampleConfig string

// stored tags
var joinTags map[string]string

type Store struct {
	Measurement string `toml:"measurement"`
	MatchTag    string `toml:"match_tag"`
	CopyTag     string `toml:"copy_tag"`
}

type Copy struct {
	MatchTag string `toml:"match_tag"`
	CopyTag  string `toml:"copy_tag"`
}

type Join struct {
	Stores []Store `toml:"store"`
	Copies []Copy  `toml:"copy"`

	Log telegraf.Logger `toml:"-"`
}

func (*Join) SampleConfig() string {
	return sampleConfig
}

// Init is used here to initalize the joinTags global map
// we protect it with an if nil because the Init function
// is called twice by default and can be called more depending
// on config.
func (p *Join) Init() error {
	fmt.Println("join Init")
	if joinTags == nil {
		fmt.Println("join Init - make joinTags")
		joinTags = make(map[string]string)
	}
	fmt.Println("stores - ", p.Stores)
	fmt.Println("copies - ", p.Copies)
	return nil
}

func (p *Join) Apply(in ...telegraf.Metric) []telegraf.Metric {
	fmt.Println("Apply metric count - ", len(in))
	matchCount := 0
	for _, store := range p.Stores {
		for _, metric := range in {
			if metric.Name() == store.Measurement {
				matchCount += 1
				fmt.Println("metric - ", metric)
				if key, isSet := metric.GetTag(store.MatchTag); isSet {
					if value, isSet := metric.GetTag(store.CopyTag); isSet {
						joinTags[key] = value
					}
				}
			}
		}
	}

	fmt.Println("matchCount - ", matchCount)
	fmt.Println("joinTags - ", joinTags)

	return in
}

func init() {
	fmt.Println("join init - Add")
	processors.Add("join", func() telegraf.Processor {
		return &Join{}
	})
}
