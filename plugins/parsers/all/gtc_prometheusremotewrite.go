//go:build !custom || parsers || parsers.gtc_prometheusremotewrite

package all

import _ "github.com/influxdata/telegraf/plugins/parsers/gtc_prometheusremotewrite" // register plugin
