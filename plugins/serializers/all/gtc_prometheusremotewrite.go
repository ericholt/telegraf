//go:build !custom || serializers || serializers.gtc_prometheusremotewrite

package all

import (
	_ "github.com/influxdata/telegraf/plugins/serializers/gtc_prometheusremotewrite" // register plugin
)
