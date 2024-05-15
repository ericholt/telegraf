//go:build !custom || processors || processors.gtc_processor

package all

import _ "github.com/influxdata/telegraf/plugins/processors/gtc_processor" // register plugin
