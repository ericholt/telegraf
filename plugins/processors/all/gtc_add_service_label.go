//go:build !custom || processors || processors.gtc_add_service_label

package all

import _ "github.com/influxdata/telegraf/plugins/processors/gtc_add_service_label" // register plugin
