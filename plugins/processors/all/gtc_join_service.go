//go:build !custom || processors || processors.gtc_join_service

package all

import _ "github.com/influxdata/telegraf/plugins/processors/gtc_join_service" // register plugin
