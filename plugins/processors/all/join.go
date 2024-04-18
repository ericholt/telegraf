//go:build !custom || processors || processors.join

package all

import _ "github.com/influxdata/telegraf/plugins/processors/join" // register plugin
