package inputs

import "github.com/blastbao/influxdb/telegraf/plugins"

type baseInput int

func (b baseInput) Type() plugins.Type {
	return plugins.Input
}
