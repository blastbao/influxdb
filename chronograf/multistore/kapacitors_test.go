package multistore

import (
	"testing"

	"github.com/blastbao/influxdb/chronograf"
)

func TestInterfaceImplementation(t *testing.T) {
	var _ chronograf.ServersStore = &KapacitorStore{}
}
