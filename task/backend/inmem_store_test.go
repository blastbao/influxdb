package backend_test

import (
	"testing"

	"github.com/blastbao/influxdb/task/backend"
	"github.com/blastbao/influxdb/task/backend/storetest"
	"github.com/blastbao/influxdb/task/options"
)

func init() {
	// TODO(mr): remove as part of https://github.com/influxdata/platform/issues/484.
	options.EnableScriptCacheForTest()
}

func TestInMemStore(t *testing.T) {
	storetest.NewStoreTest(
		"in-mem store",
		func(t *testing.T) backend.Store {
			return backend.NewInMemStore()
		},
		func(t *testing.T, s backend.Store) {},
	)(t)
}
