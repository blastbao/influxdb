package inmem

import (
	"context"
	"testing"

	platform "github.com/blastbao/influxdb"
	platformtesting "github.com/blastbao/influxdb/testing"
)

func initDashboardService(f platformtesting.DashboardFields, t *testing.T) (platform.DashboardService, string, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.Background()
	s.WithTime(f.NowFn)
	for _, b := range f.Dashboards {
		if err := s.PutDashboard(ctx, b); err != nil {
			t.Fatalf("failed to populate Dashboards")
		}
	}
	return s, OpPrefix, func() {}
}

func TestDashboardService(t *testing.T) {
	platformtesting.DashboardService(initDashboardService, t)
}
