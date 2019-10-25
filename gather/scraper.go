package gather

import (
	"context"

	"github.com/blastbao/influxdb"
)

// Scraper gathers metrics from a scraper target.
type Scraper interface {
	Gather(ctx context.Context, target influxdb.ScraperTarget) (collected MetricsCollection, err error)
}
