package testing

import "github.com/blastbao/influxdb"

// IDPtr returns a pointer to an influxdb.ID.
func IDPtr(id influxdb.ID) *influxdb.ID { return &id }
