package id

import (
	"github.com/blastbao/influxdb/chronograf"
	uuid "github.com/satori/go.uuid"
)

var _ chronograf.ID = &UUID{}

// UUID generates a V4 uuid
type UUID struct{}

// Generate creates a UUID v4 string
func (i *UUID) Generate() (string, error) {
	return uuid.NewV4().String(), nil
}
