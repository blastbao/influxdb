package noop

import (
	"context"
	"fmt"

	"github.com/blastbao/influxdb/chronograf"
)

// ensure OrganizationConfigStore implements chronograf.OrganizationConfigStore
var _ chronograf.OrganizationConfigStore = &OrganizationConfigStore{}

type OrganizationConfigStore struct{}

func (s *OrganizationConfigStore) FindOrCreate(context.Context, string) (*chronograf.OrganizationConfig, error) {
	return nil, chronograf.ErrOrganizationConfigNotFound
}

func (s *OrganizationConfigStore) Put(context.Context, *chronograf.OrganizationConfig) error {
	return fmt.Errorf("cannot replace config")
}
