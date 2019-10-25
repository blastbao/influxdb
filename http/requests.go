package http

import (
	"context"
	"net/http"

	platform "github.com/blastbao/influxdb"
)

const (
	// OrgName is the http query parameter to specify an organization by name.
	OrgName = "org"
	// OrgID is the http query parameter to specify an organization by ID.
	OrgID = "orgID"
)

// queryOrganization returns the organization for any http request.
func queryOrganization(ctx context.Context, r *http.Request, svc platform.OrganizationService) (o *platform.Organization, err error) {
	filter := platform.OrganizationFilter{}
	if reqID := r.URL.Query().Get(OrgID); reqID != "" {
		filter.ID, err = platform.IDFromString(reqID)
		if err != nil {
			return nil, err
		}
	}

	if name := r.URL.Query().Get(OrgName); name != "" {
		filter.Name = &name
	}

	return svc.FindOrganization(ctx, filter)
}
