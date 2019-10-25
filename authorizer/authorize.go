package authorizer

import (
	"context"
	"fmt"

	"github.com/blastbao/influxdb"
	influxdbcontext "github.com/blastbao/influxdb/context"
)

// IsAllowed checks to see if an action is authorized by retrieving the authorizer
// off of context and authorizing the action appropriately.
func IsAllowed(ctx context.Context, p influxdb.Permission) error {
	a, err := influxdbcontext.GetAuthorizer(ctx)
	if err != nil {
		return err
	}

	if !a.Allowed(p) {
		return &influxdb.Error{
			Code: influxdb.EUnauthorized,
			Msg:  fmt.Sprintf("%s is unauthorized", p),
		}
	}

	return nil
}
