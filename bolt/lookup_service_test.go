package bolt_test

import (
	"context"
	"testing"

	platform "github.com/blastbao/influxdb"
	"github.com/blastbao/influxdb/bolt"
	"github.com/blastbao/influxdb/mock"
	platformtesting "github.com/blastbao/influxdb/testing"
)

var (
	testID    = platform.ID(1)
	testIDStr = testID.String()
)

func TestClient_Name(t *testing.T) {
	type initFn func(ctx context.Context, c *bolt.Client) error
	type args struct {
		resource platform.Resource
		init     initFn
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "error if id is invalid",
			args: args{
				resource: platform.Resource{
					Type: platform.DashboardsResourceType,
					ID:   platformtesting.IDPtr(platform.InvalidID()),
				},
			},
			wantErr: true,
		},
		{
			name: "error if resource is invalid",
			args: args{
				resource: platform.Resource{
					Type: platform.ResourceType("invalid"),
				},
			},
			wantErr: true,
		},
		{
			name: "authorization resource without a name returns empty string",
			args: args{
				resource: platform.Resource{
					Type: platform.AuthorizationsResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
			},
			want: "",
		},
		{
			name: "task resource without a name returns empty string",
			args: args{
				resource: platform.Resource{
					Type: platform.TasksResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
			},
			want: "",
		},
		{
			name: "bucket with existing id returns name",
			args: args{
				resource: platform.Resource{
					Type: platform.BucketsResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
				init: func(ctx context.Context, s *bolt.Client) error {
					_ = s.CreateOrganization(ctx, &platform.Organization{
						Name: "o1",
					})
					return s.CreateBucket(ctx, &platform.Bucket{
						Name:           "b1",
						OrganizationID: testID,
					})
				},
			},
			want: "b1",
		},
		{
			name: "bucket with non-existent id returns error",
			args: args{
				resource: platform.Resource{
					Type: platform.BucketsResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
			},
			wantErr: true,
		},
		{
			name: "dashboard with existing id returns name",
			args: args{
				resource: platform.Resource{
					Type: platform.DashboardsResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
				init: func(ctx context.Context, s *bolt.Client) error {
					return s.CreateDashboard(ctx, &platform.Dashboard{
						Name:           "dashboard1",
						OrganizationID: 1,
					})
				},
			},
			want: "dashboard1",
		},
		{
			name: "dashboard with non-existent id returns error",
			args: args{
				resource: platform.Resource{
					Type: platform.DashboardsResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
			},
			wantErr: true,
		},
		{
			name: "org with existing id returns name",
			args: args{
				resource: platform.Resource{
					Type: platform.OrgsResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
				init: func(ctx context.Context, s *bolt.Client) error {
					return s.CreateOrganization(ctx, &platform.Organization{
						Name: "org1",
					})
				},
			},
			want: "org1",
		},
		{
			name: "org with non-existent id returns error",
			args: args{
				resource: platform.Resource{
					Type: platform.OrgsResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
			},
			wantErr: true,
		},
		{
			name: "source with existing id returns name",
			args: args{
				resource: platform.Resource{
					Type: platform.SourcesResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
				init: func(ctx context.Context, s *bolt.Client) error {
					return s.CreateSource(ctx, &platform.Source{
						Name: "source1",
					})
				},
			},
			want: "source1",
		},
		{
			name: "source with non-existent id returns error",
			args: args{
				resource: platform.Resource{
					Type: platform.SourcesResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
			},
			wantErr: true,
		},
		{
			name: "telegraf with existing id returns name",
			args: args{
				resource: platform.Resource{
					Type: platform.TelegrafsResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
				init: func(ctx context.Context, s *bolt.Client) error {
					return s.CreateTelegrafConfig(ctx, &platform.TelegrafConfig{
						OrganizationID: platformtesting.MustIDBase16("0000000000000009"),
						Name:           "telegraf1",
					}, testID)
				},
			},
			want: "telegraf1",
		},
		{
			name: "telegraf with non-existent id returns error",
			args: args{
				resource: platform.Resource{
					Type: platform.TelegrafsResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
			},
			wantErr: true,
		},
		{
			name: "user with existing id returns name",
			args: args{
				resource: platform.Resource{
					Type: platform.UsersResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
				init: func(ctx context.Context, s *bolt.Client) error {
					return s.CreateUser(ctx, &platform.User{
						Name: "user1",
					})
				},
			},
			want: "user1",
		},
		{
			name: "user with non-existent id returns error",
			args: args{
				resource: platform.Resource{
					Type: platform.UsersResourceType,
					ID:   platformtesting.IDPtr(testID),
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, done, err := NewTestClient()
			if err != nil {
				t.Fatalf("unable to create bolt test client: %v", err)
			}
			defer done()

			c.IDGenerator = mock.NewIDGenerator(testIDStr, t)
			ctx := context.Background()
			if tt.args.init != nil {
				if err := tt.args.init(ctx, c); err != nil {
					t.Errorf("Service.Name() unable to initialize service: %v", err)
				}
			}
			id := platform.InvalidID()
			if tt.args.resource.ID != nil {
				id = *tt.args.resource.ID
			}
			got, err := c.Name(ctx, tt.args.resource.Type, id)
			if (err != nil) != tt.wantErr {
				t.Errorf("Service.Name() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Service.Name() = %v, want %v", got, tt.want)
			}
		})
	}
}
