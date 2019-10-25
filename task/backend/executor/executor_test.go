package executor_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/values"
	platform "github.com/blastbao/influxdb"
	icontext "github.com/blastbao/influxdb/context"
	"github.com/blastbao/influxdb/inmem"
	"github.com/blastbao/influxdb/query"
	_ "github.com/blastbao/influxdb/query/builtin"
	"github.com/blastbao/influxdb/task"
	"github.com/blastbao/influxdb/task/backend"
	"github.com/blastbao/influxdb/task/backend/executor"
	"go.uber.org/zap"
)

type fakeQueryService struct {
	mu       sync.Mutex
	queries  map[string]*fakeQuery
	queryErr error
	// The most recent ctx received in the Query method.
	// Used to validate that the executor applied the correct authorizer.
	mostRecentCtx context.Context
}

var _ query.AsyncQueryService = (*fakeQueryService)(nil)

func makeSpec(q string) *flux.Spec {
	qs, err := flux.Compile(context.Background(), q, time.Unix(123, 0))
	if err != nil {
		panic(err)
	}
	return qs
}

func makeSpecString(q *flux.Spec) string {
	b, err := json.Marshal(q)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func newFakeQueryService() *fakeQueryService {
	return &fakeQueryService{queries: make(map[string]*fakeQuery)}
}

func (s *fakeQueryService) Query(ctx context.Context, req *query.Request) (flux.Query, error) {
	if req.Authorization == nil {
		panic("authorization required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mostRecentCtx = ctx
	if s.queryErr != nil {
		err := s.queryErr
		s.queryErr = nil
		return nil, err
	}

	sc, ok := req.Compiler.(lang.SpecCompiler)
	if !ok {
		return nil, fmt.Errorf("fakeQueryService only supports the SpecCompiler, got %T", req.Compiler)
	}

	fq := &fakeQuery{
		wait:  make(chan struct{}),
		ready: make(chan map[string]flux.Result),
	}
	s.queries[makeSpecString(sc.Spec)] = fq

	go fq.run(ctx)

	return fq, nil
}

// SucceedQuery allows the running query matching the given script to return on its Ready channel.
func (s *fakeQueryService) SucceedQuery(script string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Unblock the flux.
	spec := makeSpecString(makeSpec(script))
	close(s.queries[spec].wait)
	delete(s.queries, spec)
}

// FailQuery closes the running query's Ready channel and sets its error to the given value.
func (s *fakeQueryService) FailQuery(script string, forced error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Unblock the flux.
	spec := makeSpecString(makeSpec(script))
	s.queries[spec].forcedError = forced
	close(s.queries[spec].wait)
	delete(s.queries, spec)
}

// FailNextQuery causes the next call to QueryWithCompile to return the given error.
func (s *fakeQueryService) FailNextQuery(forced error) {
	s.queryErr = forced
}

// WaitForQueryLive ensures that the query has made it into the service.
// This is particularly useful for the synchronous executor,
// because the execution starts on a separate goroutine.
func (s *fakeQueryService) WaitForQueryLive(t *testing.T, script string) {
	t.Helper()

	const attempts = 10
	spec := makeSpecString(makeSpec(script))
	for i := 0; i < attempts; i++ {
		if i != 0 {
			time.Sleep(5 * time.Millisecond)
		}

		s.mu.Lock()
		_, ok := s.queries[spec]
		s.mu.Unlock()
		if ok {
			return
		}
	}

	t.Fatalf("Did not see live query %q in time", script)
}

type fakeQuery struct {
	ready       chan map[string]flux.Result
	wait        chan struct{} // Blocks Ready from returning.
	forcedError error         // Value to return from Err() method.

	ctxErr error // Error from ctx.Done.
}

var _ flux.Query = (*fakeQuery)(nil)

func (q *fakeQuery) Spec() *flux.Spec                     { return nil }
func (q *fakeQuery) Done()                                {}
func (q *fakeQuery) Cancel()                              { close(q.ready) }
func (q *fakeQuery) Statistics() flux.Statistics          { return flux.Statistics{} }
func (q *fakeQuery) Ready() <-chan map[string]flux.Result { return q.ready }

func (q *fakeQuery) Err() error {
	if q.ctxErr != nil {
		return q.ctxErr
	}
	return q.forcedError
}

// run is intended to be run on its own goroutine.
// It blocks until q.wait is closed, then sends a fake result on the q.ready channel.
func (q *fakeQuery) run(ctx context.Context) {
	// Wait for call to set query success/fail.
	select {
	case <-ctx.Done():
		q.ctxErr = ctx.Err()
		close(q.ready)
		return
	case <-q.wait:
		// Normal case.
	}

	if q.forcedError == nil {
		res := newFakeResult()
		q.ready <- map[string]flux.Result{
			res.Name(): res,
		}
	} else {
		close(q.ready)
	}
}

// fakeResult is a dumb implementation of flux.Result that always returns the same values.
type fakeResult struct {
	name  string
	table flux.Table
}

var _ flux.Result = (*fakeResult)(nil)

func newFakeResult() *fakeResult {
	meta := []flux.ColMeta{{Label: "x", Type: flux.TInt}}
	vals := []values.Value{values.NewInt(int64(1))}
	gk := execute.NewGroupKey(meta, vals)
	a := &memory.Allocator{}
	b := execute.NewColListTableBuilder(gk, a)
	i, _ := b.AddCol(meta[0])
	b.AppendInt(i, int64(1))
	t, err := b.Table()
	if err != nil {
		panic(err)
	}
	return &fakeResult{name: "res", table: t}
}

func (r *fakeResult) Statistics() flux.Statistics {
	return flux.Statistics{}
}

func (r *fakeResult) Name() string               { return r.name }
func (r *fakeResult) Tables() flux.TableIterator { return tables{r.table} }

// tables makes a TableIterator out of a slice of Tables.
type tables []flux.Table

var _ flux.TableIterator = tables(nil)

func (ts tables) Do(f func(flux.Table) error) error {
	for _, t := range ts {
		if err := f(t); err != nil {
			return err
		}
	}
	return nil
}

func (ts tables) Statistics() flux.Statistics { return flux.Statistics{} }

type noopRunCanceler struct{}

func (noopRunCanceler) CancelRun(ctx context.Context, taskID, runID platform.ID) error { return nil }

type system struct {
	name string
	svc  *fakeQueryService
	ts   platform.TaskService
	ex   backend.Executor
	// We really just want an authorization service here, but we take a whole inmem service
	// to ensure that the authorization service validates org and user existence properly.
	i *inmem.Service
}

type createSysFn func() *system

func createAsyncSystem() *system {
	svc := newFakeQueryService()
	i := inmem.NewService()
	ts := task.PlatformAdapter(backend.NewInMemStore(), backend.NopLogReader{}, noopRunCanceler{}, i, i, i)
	return &system{
		name: "AsyncExecutor",
		svc:  svc,
		ts:   ts,
		ex:   executor.NewAsyncQueryServiceExecutor(zap.NewNop(), svc, i, ts),
		i:    i,
	}
}

func createSyncSystem() *system {
	svc := newFakeQueryService()
	i := inmem.NewService()
	ts := task.PlatformAdapter(backend.NewInMemStore(), backend.NopLogReader{}, noopRunCanceler{}, i, i, i)
	return &system{
		name: "SynchronousExecutor",
		svc:  svc,
		ts:   ts,
		ex: executor.NewQueryServiceExecutor(
			zap.NewNop(),
			query.QueryServiceBridge{
				AsyncQueryService: svc,
			},
			i,
			ts,
		),
		i: i,
	}
}

func TestExecutor(t *testing.T) {
	for _, fn := range []createSysFn{createAsyncSystem, createSyncSystem} {
		testExecutorQuerySuccess(t, fn)
		testExecutorQueryFailure(t, fn)
		testExecutorPromiseCancel(t, fn)
		testExecutorServiceError(t, fn)
		testExecutorWait(t, fn)
	}
}

// Some tests use t.Parallel, and the fake query service depends on unique scripts,
// so format a new script with the test name in each test.
const fmtTestScript = `
import "http"

option task = {
			name: %q,
			every: 1m,
}

from(bucket: "one") |> http.to(url: "http://example.com")`

func testExecutorQuerySuccess(t *testing.T, fn createSysFn) {
	sys := fn()
	tc := createCreds(t, sys.i)
	t.Run(sys.name+"/QuerySuccess", func(t *testing.T) {
		t.Parallel()

		script := fmt.Sprintf(fmtTestScript, t.Name())
		ctx := icontext.SetAuthorizer(context.Background(), tc.Auth)
		task, err := sys.ts.CreateTask(ctx, platform.TaskCreate{OrganizationID: tc.OrgID, Token: tc.Auth.Token, Flux: script})
		if err != nil {
			t.Fatal(err)
		}
		qr := backend.QueuedRun{TaskID: task.ID, RunID: platform.ID(1), Now: 123}
		rp, err := sys.ex.Execute(context.Background(), qr)
		if err != nil {
			t.Fatal(err)
		}

		if got := rp.Run(); !reflect.DeepEqual(qr, got) {
			t.Fatalf("unexpected queued run returned: %#v", got)
		}

		doneWaiting := make(chan struct{})
		go func() {
			_, _ = rp.Wait()
			close(doneWaiting)
		}()

		select {
		case <-doneWaiting:
			t.Fatal("Wait returned before query was unblocked")
		case <-time.After(10 * time.Millisecond):
			// Okay.
		}

		sys.svc.WaitForQueryLive(t, script)
		sys.svc.SucceedQuery(script)
		res, err := rp.Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := res.Err(); got != nil {
			t.Fatal(got)
		}

		res2, err := rp.Wait()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(res, res2) {
			t.Fatalf("second call to wait returned a different result: %#v", res2)
		}

		// The query must have received the appropriate authorizer.
		qa, err := icontext.GetAuthorizer(sys.svc.mostRecentCtx)
		if err != nil {
			t.Fatal(err)
		}
		if qa.Identifier() != tc.Auth.ID {
			t.Fatalf("expected query authorizer to have ID %v, got %v", tc.Auth.ID, qa.Identifier())
		}
	})
}

func testExecutorQueryFailure(t *testing.T, fn createSysFn) {
	sys := fn()
	tc := createCreds(t, sys.i)
	t.Run(sys.name+"/QueryFail", func(t *testing.T) {
		t.Parallel()
		script := fmt.Sprintf(fmtTestScript, t.Name())
		ctx := icontext.SetAuthorizer(context.Background(), tc.Auth)
		task, err := sys.ts.CreateTask(ctx, platform.TaskCreate{OrganizationID: tc.OrgID, Token: tc.Auth.Token, Flux: script})
		if err != nil {
			t.Fatal(err)
		}
		qr := backend.QueuedRun{TaskID: task.ID, RunID: platform.ID(1), Now: 123}
		rp, err := sys.ex.Execute(context.Background(), qr)
		if err != nil {
			t.Fatal(err)
		}

		expErr := errors.New("forced error")
		sys.svc.WaitForQueryLive(t, script)
		sys.svc.FailQuery(script, expErr)
		res, err := rp.Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := res.Err(); got != expErr {
			t.Fatalf("expected error %v; got %v", expErr, got)
		}
	})
}

func testExecutorPromiseCancel(t *testing.T, fn createSysFn) {
	sys := fn()
	tc := createCreds(t, sys.i)
	t.Run(sys.name+"/PromiseCancel", func(t *testing.T) {
		t.Parallel()
		script := fmt.Sprintf(fmtTestScript, t.Name())
		ctx := icontext.SetAuthorizer(context.Background(), tc.Auth)
		task, err := sys.ts.CreateTask(ctx, platform.TaskCreate{OrganizationID: tc.OrgID, Token: tc.Auth.Token, Flux: script})
		if err != nil {
			t.Fatal(err)
		}
		qr := backend.QueuedRun{TaskID: task.ID, RunID: platform.ID(1), Now: 123}
		rp, err := sys.ex.Execute(context.Background(), qr)
		if err != nil {
			t.Fatal(err)
		}

		rp.Cancel()

		res, err := rp.Wait()
		if err != backend.ErrRunCanceled {
			t.Fatalf("expected ErrRunCanceled, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result after cancel, got %#v", res)
		}
	})
}

func testExecutorServiceError(t *testing.T, fn createSysFn) {
	sys := fn()
	tc := createCreds(t, sys.i)
	t.Run(sys.name+"/ServiceError", func(t *testing.T) {
		t.Parallel()
		script := fmt.Sprintf(fmtTestScript, t.Name())
		ctx := icontext.SetAuthorizer(context.Background(), tc.Auth)
		task, err := sys.ts.CreateTask(ctx, platform.TaskCreate{OrganizationID: tc.OrgID, Token: tc.Auth.Token, Flux: script})
		if err != nil {
			t.Fatal(err)
		}
		qr := backend.QueuedRun{TaskID: task.ID, RunID: platform.ID(1), Now: 123}

		var forced = errors.New("forced")
		sys.svc.FailNextQuery(forced)
		rp, err := sys.ex.Execute(context.Background(), qr)
		if err == forced {
			// Expected err matches.
			if rp != nil {
				t.Fatalf("expected nil run promise when execution fails, got %v", rp)
			}
		} else if err == nil {
			// Synchronous query service always returns <rp>, nil.
			// Make sure rp.Wait returns the correct error.
			res, err := rp.Wait()
			if err != forced {
				t.Fatalf("expected forced error, got %v", err)
			}
			if res != nil {
				t.Fatalf("expected nil result on query service error, got %v", res)
			}
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func testExecutorWait(t *testing.T, createSys createSysFn) {
	// This is a longer delay than I'd prefer,
	// but it needs to be large-ish for slow machines running with the race detector.
	const waitCheckDelay = 100 * time.Millisecond

	// Other executor tests create a single sys and share it among subtests.
	// For this set of tests, we are testing Wait, which does not allow calling Execute concurrently,
	// so we make a new sys for each subtest.

	t.Run(createSys().name+"/Wait", func(t *testing.T) {
		t.Run("with nothing running", func(t *testing.T) {
			t.Parallel()
			sys := createSys()

			executorWaited := make(chan struct{})
			go func() {
				sys.ex.Wait()
				close(executorWaited)
			}()

			select {
			case <-executorWaited:
				// Okay.
			case <-time.After(waitCheckDelay):
				t.Fatalf("executor.Wait should have returned immediately")
			}
		})

		t.Run("cancel execute context", func(t *testing.T) {
			t.Parallel()
			sys := createSys()
			tc := createCreds(t, sys.i)

			ctx, ctxCancel := context.WithCancel(context.Background())
			defer ctxCancel()

			script := fmt.Sprintf(fmtTestScript, t.Name())
			ctx = icontext.SetAuthorizer(ctx, tc.Auth)
			task, err := sys.ts.CreateTask(ctx, platform.TaskCreate{OrganizationID: tc.OrgID, Token: tc.Auth.Token, Flux: script})
			if err != nil {
				t.Fatal(err)
			}
			qr := backend.QueuedRun{TaskID: task.ID, RunID: platform.ID(1), Now: 123}
			if _, err := sys.ex.Execute(ctx, qr); err != nil {
				t.Fatal(err)
			}

			executorWaited := make(chan struct{})
			go func() {
				sys.ex.Wait()
				close(executorWaited)
			}()

			select {
			case <-executorWaited:
				t.Fatalf("executor.Wait returned too early")
			case <-time.After(waitCheckDelay):
				// Okay.
			}

			ctxCancel()

			select {
			case <-executorWaited:
				// Okay.
			case <-time.After(waitCheckDelay):
				t.Fatalf("executor.Wait didn't return after execute context canceled")
			}
		})

		t.Run("cancel run promise", func(t *testing.T) {
			t.Parallel()
			sys := createSys()
			tc := createCreds(t, sys.i)

			ctx := icontext.SetAuthorizer(context.Background(), tc.Auth)

			script := fmt.Sprintf(fmtTestScript, t.Name())
			task, err := sys.ts.CreateTask(ctx, platform.TaskCreate{OrganizationID: tc.OrgID, Token: tc.Auth.Token, Flux: script})
			if err != nil {
				t.Fatal(err)
			}
			qr := backend.QueuedRun{TaskID: task.ID, RunID: platform.ID(1), Now: 123}
			rp, err := sys.ex.Execute(ctx, qr)
			if err != nil {
				t.Fatal(err)
			}

			executorWaited := make(chan struct{})
			go func() {
				sys.ex.Wait()
				close(executorWaited)
			}()

			select {
			case <-executorWaited:
				t.Fatalf("executor.Wait returned too early")
			case <-time.After(waitCheckDelay):
				// Okay.
			}

			rp.Cancel()

			select {
			case <-executorWaited:
				// Okay.
			case <-time.After(waitCheckDelay):
				t.Fatalf("executor.Wait didn't return after run promise canceled")
			}
		})

		t.Run("run success", func(t *testing.T) {
			t.Parallel()
			sys := createSys()
			tc := createCreds(t, sys.i)

			ctx := icontext.SetAuthorizer(context.Background(), tc.Auth)

			script := fmt.Sprintf(fmtTestScript, t.Name())
			task, err := sys.ts.CreateTask(ctx, platform.TaskCreate{OrganizationID: tc.OrgID, Token: tc.Auth.Token, Flux: script})
			if err != nil {
				t.Fatal(err)
			}
			qr := backend.QueuedRun{TaskID: task.ID, RunID: platform.ID(1), Now: 123}
			if _, err := sys.ex.Execute(ctx, qr); err != nil {
				t.Fatal(err)
			}

			executorWaited := make(chan struct{})
			go func() {
				sys.ex.Wait()
				close(executorWaited)
			}()

			select {
			case <-executorWaited:
				t.Fatalf("executor.Wait returned too early")
			case <-time.After(waitCheckDelay):
				// Okay.
			}

			sys.svc.WaitForQueryLive(t, script)
			sys.svc.SucceedQuery(script)

			select {
			case <-executorWaited:
				// Okay.
			case <-time.After(waitCheckDelay):
				t.Fatalf("executor.Wait didn't return after query succeeded")
			}
		})

		t.Run("run failure", func(t *testing.T) {
			t.Parallel()
			sys := createSys()
			tc := createCreds(t, sys.i)

			script := fmt.Sprintf(fmtTestScript, t.Name())
			ctx := icontext.SetAuthorizer(context.Background(), tc.Auth)
			task, err := sys.ts.CreateTask(ctx, platform.TaskCreate{OrganizationID: tc.OrgID, Token: tc.Auth.Token, Flux: script})
			if err != nil {
				t.Fatal(err)
			}
			qr := backend.QueuedRun{TaskID: task.ID, RunID: platform.ID(1), Now: 123}
			if _, err := sys.ex.Execute(ctx, qr); err != nil {
				t.Fatal(err)
			}

			executorWaited := make(chan struct{})
			go func() {
				sys.ex.Wait()
				close(executorWaited)
			}()

			select {
			case <-executorWaited:
				t.Fatalf("executor.Wait returned too early")
			case <-time.After(waitCheckDelay):
				// Okay.
			}

			sys.svc.WaitForQueryLive(t, script)
			sys.svc.FailQuery(script, errors.New("forced"))

			select {
			case <-executorWaited:
				// Okay.
			case <-time.After(waitCheckDelay):
				t.Fatalf("executor.Wait didn't return after query failed")
			}
		})
	})
}

type testCreds struct {
	OrgID, UserID platform.ID
	Auth          *platform.Authorization
}

func createCreds(t *testing.T, i *inmem.Service) testCreds {
	t.Helper()

	org := &platform.Organization{Name: t.Name() + "-org"}
	if err := i.CreateOrganization(context.Background(), org); err != nil {
		t.Fatal(err)
	}

	user := &platform.User{Name: t.Name() + "-user"}
	if err := i.CreateUser(context.Background(), user); err != nil {
		t.Fatal(err)
	}

	readPerm, err := platform.NewGlobalPermission(platform.ReadAction, platform.BucketsResourceType)
	if err != nil {
		t.Fatal(err)
	}
	writePerm, err := platform.NewGlobalPermission(platform.WriteAction, platform.BucketsResourceType)
	if err != nil {
		t.Fatal(err)
	}
	auth := &platform.Authorization{
		OrgID:       org.ID,
		UserID:      user.ID,
		Token:       "hifriend!",
		Permissions: []platform.Permission{*readPerm, *writePerm},
	}
	if err := i.CreateAuthorization(context.Background(), auth); err != nil {
		t.Fatal(err)
	}

	return testCreds{OrgID: org.ID, Auth: auth}
}
