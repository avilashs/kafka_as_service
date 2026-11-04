package workflows

import (
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/avilashs/kafka_as_service/internal/platform/engine"
	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

// Input is the workflow argument for cluster operations.
type Input struct {
	OperationID string `json:"operationId"`
	ClusterID   string `json:"clusterId"`
}

// Timeouts; variables so tests can shorten them. A worker that dies mid-activity
// is detected after ShortActivityTimeout (short activities) or WaitHeartbeatTimeout
// (waits), after which Temporal retries the activity on another worker.
var (
	ReadyTimeout         = 45 * time.Minute
	DeletedTimeout       = 30 * time.Minute
	ShortActivityTimeout = 30 * time.Second
	WaitHeartbeatTimeout = 30 * time.Second
)

func shortOpts(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: ShortActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2,
			MaximumInterval:        time.Minute,
			MaximumAttempts:        30,
			NonRetryableErrorTypes: []string{TerminalErrorType},
		},
	})
}

func waitOpts(ctx workflow.Context, timeout time.Duration) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: timeout,
		StartToCloseTimeout:    timeout,
		HeartbeatTimeout:       WaitHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2,
			MaximumInterval:        30 * time.Second,
			NonRetryableErrorTypes: []string{TerminalErrorType},
		},
	})
}

func opError(err error) store.OpError {
	var appErr *temporal.ApplicationError
	retryable := true
	code := "INTERNAL"
	if errors.As(err, &appErr) {
		retryable = !appErr.NonRetryable()
		if appErr.Type() == TerminalErrorType {
			code = "TERMINAL"
		}
	}
	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) {
		code = "TIMEOUT"
	}
	return store.OpError{Code: code, Message: rootMessage(err), Retryable: retryable}
}

func rootMessage(err error) string {
	for {
		u := errors.Unwrap(err)
		if u == nil {
			return err.Error()
		}
		err = u
	}
}

// CreateCluster: reserve → render+publish gen 1 → wait Ready → commit.
// Any failure after the reservation compensates by tombstoning and waiting for deletion.
func CreateCluster(ctx workflow.Context, in Input) error {
	var a *Activities
	sctx := shortOpts(ctx)
	step := func(name string) {
		_ = workflow.ExecuteActivity(sctx, ActMarkStep, in.OperationID, name).Get(ctx, nil)
	}

	step("reserve-capacity")
	var as engine.Assignment
	if err := workflow.ExecuteActivity(sctx, a.ReserveCapacity, in.OperationID, in.ClusterID).Get(ctx, &as); err != nil {
		// Nothing was published; release whatever may have been reserved.
		return failCreate(ctx, in, err, true)
	}

	step("publish-desired-state")
	var rev int64
	if err := workflow.ExecuteActivity(sctx, a.RenderAndPublish, in.OperationID, in.ClusterID, int64(1)).Get(ctx, &rev); err != nil {
		return compensateCreate(ctx, in, err)
	}

	step("wait-ready")
	var st engine.Status
	if err := workflow.ExecuteActivity(waitOpts(ctx, ReadyTimeout), a.WaitForCondition, in.OperationID, in.ClusterID, int64(1), engine.Condition{Type: "Ready"}).Get(ctx, &st); err != nil {
		return compensateCreate(ctx, in, err)
	}

	step("finish")
	return workflow.ExecuteActivity(sctx, a.FinishCreate, in.OperationID, in.ClusterID).Get(ctx, nil)
}

func compensateCreate(ctx workflow.Context, in Input, cause error) error {
	var a *Activities
	sctx := shortOpts(ctx)
	_ = workflow.ExecuteActivity(sctx, ActMarkStep, in.OperationID, "compensate").Get(ctx, nil)
	var gen int64
	cleaned := false
	if err := workflow.ExecuteActivity(sctx, a.PublishTombstone, in.OperationID, in.ClusterID).Get(ctx, &gen); err == nil {
		var st engine.Status
		if err := workflow.ExecuteActivity(waitOpts(ctx, DeletedTimeout), a.WaitForCondition, in.OperationID, in.ClusterID, gen, engine.Condition{Type: "Deleted"}).Get(ctx, &st); err == nil {
			cleaned = true
			_ = workflow.ExecuteActivity(sctx, a.DeleteClusterSecrets, in.ClusterID).Get(ctx, nil)
		}
	}
	return failCreate(ctx, in, cause, cleaned)
}

func failCreate(ctx workflow.Context, in Input, cause error, cleaned bool) error {
	var a *Activities
	if err := workflow.ExecuteActivity(shortOpts(ctx), a.FailCreate, in.OperationID, in.ClusterID, opError(cause), cleaned).Get(ctx, nil); err != nil {
		return err
	}
	return nil // the operation records the failure; the workflow itself completed its job
}

// DeleteCluster: tombstone → wait Deleted → soft-delete and release capacity.
func DeleteCluster(ctx workflow.Context, in Input) error {
	var a *Activities
	sctx := shortOpts(ctx)
	_ = workflow.ExecuteActivity(sctx, ActMarkStep, in.OperationID, "publish-tombstone").Get(ctx, nil)
	var gen int64
	if err := workflow.ExecuteActivity(sctx, a.PublishTombstone, in.OperationID, in.ClusterID).Get(ctx, &gen); err != nil {
		return workflow.ExecuteActivity(sctx, a.FailDelete, in.OperationID, in.ClusterID, opError(err)).Get(ctx, nil)
	}
	_ = workflow.ExecuteActivity(sctx, ActMarkStep, in.OperationID, "wait-deleted").Get(ctx, nil)
	var st engine.Status
	if err := workflow.ExecuteActivity(waitOpts(ctx, DeletedTimeout), a.WaitForCondition, in.OperationID, in.ClusterID, gen, engine.Condition{Type: "Deleted"}).Get(ctx, &st); err != nil {
		return workflow.ExecuteActivity(sctx, a.FailDelete, in.OperationID, in.ClusterID, opError(err)).Get(ctx, nil)
	}
	_ = workflow.ExecuteActivity(sctx, ActMarkStep, in.OperationID, "delete-secrets").Get(ctx, nil)
	if err := workflow.ExecuteActivity(sctx, a.DeleteClusterSecrets, in.ClusterID).Get(ctx, nil); err != nil {
		return workflow.ExecuteActivity(sctx, a.FailDelete, in.OperationID, in.ClusterID, opError(err)).Get(ctx, nil)
	}
	_ = workflow.ExecuteActivity(sctx, ActMarkStep, in.OperationID, "finish").Get(ctx, nil)
	return workflow.ExecuteActivity(sctx, a.FinishDelete, in.OperationID, in.ClusterID).Get(ctx, nil)
}

// UpdateCluster applies a change to a live cluster's spec: prepare secrets, publish the new
// generation, wait until the engine reports it Ready, then finish. The spec was changed (and the
// cluster locked and marked UPDATING) by the API in one transaction, so a failure here leaves the
// desired state in place and is reported on the operation.
func UpdateCluster(ctx workflow.Context, in Input) error {
	var a *Activities
	sctx := shortOpts(ctx)
	fail := func(cause error) error {
		return workflow.ExecuteActivity(sctx, a.FailUpdate, in.OperationID, in.ClusterID, opError(cause)).Get(ctx, nil)
	}
	_ = workflow.ExecuteActivity(sctx, ActMarkStep, in.OperationID, "prepare").Get(ctx, nil)
	if err := workflow.ExecuteActivity(sctx, a.PrepareUpdate, in.OperationID, in.ClusterID).Get(ctx, nil); err != nil {
		return fail(err)
	}
	_ = workflow.ExecuteActivity(sctx, ActMarkStep, in.OperationID, "publish-desired-state").Get(ctx, nil)
	var gen int64
	if err := workflow.ExecuteActivity(sctx, a.RenderAndPublish, in.OperationID, in.ClusterID, int64(0)).Get(ctx, &gen); err != nil {
		return fail(err)
	}
	_ = workflow.ExecuteActivity(sctx, ActMarkStep, in.OperationID, "wait-ready").Get(ctx, nil)
	var st engine.Status
	if err := workflow.ExecuteActivity(waitOpts(ctx, ReadyTimeout), a.WaitForCondition, in.OperationID, in.ClusterID, gen, engine.Condition{Type: "Ready"}).Get(ctx, &st); err != nil {
		return fail(err)
	}
	_ = workflow.ExecuteActivity(sctx, ActMarkStep, in.OperationID, "finish").Get(ctx, nil)
	if err := workflow.ExecuteActivity(sctx, a.FinishUpdate, in.OperationID, in.ClusterID).Get(ctx, nil); err != nil {
		return fail(err)
	}
	return nil
}

// Register adds workflows and activities to a worker.
func Register(w worker.Registry, a *Activities) {
	w.RegisterWorkflowWithOptions(CreateCluster, workflow.RegisterOptions{Name: "CreateCluster"})
	w.RegisterWorkflowWithOptions(DeleteCluster, workflow.RegisterOptions{Name: "DeleteCluster"})
	w.RegisterWorkflowWithOptions(UpdateCluster, workflow.RegisterOptions{Name: "UpdateCluster"})
	w.RegisterActivity(a)
}
