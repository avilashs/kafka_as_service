// Package outbox starts workflows for operations committed to Postgres. The API
// writes the operation and an outbox row in one transaction; this dispatcher turns
// rows into Temporal workflow starts. The workflow ID is the operation ID, so a
// redelivered row can never start a second workflow.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/avilashs/kafka_as_service/internal/platform/store"
	"github.com/avilashs/kafka_as_service/internal/platform/workflows"
)

// Dispatcher polls the outbox.
type Dispatcher struct {
	Store    *store.Store
	Temporal client.Client
	Interval time.Duration
	Log      *slog.Logger
}

type payload struct {
	Workflow    string `json:"workflow"`
	OperationID string `json:"operationId"`
	ClusterID   string `json:"clusterId"`
}

// Run dispatches until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	iv := d.Interval
	if iv == 0 {
		iv = 500 * time.Millisecond
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		if _, err := d.Once(ctx); err != nil && ctx.Err() == nil {
			d.Log.Warn("outbox dispatch failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Once dispatches one batch.
func (d *Dispatcher) Once(ctx context.Context) (int, error) {
	return d.Store.DispatchOutbox(ctx, 50, func(e store.OutboxEntry) error {
		if e.Kind != "start_workflow" {
			return fmt.Errorf("unknown outbox kind %q", e.Kind)
		}
		var p payload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return err
		}
		_, err := d.Temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:                    p.OperationID,
			TaskQueue:             workflows.TaskQueue,
			WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		}, p.Workflow, workflows.Input{OperationID: p.OperationID, ClusterID: p.ClusterID})
		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &already) {
			return nil
		}
		if err == nil {
			d.Log.Info("workflow started", "workflow", p.Workflow, "operation", p.OperationID)
		}
		return err
	})
}
