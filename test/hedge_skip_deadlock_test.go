package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/hedgepolicy"
)

// Regression test for a deadlock in the hedge executor when:
//   1. OnHedge returns false (so the hedge iteration is skipped, shouldSkip=true),
//      and
//   2. An earlier execution completes with a non-abortable error while
//      execIdx < maxHedges (so the inner goroutine's shouldSend=false branch
//      holds back sending, expecting the outer loop to start another hedge).
//
// Before the fix, the outer loop's final unconditional `<-resultChan` would
// block forever because no goroutine would ever send. Nothing — not even
// parent-context cancellation — could unblock it, so the caller's goroutine
// leaked along with any deferred cleanup it was responsible for. The fix
// adds a `<-parentExecution.Context().Done()` case to the receive so that
// ctx cancellation unblocks the wait.
func TestHedgeDoesNotDeadlockWhenHedgesSkippedAndPrimaryNonAbortable(t *testing.T) {
	nonAbortableErr := errors.New("transient-not-abortable")

	hp := hedgepolicy.BuilderWithDelay[string](20*time.Millisecond).
		WithMaxHedges(1).
		OnHedge(func(failsafe.ExecutionEvent[string]) bool {
			// Skip every hedge — mirrors erpc's OnHedge returning false for
			// composite/write requests.
			return false
		}).
		CancelIf(func(_ failsafe.ExecutionAttempt[string], _ string, err error) bool {
			// Non-abortable for all errors: matches the "transient error,
			// let another hedge try" branch in hedgeexecutor.go:77.
			return false
		}).
		Build()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = failsafe.NewExecutor[string](hp).
			WithContext(ctx).
			GetWithExecution(func(exec failsafe.Execution[string]) (string, error) {
				// Return the non-abortable error promptly so the inner
				// goroutine takes the shouldSend=false branch before the
				// hedge-delay timer fires.
				return "", nonAbortableErr
			})
	}()

	// Let the deadlock form (primary finishes, hedge is skipped, outer loop
	// parks on the unconditional receive).
	time.Sleep(100 * time.Millisecond)

	// Cancel the parent context. After the fix, the outer receive selects on
	// ctx.Done() and returns promptly.
	cancel()

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		assert.Fail(t, "hedge executor deadlocked: Apply did not return after parent context was canceled")
	}
}
