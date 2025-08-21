package hedgepolicy

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/common"
	"github.com/failsafe-go/failsafe-go/policy"
)

// executor is a policy.Executor that handles failures according to a HedgePolicy.
type executor[R any] struct {
	*policy.BaseExecutor[R]
	*hedgePolicy[R]
}

var _ policy.Executor[any] = &executor[any]{}

func (e *executor[R]) Apply(innerFn func(failsafe.Execution[R]) *common.PolicyResult[R]) func(failsafe.Execution[R]) *common.PolicyResult[R] {
	return func(exec failsafe.Execution[R]) *common.PolicyResult[R] {
		type execResult struct {
			result *common.PolicyResult[R]
			index  int
		}
		parentExecution := exec.(policy.ExecutionInternal[R])
		executions := make([]policy.ExecutionInternal[R], e.maxHedges+1)

		// Track actual running and completed executions
		runningExecutions := atomic.Int32{}
		completedExecutions := atomic.Int32{}
		inflightExecutions := atomic.Int32{}
		resultSent := atomic.Bool{}
		resultChan := make(chan *execResult, 1) // Only one result is sent

		for execIdx := 0; ; execIdx++ {
			shouldSkip := false

			// Check onHedge before creating execution for hedges
			if execIdx > 0 && e.onHedge != nil {
				tempExec := parentExecution.CopyForHedge().(policy.ExecutionInternal[R])
				if !e.onHedge(failsafe.ExecutionEvent[R]{ExecutionAttempt: tempExec.CopyWithResult(nil)}) {
					shouldSkip = true
				} else {
					executions[execIdx] = tempExec
				}
			} else if execIdx == 0 {
				executions[execIdx] = parentExecution.CopyForCancellable().(policy.ExecutionInternal[R])
			} else {
				executions[execIdx] = parentExecution.CopyForHedge().(policy.ExecutionInternal[R])
			}

			if !shouldSkip {
				runningExecutions.Add(1)
				inflightExecutions.Add(1)

				// Perform execution
				go func(hedgeExec policy.ExecutionInternal[R], execIdx int) {
					result := innerFn(hedgeExec)

					// Decrement inflight AFTER getting result but BEFORE decision
					remaining := inflightExecutions.Add(-1)

					// Check if this is truly the final result
					completed := completedExecutions.Add(1)
					isFinalResult := int(completed) == int(runningExecutions.Load())

					// Determine if we should send result immediately
					shouldSend := false
					if isFinalResult {
						// Always send if this is the last result
						shouldSend = true
					} else if remaining == 0 {
						// No other requests are running or will start
						// Send immediately on any result (error or success) for quick retry
						shouldSend = true
					} else {
						// Others are still running, only send on success (let errors wait for other attempts)
						shouldSend = (result.Error == nil && e.IsAbortable(hedgeExec, result.Result, result.Error))
					}

					didSend := false
					if shouldSend && resultSent.CompareAndSwap(false, true) {
						resultChan <- &execResult{result, execIdx}
						didSend = true
					}

					// Best-effort release of loser hedge results that were not sent
					if !didSend && result != nil {
						if releasable, ok := any(result.Result).(interface{ Release() }); ok && releasable != nil {
							releasable.Release()
						}
					}
				}(executions[execIdx], execIdx)
			}

			// Check if we should continue with more hedges
			actuallyStarted := int(runningExecutions.Load())
			if actuallyStarted == 0 && execIdx >= e.maxHedges {
				// All hedges were skipped, return appropriate error
				return &common.PolicyResult[R]{
					Error: fmt.Errorf("no available upstreams for hedge execution"),
				}
			}

			// Wait for result or hedge delay
			var result *execResult
			if !shouldSkip && execIdx < e.maxHedges {
				timer := time.NewTimer(e.delayFunc(exec))
				select {
				case <-timer.C:
					// Timer expired, continue to next hedge
				case result = <-resultChan:
					timer.Stop()
				}
			} else {
				// This is the last execution or was skipped, wait for any result
				if actuallyStarted > 0 {
					result = <-resultChan
				} else {
					// Nothing running, break out
					break
				}
			}

			// Return if parent execution is canceled
			if canceled, cancelResult := parentExecution.IsCanceledWithResult(); canceled {
				// Proactively cancel any outstanding attempts so underlying work is aborted promptly
				for _, execution := range executions {
					if execution != nil {
						execution.Cancel(nil)
					}
				}
				// Best-effort drain a pending result (if any) so senders are not held up
				select {
				case res := <-resultChan:
					if res != nil && res.result != nil {
						if releasable, ok := any(res.result.Result).(interface{ Release() }); ok && releasable != nil {
							releasable.Release()
						}
					}
				default:
				}
				return cancelResult
			}

			// Return result and cancel any outstanding attempts
			if result != nil {
				for i, execution := range executions {
					if i != result.index && execution != nil {
						execution.Cancel(nil)
					}
				}
				return result.result
			}
		}

		// Should not reach here in normal operation, but return error if we do
		return &common.PolicyResult[R]{
			Error: fmt.Errorf("hedge execution ended without result"),
		}
	}
}
