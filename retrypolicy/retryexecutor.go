package retrypolicy

import (
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/common"
	"github.com/failsafe-go/failsafe-go/internal"
	"github.com/failsafe-go/failsafe-go/internal/util"
	"github.com/failsafe-go/failsafe-go/policy"
)

// executor is a policy.Executor that handles failures according to a RetryPolicy.
type executor[R any] struct {
	*policy.BaseExecutor[R]
	*retryPolicy[R]

	// Mutable state
	failedAttempts  atomic.Int32
	retriesExceeded atomic.Bool
	lastDelay       atomic.Int64 // Store as nanoseconds
}

var _ policy.Executor[any] = &executor[any]{}

func (e *executor[R]) Apply(innerFn func(failsafe.Execution[R]) *common.PolicyResult[R]) func(failsafe.Execution[R]) *common.PolicyResult[R] {
	return func(exec failsafe.Execution[R]) *common.PolicyResult[R] {
		execInternal := exec.(policy.ExecutionInternal[R])

		for {
			result := innerFn(exec)
			if canceled, cancelResult := execInternal.IsCanceledWithResult(); canceled {
				return cancelResult
			}
			if e.retriesExceeded.Load() {
				return result
			}

			result = e.PostExecute(execInternal, result)
			if result.Done {
				return result
			}

			// Record result
			if cancelResult := execInternal.RecordResult(result); cancelResult != nil {
				return cancelResult
			}

			// Delay
			delay := e.getDelay(exec)
			if e.onRetryScheduled != nil {
				e.onRetryScheduled(failsafe.ExecutionScheduledEvent[R]{
					ExecutionAttempt: execInternal.CopyWithResult(result),
					Delay:            delay,
				})
			}
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-exec.Canceled():
				timer.Stop()
			}

			// Prepare for next iteration
			if cancelResult := execInternal.InitializeRetry(); cancelResult != nil {
				return cancelResult
			}

			// Call retry listener
			if e.onRetry != nil {
				e.onRetry(failsafe.ExecutionEvent[R]{ExecutionAttempt: execInternal.CopyWithResult(result)})
			}
		}
	}
}

// OnFailure updates failedAttempts and retriesExceeded, and calls event listeners
func (e *executor[R]) OnFailure(exec policy.ExecutionInternal[R], result *common.PolicyResult[R]) *common.PolicyResult[R] {
	e.BaseExecutor.OnFailure(exec, result)

	e.failedAttempts.Add(1)
	maxRetriesExceeded := e.maxRetries != -1 && e.failedAttempts.Load() > int32(e.maxRetries)
	maxDurationExceeded := e.maxDuration != 0 && exec.ElapsedTime() > e.maxDuration
	e.retriesExceeded.Store(maxRetriesExceeded || maxDurationExceeded)
	isAbortable := e.IsAbortable(exec, result.Result, result.Error)
	shouldRetry := !isAbortable && !e.retriesExceeded.Load() && e.allowsRetries()
	done := isAbortable || !shouldRetry

	// Call listeners
	if isAbortable && e.onAbort != nil {
		e.onAbort(failsafe.ExecutionEvent[R]{ExecutionAttempt: exec.CopyWithResult(result)})
	}
	if e.retriesExceeded.Load() {
		if !isAbortable && e.onRetriesExceeded != nil {
			e.onRetriesExceeded(failsafe.ExecutionEvent[R]{ExecutionAttempt: exec.CopyWithResult(result)})
		}
		if !e.returnLastFailure {
			return internal.FailureResult[R](ExceededError{
				LastResult: result.Result,
				LastError:  result.Error,
			})
		}
	}
	return result.WithDone(done, false)
}

// getDelay updates lastDelay and returns the new delay
func (e *executor[R]) getDelay(exec failsafe.ExecutionAttempt[R]) time.Duration {
	var delay time.Duration
	if computedDelay := e.ComputeDelay(exec); computedDelay != -1 {
		delay = computedDelay
	} else {
		delay = e.getFixedOrRandomDelay(exec)
	}
	if delay != 0 {
		delay = e.adjustForJitter(delay)
	}
	delay = e.adjustForMaxDuration(delay, exec.ElapsedTime())
	return delay
}

func (e *executor[R]) getFixedOrRandomDelay(exec failsafe.ExecutionAttempt[R]) time.Duration {
	if e.Delay != 0 {
		// Adjust for backoffs
		ld := e.lastDelay.Load()
		if ld != 0 && exec.Retries() >= 1 && e.maxDelay != 0 {
			backoffDelay := time.Duration(float32(ld) * e.delayFactor)
			e.lastDelay.Store(int64(min(backoffDelay, e.maxDelay)))
		} else {
			e.lastDelay.Store(int64(e.Delay))
		}
		return time.Duration(e.lastDelay.Load())
	}
	if e.delayMin != 0 && e.delayMax != 0 {
		return time.Duration(util.RandomDelayInRange(e.delayMin.Nanoseconds(), e.delayMax.Nanoseconds(), rand.Float64()))
	}
	return 0
}

func (e *executor[R]) adjustForJitter(delay time.Duration) time.Duration {
	if e.jitter != 0 {
		delay = util.RandomDelay(delay, e.jitter, rand.Float64())
	} else if e.jitterFactor != 0 {
		delay = util.RandomDelayFactor(delay, e.jitterFactor, rand.Float32())
	}
	return delay
}

func (e *executor[R]) adjustForMaxDuration(delay time.Duration, elapsed time.Duration) time.Duration {
	if e.maxDuration != 0 {
		delay = min(delay, e.maxDuration-elapsed)
	}
	return max(0, delay)
}
