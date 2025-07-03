package policy

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/internal/testutil"
)

func TestIsFailureForNil(t *testing.T) {
	policy := BaseFailurePolicy[any]{}

	assert.False(t, policy.IsFailure(nil, nil, nil))
}

func TestIsFailureForError(t *testing.T) {
	policy := BaseFailurePolicy[any]{}
	assert.True(t, policy.IsFailure(nil, nil, errors.New("test")))
	assert.True(t, policy.IsFailure(nil, nil, testutil.ErrInvalidState))

	policy.HandleErrors(testutil.ErrInvalidArgument)
	assert.True(t, policy.IsFailure(nil, nil, testutil.ErrInvalidArgument))
	assert.False(t, policy.IsFailure(nil, nil, errors.New("test")))
}

func TestIsFailureForResult(t *testing.T) {
	policy := BaseFailurePolicy[any]{}
	policy.HandleResult(10)

	assert.True(t, policy.IsFailure(nil, 10, nil))
	assert.False(t, policy.IsFailure(nil, 5, nil))
}

func TestIsFailureForPredicate(t *testing.T) {
	policy := BaseFailurePolicy[any]{}
	policy.HandleIf(func(exec failsafe.ExecutionAttempt[any], result any, err error) bool {
		return result == "test" || errors.Is(err, testutil.ErrInvalidArgument)
	})

	assert.True(t, policy.IsFailure(nil, "test", nil))
	assert.False(t, policy.IsFailure(nil, 0, nil))
	assert.True(t, policy.IsFailure(nil, nil, testutil.ErrInvalidArgument))
	assert.False(t, policy.IsFailure(nil, nil, testutil.ErrInvalidState))
}

func TestShouldComputeDelay(t *testing.T) {
	expected := 5 * time.Millisecond
	policy := BaseDelayablePolicy[any]{
		DelayFunc: func(exec failsafe.ExecutionAttempt[any]) time.Duration {
			return expected
		},
	}

	assert.Equal(t, expected, policy.ComputeDelay(testutil.TestExecution[any]{
		TheLastResult: true,
	}))
	assert.Equal(t, time.Duration(-1), policy.ComputeDelay(nil))
}

func TestIsAbortableNil(t *testing.T) {
	policy := BaseAbortablePolicy[any]{}

	assert.False(t, policy.IsAbortable(nil, nil, nil))
}

func TestIsAbortableForError(t *testing.T) {
	policy := BaseAbortablePolicy[any]{}
	policy.AbortOnErrors(testutil.ErrInvalidArgument)

	assert.True(t, policy.IsAbortable(nil, nil, testutil.ErrInvalidArgument))
	assert.True(t, policy.IsAbortable(nil, nil, testutil.CompositeError{Cause: testutil.ErrInvalidArgument}))
	assert.False(t, policy.IsAbortable(nil, nil, testutil.ErrConnecting))
}

func TestIsAbortableForResult(t *testing.T) {
	policy := BaseAbortablePolicy[any]{}
	policy.AbortOnResult(10)

	assert.True(t, policy.IsAbortable(nil, 10, nil))
	assert.False(t, policy.IsAbortable(nil, 5, nil))
	assert.False(t, policy.IsAbortable(nil, 5, testutil.ErrInvalidState))
}

func TestIsAbortableForPredicate(t *testing.T) {
	policy := BaseAbortablePolicy[any]{}
	policy.AbortIf(func(exec failsafe.ExecutionAttempt[any], s any, err error) bool {
		return s == "test" || errors.Is(err, testutil.ErrInvalidArgument)
	})

	assert.True(t, policy.IsAbortable(nil, "test", nil))
	assert.False(t, policy.IsAbortable(nil, 0, nil))
	assert.True(t, policy.IsAbortable(nil, "", testutil.ErrInvalidArgument))
	assert.False(t, policy.IsAbortable(nil, "", testutil.ErrInvalidState))
}
