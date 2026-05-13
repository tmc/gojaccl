package jaccl

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestErrorContext(t *testing.T) {
	t.Run("IncludesPackagePrefix", func(t *testing.T) {
		err := wrapError(-1, "test", errors.New("boom"))
		if !strings.HasPrefix(err.Error(), "jaccl: ") {
			t.Fatalf("error = %q", err)
		}
	})
	t.Run("IncludesRankWhenKnown", func(t *testing.T) {
		err := wrapError(1, "barrier", errors.New("boom"))
		if !strings.Contains(err.Error(), "rank 1") {
			t.Fatalf("error = %q", err)
		}
	})
	t.Run("IncludesOperation", func(t *testing.T) {
		err := wrapError(1, "all sum", errors.New("boom"))
		if !strings.Contains(err.Error(), "all sum") {
			t.Fatalf("error = %q", err)
		}
	})
	t.Run("WrapsContextError", func(t *testing.T) {
		err := wrapError(1, "barrier", context.Canceled)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("errors.Is(%v, context.Canceled) = false", err)
		}
	})
}
