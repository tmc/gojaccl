package jaccl

import "testing"

func TestAvailable(t *testing.T) {
	t.Run("NeverPanics", func(t *testing.T) {
		_ = Available()
	})
	t.Run("FalseWhenBackendUnavailable", func(t *testing.T) {
		old := backendFactory
		backendFactory = old
		t.Cleanup(func() { backendFactory = old })
		_ = Available()
	})
}
