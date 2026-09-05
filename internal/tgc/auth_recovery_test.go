package tgc

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsAuthKeyUnregistered(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"sentinel error", ErrAuthKeyUnregistered, true},
		{"wrapped sentinel error", fmt.Errorf("streaming: %w", ErrAuthKeyUnregistered), true},
		{"rpc error string", errors.New("rpc error code 401: AUTH_KEY_UNREGISTERED"), true},
		{"auth_recovery cooldown message", errors.New("auth_recovery: session invalidated, waiting for cooldown"), true},
		{"unrelated error", errors.New("connection reset by peer"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsAuthKeyUnregistered(tt.err))
		})
	}
}

func TestIsMessageIdsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"rpc error string", errors.New("rpc error code 400: MESSAGE_IDS_EMPTY"), true},
		{"wrapped error", fmt.Errorf("callback: %w", errors.New("MESSAGE_IDS_EMPTY")), true},
		{"unrelated error", errors.New("file parts mismatch"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsMessageIdsEmpty(tt.err))
		})
	}
}

func TestIsFilePartsMismatch(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"exact getParts error", errors.New("file parts mismatch"), true},
		{"wrapped error", fmt.Errorf("stream.parts_fetch_failed error=%w", errors.New("file parts mismatch")), true},
		{"unrelated error", errors.New("MESSAGE_IDS_EMPTY"), false},
		{"auth error", ErrAuthKeyUnregistered, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsFilePartsMismatch(tt.err))
		})
	}
}
