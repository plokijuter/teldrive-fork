package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/tgdrive/teldrive/internal/recovery"
)

func TestReadTimeoutRecovers(t *testing.T) {
	for name, input := range map[string]bin.Encoder{
		"file": &tg.UploadGetFileRequest{}, "messages": &tg.ChannelsGetMessagesRequest{}, "channels": &tg.ChannelsGetChannelsRequest{},
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			invoke := New(3).Handle(telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
				calls++
				if calls == 1 {
					return fmt.Errorf("wrapped RPC: %w", tgerr.New(-503, "Timeout"))
				}
				return nil
			}))
			if err := invoke(context.Background(), input, nil); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatalf("calls = %d, want 2", calls)
			}
		})
	}
}

func TestReadTimeoutBudgetWithRecovery(t *testing.T) {
	calls, recoveries := 0, 0
	cause := tgerr.New(-503, "Timeout")
	inner := New(5).Handle(telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error { calls++; return cause }))
	outer := recovery.New(func() backoff.BackOff { recoveries++; return backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 1) }).Handle(inner)
	err := outer(context.Background(), &tg.UploadGetFileRequest{}, nil)
	if calls != 3 {
		t.Fatalf("calls = %d, want total budget 3", calls)
	}
	if recoveries != 1 {
		t.Fatalf("backoff instances = %d", recoveries)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("lost RPC cause: %v", err)
	}
	if rpc, ok := tgerr.As(err); !ok || rpc != cause {
		t.Fatalf("RPC no longer identifiable: %v", err)
	}
}

func TestReadTimeoutCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	invoke := New(5).Handle(telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		calls++
		cancel()
		return tgerr.New(-503, "Timeout")
	}))
	start := time.Now()
	if err := invoke(ctx, &tg.UploadGetFileRequest{}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if calls != 1 || time.Since(start) > time.Second {
		t.Fatalf("cancellation did not stop backoff: calls=%d", calls)
	}
}

func TestTimeoutScope(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input bin.Encoder
		err   error
	}{
		{"upload", &tg.UploadSaveFilePartRequest{}, tgerr.New(-503, "Timeout")},
		{"big upload", &tg.UploadSaveBigFilePartRequest{}, tgerr.New(-503, "Timeout")},
		{"send", &tg.MessagesSendMessageRequest{}, tgerr.New(-503, "Timeout")},
		{"delete", &tg.ChannelsDeleteMessagesRequest{}, tgerr.New(-503, "Timeout")},
		{"other read", &tg.MessagesGetMessagesRequest{}, tgerr.New(-503, "Timeout")},
		{"different code", &tg.UploadGetFileRequest{}, tgerr.New(503, "Timeout")},
		{"different message", &tg.UploadGetFileRequest{}, tgerr.New(-503, "TIMEOUT")},
		{"substring", &tg.UploadGetFileRequest{}, tgerr.New(-503, "Timeout extra")},
		{"plain error", &tg.UploadGetFileRequest{}, errors.New("rpc error code -503: Timeout")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			invoke := New(3).Handle(telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error { calls++; return tc.err }))
			err := invoke(context.Background(), tc.input, nil)
			if calls != 1 || !errors.Is(err, tc.err) {
				t.Fatalf("calls=%d error=%v", calls, err)
			}
		})
	}
}

func TestExistingRetryPreservesCause(t *testing.T) {
	cause := tgerr.New(500, "RPC_CALL_FAIL")
	calls := 0
	invoke := New(2).Handle(telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error { calls++; return cause }))
	if err := invoke(context.Background(), &tg.UploadGetFileRequest{}, nil); !errors.Is(err, cause) {
		t.Fatalf("lost existing retry cause: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestReadTimeoutAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	invoke := New(5).Handle(telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error { calls++; return tgerr.New(-503, "Timeout") }))
	if err := invoke(ctx, &tg.UploadGetFileRequest{}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if calls != 0 {
		t.Fatalf("invoked canceled request %d times", calls)
	}
}

func TestReadTimeoutHonorsSmallerBudgets(t *testing.T) {
	for _, max := range []int{1, 2} {
		t.Run(fmt.Sprint(max), func(t *testing.T) {
			calls := 0
			cause := tgerr.New(-503, "Timeout")
			invoke := New(max).Handle(telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error { calls++; return cause }))
			if err := invoke(context.Background(), &tg.UploadGetFileRequest{}, nil); !errors.Is(err, cause) {
				t.Fatalf("error=%v", err)
			}
			if calls != max {
				t.Fatalf("calls=%d, max=%d", calls, max)
			}
		})
	}
}
