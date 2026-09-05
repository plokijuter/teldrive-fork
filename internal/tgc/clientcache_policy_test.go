package tgc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/tgdrive/teldrive/internal/config"
)

func invokeLivePolicy(cnf *config.TGConfig, next tg.Invoker) tg.Invoker {
	middlewares := liveClientMiddlewares(nil, nil, cnf, "test-bot")
	for i := len(middlewares) - 1; i >= 0; i-- {
		next = middlewares[i].Handle(next)
	}
	return next
}

func TestLiveClientPolicyRespectsRateConfiguration(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			cnf := &config.TGConfig{RateLimit: enabled, Rate: 1000, RateBurst: 1, ReconnectTimeout: time.Second}
			calls := 0
			invoke := invokeLivePolicy(cnf, telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
				calls++
				return nil
			}))
			if err := invoke.Invoke(context.Background(), nil, nil); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			err := invoke.Invoke(ctx, nil, nil)
			if enabled && (err == nil || calls != 1) {
				t.Fatalf("rate-limited RPC reached transport: calls=%d err=%v", calls, err)
			}
			if !enabled && (err != nil || calls != 2) {
				t.Fatalf("disabled limiter blocked RPC: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestLiveClientPolicyBoundsFloodWait(t *testing.T) {
	want := tgerr.New(420, "FLOOD_WAIT_100")
	invoke := invokeLivePolicy(&config.TGConfig{}, telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		return want
	}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := invoke.Invoke(ctx, nil, nil); !errors.Is(err, want) {
		t.Fatalf("long flood wait was swallowed: %v", err)
	}
}

func TestLiveClientPolicyRetriesTransientTelegramError(t *testing.T) {
	calls := 0
	invoke := invokeLivePolicy(&config.TGConfig{}, telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		calls++
		if calls == 1 {
			return tgerr.New(500, "RPC_CALL_FAIL")
		}
		return nil
	}))
	if err := invoke.Invoke(context.Background(), nil, nil); err != nil || calls != 2 {
		t.Fatalf("transient RPC did not recover: calls=%d err=%v", calls, err)
	}
}
