package recovery

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
)

func TestRecoverySurvivesCreatorRequest(t *testing.T) {
	creator, cancelCreator := context.WithCancel(context.Background())
	middleware := New(func() backoff.BackOff {
		return backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Millisecond), 2)
	})
	defer cancelCreator()
	attempts := 0
	next := telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		attempts++
		if attempts == 2 {
			return errors.New("connection reset by peer")
		}
		return nil
	})
	invoke := middleware.Handle(next)
	if err := invoke.Invoke(creator, nil, nil); err != nil {
		t.Fatal(err)
	}
	cancelCreator()
	if err := invoke.Invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("live request lost recovery after creator ended: attempts=%d error=%v", attempts, err)
	}
}

func TestRecoveryConcurrentRPCs(t *testing.T) {
	middleware := New(func() backoff.BackOff {
		b := backoff.NewExponentialBackOff()
		b.InitialInterval = time.Microsecond
		b.MaxElapsedTime = time.Millisecond
		return b
	})
	invoke := middleware.Handle(telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error { return nil }))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				if err := invoke.Invoke(context.Background(), nil, nil); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
}

func TestRecoveryCancellationInterruptsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	middleware := New(func() backoff.BackOff {
		return backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 1)
	})
	invoke := middleware.Handle(telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		cancel()
		return errors.New("connection reset by peer")
	}))
	start := time.Now()
	_ = invoke.Invoke(ctx, nil, nil)
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("canceled RPC spent %v waiting for backoff", elapsed)
	}
}
