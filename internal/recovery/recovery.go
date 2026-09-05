package recovery

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

type recovery struct {
	newBackoff func() backoff.BackOff
}

func New(newBackoff func() backoff.BackOff) telegram.Middleware {
	return &recovery{newBackoff: newBackoff}
}

func (r *recovery) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {

		// Backoff state belongs to this RPC, not the shared Telegram client.
		// Its waits must end when this request is canceled.
		return backoff.RetryNotify(func() error {
			if err := next.Invoke(ctx, input, output); err != nil {
				if shouldRecover(ctx, err) {
					return errors.Wrap(err, "recover")
				}

				return backoff.Permanent(err)
			}

			return nil
		}, backoff.WithContext(r.newBackoff(), ctx), nil)
	}
}

func shouldRecover(ctx context.Context, err error) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}

	_, ok := tgerr.As(err)

	return !ok
}
