package retry

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

var internalErrors = []string{
	"Timedout",
	"No workers running",
	"RPC_CALL_FAIL",
	"RPC_MCGET_FAIL",
	"WORKER_BUSY_TOO_LONG_RETRY",
	"memory limit exit",
	"connection dead",
	"engine was closed",
	"STORAGE_CHOOSE_VOLUME_FAILED",
}

type retry struct {
	max    int
	errors []string
}

func isErrorMatch(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	for _, internalError := range internalErrors {
		if strings.Contains(errStr, internalError) {
			return true
		}
	}
	return false
}

// isReadTimeout restricts this Telegram transient failure to idempotent reads.
// In particular, retrying a send or mutation after Timeout may duplicate writes.
func isReadTimeout(input bin.Encoder, err error) bool {
	rpc, ok := tgerr.As(err)
	if !ok || rpc.Code != -503 || rpc.Message != "Timeout" {
		return false
	}
	switch input.(type) {
	case *tg.UploadGetFileRequest, *tg.ChannelsGetMessagesRequest, *tg.ChannelsGetChannelsRequest:
		return true
	default:
		return false
	}
}

func (r retry) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		retries := 0
		max := r.max
		var lastErr error

		for retries < max {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := next.Invoke(ctx, input, output); err != nil {
				readTimeout := isReadTimeout(input, err)
				if readTimeout {
					// Telegram may already spend seconds on each failed RPC.
					// Keep this new retry path to at most three total attempts.
					max = min(max, 3)
				}
				if readTimeout || tgerr.Is(err, r.errors...) || isErrorMatch(err) {
					lastErr = err
					retries++
					if retries >= max {
						break
					}
					// Sans delai, les 5 tentatives partaient en boucle serree : sur
					// une erreur persistante c'est du gaspillage, pas une reprise.
					// Recul exponentiel plafonne, interruptible par le contexte.
					delay := time.Duration(200*(1<<uint(retries-1))) * time.Millisecond
					if delay > 3*time.Second {
						delay = 3 * time.Second
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(delay):
					}
					continue
				}
				return errors.Wrap(err, "retry middleware skip")
			}

			return nil
		}

		if lastErr != nil {
			// Recovery outside this middleware must still recognize RPC errors;
			// losing their cause would restart an exhausted retry budget.
			return fmt.Errorf("retry limit reached after %d attempts: %w", retries, lastErr)
		}
		return fmt.Errorf("retry limit reached after %d attempts", retries)
	}
}

func New(max int, errors ...string) telegram.Middleware {
	return retry{
		max:    max,
		errors: append(errors, internalErrors...),
	}
}
