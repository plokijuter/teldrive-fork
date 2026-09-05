package tgc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// MtprotoMiddlewareLogger logs all MTProto requests and responses
type MtprotoMiddlewareLogger struct {
	file    *os.File
	mu      sync.Mutex
	enabled bool
}

var (
	mtprotoMwLoggerInstance *MtprotoMiddlewareLogger
	mtprotoMwLoggerMu       sync.Mutex
)

// InitMtprotoMiddlewareLogger initializes the middleware logger
func InitMtprotoMiddlewareLogger(filePath string) error {
	if filePath == "" {
		return nil
	}

	mtprotoMwLoggerMu.Lock()
	defer mtprotoMwLoggerMu.Unlock()

	if mtprotoMwLoggerInstance != nil && mtprotoMwLoggerInstance.file != nil {
		mtprotoMwLoggerInstance.file.Close()
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open mtproto middleware log file: %w", err)
	}
	mtprotoMwLoggerInstance = &MtprotoMiddlewareLogger{
		file:    file,
		enabled: true,
	}
	return nil
}

// GetMtprotoMiddlewareLogger returns the middleware logger instance
func GetMtprotoMiddlewareLogger() *MtprotoMiddlewareLogger {
	return mtprotoMwLoggerInstance
}

func (l *MtprotoMiddlewareLogger) log(entry map[string]interface{}) {
	if l == nil || !l.enabled || l.file == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	jsonData, _ := json.Marshal(entry)
	fmt.Fprintf(l.file, "%s\n", jsonData)
	l.file.Sync()
}

// extractRequestInfo extracts information from a request object
func extractRequestInfo(req bin.Object) map[string]interface{} {
	info := map[string]interface{}{
		"type": fmt.Sprintf("%T", req),
	}

	// Use reflection to get field values
	v := reflect.ValueOf(req)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Struct {
		t := v.Type()
		fields := make(map[string]interface{})
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			value := v.Field(i)

			// Skip unexported fields
			if !value.CanInterface() {
				continue
			}

			// Handle specific types
			switch val := value.Interface().(type) {
			case []byte:
				if len(val) > 32 {
					fields[field.Name] = fmt.Sprintf("[%d bytes]", len(val))
				} else {
					fields[field.Name] = fmt.Sprintf("%x", val)
				}
			case string:
				if len(val) > 100 {
					fields[field.Name] = val[:100] + "..."
				} else {
					fields[field.Name] = val
				}
			default:
				fields[field.Name] = val
			}
		}
		info["fields"] = fields
	}
	return info
}

// NewMtprotoLoggingMiddleware creates a middleware that logs all MTProto requests/responses
func NewMtprotoLoggingMiddleware(botID string) telegram.Middleware {
	return telegram.MiddlewareFunc(func(next tg.Invoker) telegram.InvokeFunc {
		return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
			logger := GetMtprotoMiddlewareLogger()
			if logger == nil || !logger.enabled {
				return next.Invoke(ctx, input, output)
			}

			// Get request type name
			reqType := fmt.Sprintf("%T", input)

			// Log request
			reqEntry := map[string]interface{}{
				"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
				"type":       "REQUEST",
				"bot_id":     botID,
				"method":     reqType,
			}

			// Try to extract more info from the request
			if obj, ok := input.(bin.Object); ok {
				reqEntry["request"] = extractRequestInfo(obj)
			}

			logger.log(reqEntry)

			// Execute the request
			startTime := time.Now()
			err := next.Invoke(ctx, input, output)
			duration := time.Since(startTime)

			// Log response
			respEntry := map[string]interface{}{
				"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
				"type":        "RESPONSE",
				"bot_id":      botID,
				"method":      reqType,
				"duration_ms": duration.Milliseconds(),
				"success":     err == nil,
			}

			if err != nil {
				respEntry["error"] = err.Error()

				// Try to extract RPC error details using tgerr
				if rpcErr, ok := tgerr.As(err); ok {
					respEntry["rpc_error"] = map[string]interface{}{
						"code":    rpcErr.Code,
						"message": rpcErr.Message,
						"type":    rpcErr.Type,
					}
				}
			} else {
				// Try to extract response info
				if obj, ok := output.(bin.Object); ok {
					respEntry["response"] = map[string]interface{}{
						"type": fmt.Sprintf("%T", obj),
					}
				}
			}

			logger.log(respEntry)

			return err
		}
	})
}
