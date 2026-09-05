package tgc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gotd/td/tg"
)

// Context keys for MTProto logging
type contextKey string

const (
	BotIDKey contextKey = "mtproto_bot_id"
	DCIDKey  contextKey = "mtproto_dc_id"
)

// WithBotID adds bot ID to context
func WithBotID(ctx context.Context, botID string) context.Context {
	return context.WithValue(ctx, BotIDKey, botID)
}

// WithDCID adds DC ID to context
func WithDCID(ctx context.Context, dcID int) context.Context {
	return context.WithValue(ctx, DCIDKey, dcID)
}

// GetBotIDFromContext retrieves bot ID from context
func GetBotIDFromContext(ctx context.Context) string {
	if v := ctx.Value(BotIDKey); v != nil {
		return v.(string)
	}
	return "unknown"
}

// GetDCIDFromContext retrieves DC ID from context
func GetDCIDFromContext(ctx context.Context) int {
	if v := ctx.Value(DCIDKey); v != nil {
		return v.(int)
	}
	return 0
}

// MtprotoLogger handles logging of MTProto requests and responses
type MtprotoLogger struct {
	file    *os.File
	mu      sync.Mutex
	enabled bool
}

var (
	mtprotoLoggerInstance *MtprotoLogger
	mtprotoLoggerMu       sync.Mutex
)

// InitMtprotoLogger initializes the global MTProto logger
func InitMtprotoLogger(filePath string) error {
	if filePath == "" {
		return nil
	}

	mtprotoLoggerMu.Lock()
	defer mtprotoLoggerMu.Unlock()

	// Close existing logger if any
	if mtprotoLoggerInstance != nil && mtprotoLoggerInstance.file != nil {
		mtprotoLoggerInstance.file.Close()
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open mtproto log file: %w", err)
	}
	mtprotoLoggerInstance = &MtprotoLogger{
		file:    file,
		enabled: true,
	}
	return nil
}

// GetMtprotoLogger returns the global MTProto logger instance
func GetMtprotoLogger() *MtprotoLogger {
	return mtprotoLoggerInstance
}

// LogRequest logs an MTProto request
func (l *MtprotoLogger) LogRequest(ctx context.Context, req *tg.UploadGetFileRequest) {
	if l == nil || !l.enabled {
		return
	}

	botID := GetBotIDFromContext(ctx)
	dcID := GetDCIDFromContext(ctx)

	l.mu.Lock()
	defer l.mu.Unlock()

	entry := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"type":      "REQUEST",
		"bot_id":    botID,
		"dc_id":     dcID,
		"method":    "upload.getFile",
		"request": map[string]interface{}{
			"offset":  req.Offset,
			"limit":   req.Limit,
			"precise": req.Precise,
		},
	}

	// Add location details based on type
	switch loc := req.Location.(type) {
	case *tg.InputDocumentFileLocation:
		entry["location"] = map[string]interface{}{
			"type":           "InputDocumentFileLocation",
			"id":             loc.ID,
			"access_hash":    loc.AccessHash,
			"file_reference": fmt.Sprintf("%x", loc.FileReference),
			"thumb_size":     loc.ThumbSize,
		}
	case *tg.InputFileLocation:
		entry["location"] = map[string]interface{}{
			"type":           "InputFileLocation",
			"volume_id":      loc.VolumeID,
			"local_id":       loc.LocalID,
			"secret":         loc.Secret,
			"file_reference": fmt.Sprintf("%x", loc.FileReference),
		}
	case *tg.InputPhotoFileLocation:
		entry["location"] = map[string]interface{}{
			"type":           "InputPhotoFileLocation",
			"id":             loc.ID,
			"access_hash":    loc.AccessHash,
			"file_reference": fmt.Sprintf("%x", loc.FileReference),
			"thumb_size":     loc.ThumbSize,
		}
	default:
		entry["location"] = map[string]interface{}{
			"type": fmt.Sprintf("%T", req.Location),
		}
	}

	jsonData, _ := json.Marshal(entry)
	fmt.Fprintf(l.file, "%s\n", jsonData)
	l.file.Sync()
}

// LogResponse logs an MTProto response
func (l *MtprotoLogger) LogResponse(ctx context.Context, result tg.UploadFileClass, err error, duration time.Duration) {
	if l == nil || !l.enabled {
		return
	}

	botID := GetBotIDFromContext(ctx)
	dcID := GetDCIDFromContext(ctx)

	l.mu.Lock()
	defer l.mu.Unlock()

	entry := map[string]interface{}{
		"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
		"type":        "RESPONSE",
		"bot_id":      botID,
		"dc_id":       dcID,
		"method":      "upload.getFile",
		"duration_ms": duration.Milliseconds(),
	}

	if err != nil {
		entry["error"] = err.Error()
		entry["success"] = false
	} else {
		entry["success"] = true
		switch res := result.(type) {
		case *tg.UploadFile:
			entry["response"] = map[string]interface{}{
				"type":      "UploadFile",
				"bytes_len": len(res.Bytes),
				"mtime":     res.Mtime,
				"file_type": fmt.Sprintf("%T", res.Type),
			}
		case *tg.UploadFileCDNRedirect:
			entry["response"] = map[string]interface{}{
				"type":           "UploadFileCDNRedirect",
				"dc_id":          res.DCID,
				"file_token_len": len(res.FileToken),
			}
		default:
			entry["response"] = map[string]interface{}{
				"type": fmt.Sprintf("%T", result),
			}
		}
	}

	jsonData, _ := json.Marshal(entry)
	fmt.Fprintf(l.file, "%s\n", jsonData)
	l.file.Sync()
}

// Close closes the log file
func (l *MtprotoLogger) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	return l.file.Close()
}
