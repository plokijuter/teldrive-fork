package tgc

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/logging"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// AuthRecovery is a middleware that handles AUTH_KEY_UNREGISTERED errors
// by cleaning up invalid sessions and preventing retry storms.
type AuthRecovery struct {
	db           *gorm.DB
	cache        cache.Cacher
	sessionKey   string
	blockedUntil time.Time
	mu           sync.RWMutex
}

// NewAuthRecovery creates a new auth recovery middleware.
// sessionKey is the cache key for this bot's session (e.g., "sessions:teldrive:botId")
func NewAuthRecovery(db *gorm.DB, cache cache.Cacher, sessionKey string) *AuthRecovery {
	return &AuthRecovery{
		db:         db,
		cache:      cache,
		sessionKey: sessionKey,
	}
}

// Handle implements the telegram.Middleware interface.
func (a *AuthRecovery) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		logger := logging.FromContext(ctx)

		// Allow authentication requests to pass through even during cooldown
		// This is critical: we need to let re-auth attempts succeed
		if isAuthRequest(input) {
			return next.Invoke(ctx, input, output)
		}

		// Check if we're currently blocked due to previous AUTH_KEY_UNREGISTERED
		a.mu.RLock()
		blocked := time.Now().Before(a.blockedUntil)
		a.mu.RUnlock()

		if blocked {
			return errors.New("auth_recovery: session invalidated, waiting for cooldown")
		}

		err := next.Invoke(ctx, input, output)
		if err == nil {
			return nil
		}

		// Check for AUTH_KEY_UNREGISTERED error
		if tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
			logger.Warn("auth_recovery: AUTH_KEY_UNREGISTERED detected, invalidating session",
				zap.String("session_key", a.sessionKey),
			)

			// Block further attempts for 5 minutes to prevent retry storm
			a.mu.Lock()
			a.blockedUntil = time.Now().Add(5 * time.Minute)
			a.mu.Unlock()

			// Delete the invalid session from database
			if a.sessionKey != "" {
				if err := a.deleteSession(ctx); err != nil {
					logger.Error("auth_recovery: failed to delete invalid session",
						zap.String("session_key", a.sessionKey),
						zap.Error(err),
					)
				} else {
					logger.Info("auth_recovery: deleted invalid session from database",
						zap.String("session_key", a.sessionKey),
					)
				}
			}

			// Return a permanent error to prevent retries
			return errors.Wrap(err, "auth_recovery: session invalidated and removed")
		}

		return err
	}
}

// deleteSession removes the invalid session from the database and cache.
func (a *AuthRecovery) deleteSession(ctx context.Context) error {
	// Delete from cache first
	if a.cache != nil {
		a.cache.Delete(a.sessionKey)
	}

	// Delete from database
	if a.db != nil {
		result := a.db.Exec("DELETE FROM teldrive.kv WHERE key = ?", a.sessionKey)
		if result.Error != nil {
			return result.Error
		}
	}

	return nil
}

// IsBlocked returns true if this session is currently blocked.
func (a *AuthRecovery) IsBlocked() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return time.Now().Before(a.blockedUntil)
}

// BlockedUntil returns the time when the block expires.
func (a *AuthRecovery) BlockedUntil() time.Time {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.blockedUntil
}

// dcBlocker tracks which datacenters are blocked due to auth issues.
type dcBlocker struct {
	mu      sync.RWMutex
	blocked map[int]time.Time // DC ID -> unblock time
}

var globalDCBlocker = &dcBlocker{
	blocked: make(map[int]time.Time),
}

// BlockDC marks a datacenter as blocked for the specified duration.
func BlockDC(dcID int, duration time.Duration) {
	globalDCBlocker.mu.Lock()
	defer globalDCBlocker.mu.Unlock()
	globalDCBlocker.blocked[dcID] = time.Now().Add(duration)
}

// IsDCBlocked returns true if the datacenter is currently blocked.
func IsDCBlocked(dcID int) bool {
	globalDCBlocker.mu.RLock()
	defer globalDCBlocker.mu.RUnlock()
	unblockTime, exists := globalDCBlocker.blocked[dcID]
	if !exists {
		return false
	}
	if time.Now().After(unblockTime) {
		// Clean up expired entry
		delete(globalDCBlocker.blocked, dcID)
		return false
	}
	return true
}

// GetDCBlockedUntil returns when the DC block expires (zero time if not blocked).
func GetDCBlockedUntil(dcID int) time.Time {
	globalDCBlocker.mu.RLock()
	defer globalDCBlocker.mu.RUnlock()
	return globalDCBlocker.blocked[dcID]
}

// ExtractBotIdFromSessionKey extracts the bot ID from a session key.
// Session keys are in format: "sessions:teldrive:botId"
func ExtractBotIdFromSessionKey(sessionKey string) string {
	parts := strings.Split(sessionKey, ":")
	if len(parts) >= 3 {
		return parts[2]
	}
	return ""
}

// ErrAuthKeyUnregistered is a sentinel error for AUTH_KEY_UNREGISTERED
var ErrAuthKeyUnregistered = errors.New("AUTH_KEY_UNREGISTERED")

// isAuthRequest checks if the request is an authentication-related request
// These requests should be allowed to pass through even during cooldown
func isAuthRequest(input bin.Encoder) bool {
	switch input.(type) {
	case *tg.AuthImportBotAuthorizationRequest,
		*tg.AuthExportAuthorizationRequest,
		*tg.AuthImportAuthorizationRequest,
		*tg.AuthBindTempAuthKeyRequest,
		*tg.AuthCheckPasswordRequest,
		*tg.AuthRecoverPasswordRequest,
		*tg.AuthRequestPasswordRecoveryRequest,
		*tg.AuthSendCodeRequest,
		*tg.AuthSignInRequest,
		*tg.AuthSignUpRequest,
		*tg.AuthLogOutRequest:
		return true
	default:
		return false
	}
}

// IsAuthKeyUnregistered checks if an error is an AUTH_KEY_UNREGISTERED error
func IsAuthKeyUnregistered(err error) bool {
	if err == nil {
		return false
	}
	// Check for the sentinel error
	if errors.Is(err, ErrAuthKeyUnregistered) {
		return true
	}
	// Check for tgerr AUTH_KEY_UNREGISTERED
	if tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
		return true
	}
	// Check for wrapped error message
	errStr := err.Error()
	return strings.Contains(errStr, "AUTH_KEY_UNREGISTERED") ||
		strings.Contains(errStr, "auth_recovery: session invalidated")
}

// IsMessageIdsEmpty checks if an error is a MESSAGE_IDS_EMPTY error
// This error occurs when the message containing the file no longer exists on Telegram
func IsMessageIdsEmpty(err error) bool {
	if err == nil {
		return false
	}
	// Check for tgerr MESSAGE_IDS_EMPTY
	if tgerr.Is(err, "MESSAGE_IDS_EMPTY") {
		return true
	}
	// Check for wrapped error message
	return strings.Contains(err.Error(), "MESSAGE_IDS_EMPTY")
}

// IsFilePartsMismatch checks if an error is a "file parts mismatch" error.
// This occurs when getParts resolves fewer real Telegram messages than the
// DB expects for a file (e.g. some of its messages were deleted) — the file
// is orphaned the same way a MESSAGE_IDS_EMPTY file is, just detected earlier.
func IsFilePartsMismatch(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "file parts mismatch")
}

// botBlocker tracks which bots are blocked due to auth issues
type botBlocker struct {
	mu      sync.RWMutex
	blocked map[string]time.Time // botId -> unblock time
}

var globalBotBlocker = &botBlocker{
	blocked: make(map[string]time.Time),
}

// BlockBot marks a bot as blocked for the specified duration
func BlockBot(botId string, duration time.Duration) {
	globalBotBlocker.mu.Lock()
	defer globalBotBlocker.mu.Unlock()
	globalBotBlocker.blocked[botId] = time.Now().Add(duration)
}

// IsBotBlocked returns true if the bot is currently blocked
func IsBotBlocked(botId string) bool {
	globalBotBlocker.mu.RLock()
	defer globalBotBlocker.mu.RUnlock()
	unblockTime, exists := globalBotBlocker.blocked[botId]
	if !exists {
		return false
	}
	if time.Now().After(unblockTime) {
		return false
	}
	return true
}

// GetBotBlockedUntil returns when the bot block expires (zero time if not blocked)
func GetBotBlockedUntil(botId string) time.Time {
	globalBotBlocker.mu.RLock()
	defer globalBotBlocker.mu.RUnlock()
	return globalBotBlocker.blocked[botId]
}

// CleanupExpiredBotBlocks removes expired bot blocks
func CleanupExpiredBotBlocks() {
	globalBotBlocker.mu.Lock()
	defer globalBotBlocker.mu.Unlock()
	now := time.Now()
	for botId, unblockTime := range globalBotBlocker.blocked {
		if now.After(unblockTime) {
			delete(globalBotBlocker.blocked, botId)
		}
	}
}

// GetAvailableBots filters out blocked bots from a list of tokens
func GetAvailableBots(tokens []string) []string {
	available := make([]string, 0, len(tokens))
	for _, token := range tokens {
		botId := strings.Split(token, ":")[0]
		if !IsBotBlocked(botId) {
			available = append(available, token)
		}
	}
	return available
}
