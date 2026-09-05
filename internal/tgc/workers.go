package tgc

import (
	"sync"
	"time"
)

type BotWorker struct {
	mu           sync.RWMutex
	bots         map[int64][]string
	currIdx      map[int64]int
	blocked      map[string]time.Time // bot token -> unblock time
	blockedMu    sync.RWMutex
	botProxy     map[string]string // bot token -> proxy URL
	proxyEnabled bool
}

func NewBotWorker(proxyEnabled bool) *BotWorker {
	return &BotWorker{
		bots:         make(map[int64][]string),
		currIdx:      make(map[int64]int),
		blocked:      make(map[string]time.Time),
		botProxy:     make(map[string]string),
		proxyEnabled: proxyEnabled,
	}
}

func (w *BotWorker) Set(bots []string, userID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.bots[userID]; ok {
		return
	}
	w.bots[userID] = bots
	w.currIdx[userID] = 0
}

// SetWithProxies sets the bots for a user and stores the proxy mapping for each bot token
func (w *BotWorker) SetWithProxies(bots []string, proxies map[string]string, userID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Always update proxy mappings, even if bots already exist
	for _, token := range bots {
		if proxyURL, exists := proxies[token]; exists {
			w.botProxy[token] = proxyURL
		}
	}

	// Only set bots if not already set
	if _, ok := w.bots[userID]; ok {
		return
	}
	w.bots[userID] = bots
	w.currIdx[userID] = 0
}

// GetProxy returns the proxy URL for a given bot token (empty if proxy is disabled)
func (w *BotWorker) GetProxy(token string) string {
	if !w.proxyEnabled {
		return ""
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.botProxy[token]
}

// MarkBlocked marks a bot as blocked until the specified duration passes
func (w *BotWorker) MarkBlocked(token string, duration time.Duration) {
	w.blockedMu.Lock()
	defer w.blockedMu.Unlock()
	w.blocked[token] = time.Now().Add(duration)
}

// IsBlocked checks if a bot is currently blocked
func (w *BotWorker) IsBlocked(token string) bool {
	w.blockedMu.RLock()
	defer w.blockedMu.RUnlock()
	unblockTime, exists := w.blocked[token]
	if !exists {
		return false
	}
	// Un blocage expire est simplement ignore : l'entree sera ecrasee au
	// prochain MarkBlocked. La supprimer ici ecrivait dans la map sous un
	// verrou de LECTURE -> "fatal error: concurrent map writes" (non rattrapable)
	// des que deux goroutines verifiaient le meme bot expire simultanement.
	return time.Now().Before(unblockTime)
}

func (w *BotWorker) Next(userID int64) (string, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	bots := w.bots[userID]
	if len(bots) == 0 {
		return "", -1
	}
	startIdx := w.currIdx[userID]

	// Try to find a non-blocked bot
	for i := 0; i < len(bots); i++ {
		index := (startIdx + i) % len(bots)
		token := bots[index]
		if !w.IsBlocked(token) {
			w.currIdx[userID] = (index + 1) % len(bots)
			return token, index
		}
	}

	// All bots blocked, return the next one anyway (will fail but shows error)
	index := startIdx
	w.currIdx[userID] = (index + 1) % len(bots)
	return bots[index], index
}
