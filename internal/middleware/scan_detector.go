// Package middleware provides HTTP middleware components for the teldrive server.
package middleware

import (
	"fmt"
	"math/rand"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tgdrive/teldrive/internal/logging"
	"go.uber.org/zap"
)

// ScanDetectorConfig holds configuration for the scan detector middleware.
type ScanDetectorConfig struct {
	// Enabled determines if the scan detector is active.
	Enabled bool `config:"enabled" description:"Enable Jellyfin scan detection and throttling" default:"true"`
	// ThrottleDelayMs is the delay in milliseconds to apply to metadata requests during scans.
	// Actual delay will be randomized between ThrottleDelayMs and ThrottleDelayMs + 300ms.
	ThrottleDelayMs int `config:"throttle-delay-ms" description:"Base throttle delay in milliseconds for metadata requests" default:"200"`
	// ScanThresholdPerSec is the number of requests per second that triggers scan detection.
	ScanThresholdPerSec int `config:"scan-threshold-per-sec" description:"Request threshold per second to detect scan activity" default:"10"`
}

// DefaultScanDetectorConfig returns a ScanDetectorConfig with sensible defaults.
func DefaultScanDetectorConfig() *ScanDetectorConfig {
	return &ScanDetectorConfig{
		Enabled:             true,
		ThrottleDelayMs:     200,
		ScanThresholdPerSec: 10,
	}
}

// requestTracker tracks request frequency per client.
type requestTracker struct {
	mu         sync.RWMutex
	clients    map[string]*clientStats
	cleanupTTL time.Duration
}

// clientStats holds per-client request statistics.
type clientStats struct {
	requests   []time.Time
	lastAccess time.Time
}

// newRequestTracker creates a new request tracker with automatic cleanup.
func newRequestTracker() *requestTracker {
	rt := &requestTracker{
		clients:    make(map[string]*clientStats),
		cleanupTTL: 5 * time.Minute,
	}
	go rt.cleanup()
	return rt
}

// cleanup periodically removes stale client entries.
func (rt *requestTracker) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		rt.mu.Lock()
		now := time.Now()
		for key, stats := range rt.clients {
			if now.Sub(stats.lastAccess) > rt.cleanupTTL {
				delete(rt.clients, key)
			}
		}
		rt.mu.Unlock()
	}
}

// recordRequest records a request and returns the current request rate per second.
func (rt *requestTracker) recordRequest(clientKey string) float64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	now := time.Now()
	stats, exists := rt.clients[clientKey]
	if !exists {
		stats = &clientStats{
			requests: make([]time.Time, 0, 100),
		}
		rt.clients[clientKey] = stats
	}

	stats.lastAccess = now
	stats.requests = append(stats.requests, now)

	// Keep only requests from the last second for rate calculation
	cutoff := now.Add(-time.Second)
	validRequests := stats.requests[:0]
	for _, t := range stats.requests {
		if t.After(cutoff) {
			validRequests = append(validRequests, t)
		}
	}
	stats.requests = validRequests

	return float64(len(validRequests))
}

// isScanning checks if the client appears to be performing a scan.
func (rt *requestTracker) isScanning(clientKey string, threshold int) bool {
	rate := rt.recordRequest(clientKey)
	return rate > float64(threshold)
}

// ScanDetector creates a middleware that detects and throttles Jellyfin scan patterns.
// It tracks request frequency per client and applies delays to all requests during scans,
// EXCEPT actual video streaming (detected by Range header with significant byte range).
func ScanDetector(config *ScanDetectorConfig) Middleware {
	if config == nil {
		config = DefaultScanDetectorConfig()
	}

	tracker := newRequestTracker()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip if disabled
			if !config.Enabled {
				next.ServeHTTP(w, r)
				return
			}

			logger := logging.FromContext(r.Context())
			clientKey := getClientKey(r)
			requestPath := r.URL.Path
			ext := strings.ToLower(filepath.Ext(requestPath))

			// Check if this is actual video streaming (has Range header)
			// Jellyfin scan requests video files WITHOUT Range header to probe metadata
			// Real playback always uses Range header for byte-range requests
			isVideoFile := isVideoRequest(ext)
			rangeHeader := r.Header.Get("Range")
			isActualStreaming := isVideoFile && rangeHeader != "" && isSignificantRange(rangeHeader)

			// Check if client is in scan mode
			isScanning := tracker.isScanning(clientKey, config.ScanThresholdPerSec)

			// Only actual streaming gets priority (video file + Range header)
			if isActualStreaming {
				logger.Debug("scan_detector: streaming request - priority pass",
					zap.String("path", requestPath),
					zap.String("client", clientKey),
					zap.String("range", rangeHeader),
					zap.Bool("scan_detected", isScanning),
				)
				next.ServeHTTP(w, r)
				return
			}

			// During scan mode, throttle ALL other requests (including video metadata probes)
			if isScanning {
				// Random delay between ThrottleDelayMs and ThrottleDelayMs + 300ms
				delay := time.Duration(config.ThrottleDelayMs+rand.Intn(300)) * time.Millisecond
				logger.Debug("scan_detector: throttling request during scan",
					zap.String("path", requestPath),
					zap.String("client", clientKey),
					zap.String("ext", ext),
					zap.Bool("is_video_file", isVideoFile),
					zap.String("range", rangeHeader),
					zap.Duration("delay", delay),
				)
				time.Sleep(delay)
			}

			next.ServeHTTP(w, r)
		})
	}
}

// ScanDetectorWithConfig creates a scan detector middleware with the given configuration.
// This is an alias for ScanDetector for API consistency with other middleware.
func ScanDetectorWithConfig(config *ScanDetectorConfig) Middleware {
	return ScanDetector(config)
}

// getClientKey returns a unique identifier for the client making the request.
// It combines IP address and User-Agent for better client differentiation.
func getClientKey(r *http.Request) string {
	ip := r.RemoteAddr

	// Check for X-Forwarded-For header (common in proxied setups)
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		// Use the first IP in the chain (original client)
		if idx := strings.Index(forwarded, ","); idx > 0 {
			ip = strings.TrimSpace(forwarded[:idx])
		} else {
			ip = strings.TrimSpace(forwarded)
		}
	}

	// Check for X-Real-IP header
	if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
		ip = realIP
	}

	// Combine IP with User-Agent for better differentiation
	// (same IP might be different users/applications)
	userAgent := r.Header.Get("User-Agent")
	if userAgent != "" {
		// Truncate user agent to avoid excessive memory usage
		if len(userAgent) > 100 {
			userAgent = userAgent[:100]
		}
		return ip + "|" + userAgent
	}

	return ip
}

// isSignificantRange checks if the Range header indicates actual video streaming
// vs just a small probe for metadata extraction.
// Jellyfin typically probes with small ranges (first few KB) during scans,
// while actual playback requests larger chunks (64KB+).
func isSignificantRange(rangeHeader string) bool {
	// Parse Range header: "bytes=0-1048576" or "bytes=0-"
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return false
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return false
	}

	// If end is empty (e.g., "bytes=0-"), it's requesting to end of file - likely streaming
	if parts[1] == "" {
		return true
	}

	// Parse start and end
	var start, end int64
	_, err1 := fmt.Sscanf(parts[0], "%d", &start)
	_, err2 := fmt.Sscanf(parts[1], "%d", &end)
	if err1 != nil || err2 != nil {
		return false
	}

	// Consider it actual streaming if requesting more than 32KB
	// Metadata probes typically request first 4KB-16KB
	rangeSize := end - start + 1
	return rangeSize > 32*1024
}

// isVideoRequest checks if the request is for a video file.
// Video requests get priority and are never throttled.
func isVideoRequest(ext string) bool {
	videoExtensions := map[string]bool{
		".mkv":  true,
		".mp4":  true,
		".avi":  true,
		".m4v":  true,
		".mov":  true,
		".wmv":  true,
		".flv":  true,
		".webm": true,
		".ts":   true,
		".m2ts": true,
		".mpg":  true,
		".mpeg": true,
		".vob":  true,
		".3gp":  true,
		".ogv":  true,
	}
	return videoExtensions[ext]
}

// isMetadataRequest checks if the request is for metadata or thumbnail files.
// These are typically small files that Jellyfin fetches during library scans.
func isMetadataRequest(ext, filename string) bool {
	// Common metadata file extensions
	metadataExtensions := map[string]bool{
		".nfo":  true,
		".jpg":  true,
		".jpeg": true,
		".png":  true,
		".gif":  true,
		".bmp":  true,
		".webp": true,
		".xml":  true,
		".srt":  true,
		".ass":  true,
		".ssa":  true,
		".sub":  true,
		".idx":  true,
		".vtt":  true,
	}

	// Check extension first
	if metadataExtensions[ext] {
		return true
	}

	// Check for common Jellyfin/media server thumbnail filenames
	thumbnailNames := map[string]bool{
		"folder.jpg":    true,
		"folder.png":    true,
		"thumb.jpg":     true,
		"thumb.png":     true,
		"poster.jpg":    true,
		"poster.png":    true,
		"fanart.jpg":    true,
		"fanart.png":    true,
		"backdrop.jpg":  true,
		"backdrop.png":  true,
		"banner.jpg":    true,
		"banner.png":    true,
		"landscape.jpg": true,
		"landscape.png": true,
		"logo.png":      true,
		"clearlogo.png": true,
		"clearart.png":  true,
		"disc.png":      true,
		"cdart.png":     true,
		"movie.nfo":     true,
		"tvshow.nfo":    true,
	}

	return thumbnailNames[filename]
}
