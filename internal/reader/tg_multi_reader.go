package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/config"
	"github.com/tgdrive/teldrive/internal/tgc"
)

var (
	ErrStreamAbandoned = errors.New("stream abandoned")
	ErrChunkTimeout    = errors.New("chunk fetch timed out")
)

type ChunkSource interface {
	Chunk(ctx context.Context, offset int64, limit int64) ([]byte, error)
	ChunkSize(start, end int64) int64
}

type chunkSource struct {
	channelId   int64
	partId      int64
	concurrency int
	client      *tg.Client
	key         string
	cache       cache.Cacher
	// useCache reflete [tg.stream] location-cache. Permet de rejouer
	// l'ancien comportement pour mesurer le gain sans reconstruire.
	useCache       bool
	locationOnce   sync.Once
	locationGate   chan struct{}
	lastLocation   *tg.InputDocumentFileLocation // guarded by locationGate; obtained by this source’s client
	lastLocationAt time.Time                     // monotonic completion time, also guarded by locationGate
}

// ttlLocation : duree de vie du cache de localisation Telegram.
// Chaque miss coute DEUX appels reseau (channels.getChannels puis
// channels.getMessages). 30 min etait tres prudent : le seul risque est
// qu'une file_reference expire, cas deja rattrape juste en dessous par un
// unique reessai avec une location fraiche. On peut donc voir large.
const ttlLocation = 6 * time.Hour

func (c *chunkSource) ChunkSize(start, end int64) int64 {
	return tgc.CalculateChunkSize(start, end)
}

// estReferencePerimee reconnait FILE_REFERENCE_EXPIRED et ses variantes.
// Telegram invalide periodiquement les file_reference ; une location mise
// en cache peut donc devenir caduque avant la fin de son TTL.
func estReferencePerimee(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToUpper(err.Error()), "FILE_REFERENCE")
}

// location serializes metadata misses within this reader's part. Waiting is
// cancellable, and each fetch retains its own caller's context: cancellation of
// the first request cannot poison other chunks waiting for the same location.
func (c *chunkSource) location(ctx context.Context, stale *tg.InputDocumentFileLocation, chunkStarted time.Time) (*tg.InputDocumentFileLocation, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if !c.useCache {
		loc, err := tgc.GetLocation(ctx, c.client, c.channelId, c.partId)
		return loc, false, err
	}
	cached := func() *tg.InputDocumentFileLocation {
		var loc *tg.InputDocumentFileLocation
		if c.cache.Get(c.key, &loc) != nil || loc == nil {
			return nil
		}
		return loc
	}
	if stale == nil {
		if loc := cached(); loc != nil {
			return loc, true, nil
		}
	}
	c.locationOnce.Do(func() { c.locationGate = make(chan struct{}, 1) })
	select {
	case c.locationGate <- struct{}{}:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	defer func() { <-c.locationGate }()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if stale == nil {
		if loc := cached(); loc != nil {
			return loc, true, nil
		}
	} else {
		// Other readers can populate the shared cache using another bot. Only
		// reuse a reference obtained by this source's own client during this
		// chunk request. An older local reference may itself have expired.
		loc := c.lastLocation
		if loc != nil && c.lastLocationAt.After(chunkStarted) && (loc.ID != stale.ID || loc.AccessHash != stale.AccessHash || loc.ThumbSize != stale.ThumbSize || !bytes.Equal(loc.FileReference, stale.FileReference)) {
			return loc, true, nil
		}
		c.cache.Delete(c.key)
	}
	loc, err := tgc.GetLocation(ctx, c.client, c.channelId, c.partId)
	if err != nil {
		return nil, false, err
	}
	c.lastLocation = loc
	c.lastLocationAt = time.Now()
	c.cache.Set(c.key, loc, ttlLocation)
	return loc, false, nil
}

func (c *chunkSource) Chunk(ctx context.Context, offset int64, limit int64) ([]byte, error) {
	started := time.Now()
	location, depuisCache, err := c.location(ctx, nil, started)
	if err != nil {
		return nil, err
	}
	chunk, err := tgc.GetChunk(ctx, c.client, location, offset, limit)
	// Retry an expired cached reference only once, preserving the requested range.
	if err != nil && depuisCache && estReferencePerimee(err) {
		location, _, err = c.location(ctx, location, started)
		if err != nil {
			return nil, err
		}
		return tgc.GetChunk(ctx, c.client, location, offset, limit)
	}
	return chunk, err
}

type tgMultiReader struct {
	ctx         context.Context
	cancel      context.CancelCauseFunc
	offset      int64
	limit       int64
	chunkSize   int64
	bufferChan  chan *buffer
	cur         *buffer
	concurrency int
	leftCut     int64
	rightCut    int64
	totalParts  int
	chunkSrc    ChunkSource
	timeout     time.Duration
}

func newTGMultiReader(
	ctx context.Context,
	start int64,
	end int64,
	config *config.TGConfig,
	chunkSrc ChunkSource,
) (*tgMultiReader, error) {
	chunkSize := chunkSrc.ChunkSize(start, end)
	if chunkSize <= 0 || start < 0 || end < start || config.Stream.MultiThreads <= 0 || config.Stream.Buffers < 0 {
		return nil, fmt.Errorf("invalid parallel reader range or configuration")
	}
	offset := start - (start % chunkSize)
	ctx, cancel := context.WithCancelCause(ctx)

	r := &tgMultiReader{
		ctx:         ctx,
		cancel:      cancel,
		limit:       end - start + 1,
		bufferChan:  make(chan *buffer, config.Stream.Buffers),
		concurrency: config.Stream.MultiThreads,
		leftCut:     start - offset,
		rightCut:    (end % chunkSize) + 1,
		totalParts:  int((end - offset + chunkSize) / chunkSize),
		offset:      offset,
		chunkSize:   chunkSize,
		chunkSrc:    chunkSrc,
		timeout:     config.Stream.ChunkTimeout,
	}

	go r.fillBuffer()
	return r, nil
}

func (r *tgMultiReader) Close() error {
	r.cancel(context.Canceled)
	return nil
}

func (r *tgMultiReader) Read(p []byte) (int, error) {
	if r.limit <= 0 {
		return 0, io.EOF
	}

	if r.cur == nil || r.cur.isEmpty() {
		select {
		case cur, ok := <-r.bufferChan:
			if !ok {
				if err := context.Cause(r.ctx); err != nil {
					return 0, err
				}
				return 0, ErrStreamAbandoned
			}
			r.cur = cur
		case <-r.ctx.Done():
			return 0, context.Cause(r.ctx)
		}
	}

	n := copy(p, r.cur.buffer())
	r.cur.increment(n)
	r.limit -= int64(n)

	if r.limit <= 0 {
		return n, io.EOF
	}

	return n, nil
}

// Keep an ordered window of at most concurrency requests. Once the next
// chunk is delivered, refill its slot without waiting for the rest of a batch.
// A stalled consumer bounds memory to the window plus bufferChan and cur.
func (r *tgMultiReader) fillBuffer() {
	defer close(r.bufferChan)
	type result struct {
		chunk []byte
		err   error
	}
	window := min(r.concurrency, r.totalParts)
	slots := make([]chan result, window)
	launch := func(part int) chan result {
		ch := make(chan result, 1)
		go func() {
			ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
			defer cancel()
			chunk, err := r.fetchChunkWithTimeout(ctx, r.offset+int64(part)*r.chunkSize)
			if errors.Is(err, context.DeadlineExceeded) {
				err = ErrChunkTimeout
			}
			if err == nil {
				left, right := int64(0), r.chunkSize
				if part == 0 {
					left = r.leftCut
				}
				if part == r.totalParts-1 {
					right = r.rightCut
				}
				if int64(len(chunk)) < right {
					err = io.ErrUnexpectedEOF
				} else {
					chunk = chunk[left:right]
				}
			}
			if err != nil {
				err = fmt.Errorf("chunk %d: %w", part, err)
			}
			ch <- result{chunk, err}
		}()
		return ch
	}
	for i := range window {
		slots[i] = launch(i)
	}
	for part := 0; part < r.totalParts; part++ {
		slot := part % window
		var res result
		select {
		case res = <-slots[slot]:
		case <-r.ctx.Done():
			return
		}
		if res.err != nil {
			r.cancel(res.err)
			return
		}
		select {
		case r.bufferChan <- &buffer{buf: res.chunk}:
		case <-r.ctx.Done():
			return
		}
		if next := part + window; next < r.totalParts {
			slots[slot] = launch(next)
		}
	}
}

func (r *tgMultiReader) fetchChunkWithTimeout(ctx context.Context, offset int64) ([]byte, error) {
	type result struct {
		chunk []byte
		err   error
	}
	ch := make(chan result, 1)
	// Capture the immutable offset before starting the request: a source that
	// returns late after cancellation cannot observe another chunk's offset.
	go func() {
		chunk, err := r.chunkSrc.Chunk(ctx, offset, r.chunkSize)
		ch <- result{chunk, err}
	}()
	select {
	case res := <-ch:
		return res.chunk, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type buffer struct {
	buf    []byte
	offset int
}

func (b *buffer) isEmpty() bool {
	return b == nil || len(b.buf)-b.offset <= 0
}

func (b *buffer) buffer() []byte {
	return b.buf[b.offset:]
}

func (b *buffer) increment(n int) {
	b.offset += n
}
