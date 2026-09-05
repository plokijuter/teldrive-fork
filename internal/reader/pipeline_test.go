package reader

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tgdrive/teldrive/internal/config"
)

type testChunkSource struct {
	size int64
	get  func(context.Context, int64, int64) ([]byte, error)
}

func (s testChunkSource) ChunkSize(_, _ int64) int64 { return s.size }
func (s testChunkSource) Chunk(ctx context.Context, offset, limit int64) ([]byte, error) {
	return s.get(ctx, offset, limit)
}

func readerConfig(threads, buffers int) *config.TGConfig {
	c := &config.TGConfig{}
	c.Stream.MultiThreads, c.Stream.Buffers = threads, buffers
	c.Stream.ChunkTimeout = time.Second
	return c
}

func TestMultiReaderRanges(t *testing.T) {
	data := make([]byte, 257)
	for i := range data {
		data[i] = byte(i)
	}
	src := testChunkSource{size: 16, get: func(_ context.Context, offset, limit int64) ([]byte, error) {
		return data[offset:min(offset+limit, int64(len(data)))], nil
	}}
	for _, threads := range []int{1, 3, 8} {
		for _, span := range [][2]int{{0, 0}, {3, 7}, {3, 20}, {0, 255}, {5, 256}, {256, 256}} {
			r, err := newTGMultiReader(context.Background(), int64(span[0]), int64(span[1]), readerConfig(threads, 2), src)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(r)
			r.Close()
			if err != nil || !bytes.Equal(got, data[span[0]:span[1]+1]) {
				t.Fatalf("threads=%d range=%v bytes=%d err=%v", threads, span, len(got), err)
			}
		}
	}
}

// A slow second chunk must not delay the first chunk or prevent its worker
// from fetching the third one. No throughput threshold or timing race needed.
func TestMultiReaderProgressPastSlowChunk(t *testing.T) {
	third := make(chan struct{})
	src := testChunkSource{size: 16, get: func(ctx context.Context, offset, limit int64) ([]byte, error) {
		if offset == 16 {
			select {
			case <-third:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if offset == 32 {
			close(third)
		}
		return bytes.Repeat([]byte{byte(offset)}, int(limit)), nil
	}}
	r, err := newTGMultiReader(context.Background(), 0, 47, readerConfig(2, 0), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || len(got) != 48 {
		t.Fatalf("bytes=%d err=%v", len(got), err)
	}
}

func TestMultiReaderErrors(t *testing.T) {
	want := errors.New("source unavailable")
	for _, short := range []bool{false, true} {
		src := testChunkSource{size: 16, get: func(context.Context, int64, int64) ([]byte, error) {
			if short {
				return make([]byte, 2), nil
			}
			return nil, want
		}}
		r, err := newTGMultiReader(context.Background(), 3, 8, readerConfig(2, 1), src)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.ReadAll(r)
		r.Close()
		expected := want
		if short {
			expected = io.ErrUnexpectedEOF
		}
		if !errors.Is(err, expected) {
			t.Fatalf("short=%v got=%v want=%v", short, err, expected)
		}
	}
}

func TestMultiReaderCloseCancelsFetches(t *testing.T) {
	started, stopped := make(chan struct{}, 2), make(chan struct{}, 2)
	src := testChunkSource{size: 16, get: func(ctx context.Context, _, _ int64) ([]byte, error) {
		started <- struct{}{}
		<-ctx.Done()
		stopped <- struct{}{}
		return nil, ctx.Err()
	}}
	r, err := newTGMultiReader(context.Background(), 0, 1023, readerConfig(2, 0), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("fetch did not start")
		}
	}
	r.Close()
	for range 2 {
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			t.Fatal("fetch did not stop")
		}
	}
}

func TestMultiReaderTimeout(t *testing.T) {
	src := testChunkSource{size: 16, get: func(ctx context.Context, _, _ int64) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	cfg := readerConfig(2, 0)
	cfg.Stream.ChunkTimeout = 10 * time.Millisecond
	r, err := newTGMultiReader(context.Background(), 0, 31, cfg, src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, err = io.ReadAll(r)
	if !errors.Is(err, ErrChunkTimeout) {
		t.Fatalf("got %v", err)
	}
}

func TestMultiReaderBoundedPrefetch(t *testing.T) {
	var calls atomic.Int32
	full := make(chan struct{})
	src := testChunkSource{size: 16, get: func(context.Context, int64, int64) ([]byte, error) {
		if calls.Add(1) == 5 {
			close(full)
		}
		return make([]byte, 16), nil
	}}
	r, err := newTGMultiReader(context.Background(), 0, 1024*1024-1, readerConfig(3, 2), src)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	select {
	case <-full:
	case <-time.After(time.Second):
		t.Fatal("window did not fill")
	}
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 5 {
		t.Fatalf("unconsumed reader fetched %d chunks, want 5", got)
	}
}

func TestMultiReaderInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		start, end, size int64
		threads, buffers int
	}{
		{0, 10, 0, 2, 0}, {-1, 10, 16, 2, 0}, {10, 0, 16, 2, 0}, {0, 10, 16, 0, 0}, {0, 10, 16, 2, -1},
	} {
		r, err := newTGMultiReader(context.Background(), tc.start, tc.end, readerConfig(tc.threads, tc.buffers), testChunkSource{size: tc.size})
		if err == nil {
			r.Close()
			t.Fatalf("accepted invalid settings: %+v", tc)
		}
	}
}

func BenchmarkMultiReaderVariableLatency(b *testing.B) {
	for range b.N {
		src := testChunkSource{size: 1024, get: func(ctx context.Context, offset, limit int64) ([]byte, error) {
			delay := time.Millisecond
			if offset/1024%5 == 3 {
				delay = 8 * time.Millisecond
			}
			select {
			case <-time.After(delay):
				return make([]byte, limit), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		r, err := newTGMultiReader(context.Background(), 0, 32*1024-1, readerConfig(4, 2), src)
		if err != nil {
			b.Fatal(err)
		}
		_, err = io.Copy(io.Discard, r)
		r.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}
