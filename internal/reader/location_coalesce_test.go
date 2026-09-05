package reader

import (
	"context"
	"errors"
	"fmt"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/tgdrive/teldrive/internal/cache"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Force every cold worker to observe a miss before any metadata RPC starts.
type coldLocationCache struct {
	cache.Cacher
	entered atomic.Int32
	ready   chan struct{}
	workers int32
}

func (c *coldLocationCache) Get(key string, value any) error {
	err := c.Cacher.Get(key, value)
	n := c.entered.Add(1)
	if n <= c.workers {
		if n == c.workers {
			close(c.ready)
		}
		<-c.ready
		return err
	}
	return err
}

func locationTestSource(t *testing.T, cached bool, hook func(context.Context, bin.Encoder) error) (*chunkSource, *atomic.Int32) {
	t.Helper()
	calls := new(atomic.Int32)
	inv := telegram.InvokeFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		if hook != nil {
			if err := hook(ctx, input); err != nil {
				return err
			}
		}
		switch req := input.(type) {
		case *tg.ChannelsGetChannelsRequest:
			calls.Add(1)
			output.(*tg.MessagesChatsBox).Chats = &tg.MessagesChats{Chats: []tg.ChatClass{&tg.Channel{ID: 7, AccessHash: 8}}}
		case *tg.ChannelsGetMessagesRequest:
			calls.Add(1)
			output.(*tg.MessagesMessagesBox).Messages = &tg.MessagesChannelMessages{Messages: []tg.MessageClass{&tg.Message{ID: 10, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 9, FileReference: []byte("fresh")}}}}}
		case *tg.UploadGetFileRequest:
			output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte(fmt.Sprint(req.Offset))}
		default:
			return fmt.Errorf("unexpected RPC %T", input)
		}
		return nil
	})
	return &chunkSource{channelId: 7, partId: 10, key: "location", cache: cache.NewMemoryCache(1024 * 1024), useCache: cached, client: tg.NewClient(inv)}, calls
}

func TestConcurrentLocationMetadataRPCs(t *testing.T) {
	for _, mode := range []string{"cold", "expired", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			const workers = 8
			start := make(chan struct{})
			var stale atomic.Int32
			src, calls := locationTestSource(t, mode != "disabled", func(ctx context.Context, req bin.Encoder) error {
				switch req := req.(type) {
				case *tg.UploadGetFileRequest:
					if string(req.Location.(*tg.InputDocumentFileLocation).FileReference) == "old" {
						if stale.Add(1) == workers {
							close(start)
						}
						select {
						case <-start:
						case <-ctx.Done():
							return ctx.Err()
						}
						return tgerr.New(400, "FILE_REFERENCE_EXPIRED")
					}
				}
				return nil
			})
			if mode == "cold" {
				src.cache = &coldLocationCache{Cacher: src.cache, ready: make(chan struct{}), workers: workers}
			}
			if mode == "expired" {
				if err := src.cache.Set(src.key, &tg.InputDocumentFileLocation{ID: 9, FileReference: []byte("old")}, time.Hour); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					data, err := src.Chunk(ctx, int64(i*1024), 1024)
					if err != nil || string(data) != fmt.Sprint(i*1024) {
						t.Errorf("worker %d: %q %v", i, data, err)
					}
				}(i)
			}
			wg.Wait()
			want := int32(2)
			if mode == "disabled" {
				want = 2 * workers
			}
			t.Logf("metadata RPCs for %d chunks: %d", workers, calls.Load())
			if calls.Load() != want {
				t.Fatalf("metadata RPCs=%d want=%d", calls.Load(), want)
			}
		})
	}
}

func TestLocationLeaderCancellationDoesNotPoisonFollower(t *testing.T) {
	entered := make(chan struct{})
	var attempts atomic.Int32
	src, calls := locationTestSource(t, true, func(ctx context.Context, req bin.Encoder) error {
		if _, ok := req.(*tg.ChannelsGetChannelsRequest); ok && attempts.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	leader, cancel := context.WithCancel(context.Background())
	result := make(chan error, 2)
	go func() { _, err := src.Chunk(leader, 0, 1024); result <- err }()
	<-entered
	go func() { _, err := src.Chunk(context.Background(), 1024, 1024); result <- err }()
	cancel()
	var canceled, success int
	for i := 0; i < 2; i++ {
		select {
		case err := <-result:
			if errors.Is(err, context.Canceled) {
				canceled++
			} else if err == nil {
				success++
			} else {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("blocked worker")
		}
	}
	if canceled != 1 || success != 1 || calls.Load() != 2 {
		t.Fatalf("canceled=%d success=%d RPCs=%d", canceled, success, calls.Load())
	}
}

func TestLocationWaitingCallerCanCancel(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	src, calls := locationTestSource(t, true, func(ctx context.Context, req bin.Encoder) error {
		if _, ok := req.(*tg.ChannelsGetChannelsRequest); ok {
			close(entered)
			<-release
		}
		return nil
	})
	first := make(chan error, 1)
	go func() { _, err := src.Chunk(context.Background(), 0, 1024); first <- err }()
	<-entered
	defer func() {
		close(release)
		select {
		case err := <-first:
			if err != nil {
				t.Errorf("leader failed: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("leader did not finish")
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { _, err := src.Chunk(ctx, 1024, 1024); second <- err }()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiter error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter blocked behind uncanceled leader")
	}
	if calls.Load() != 0 {
		t.Fatal("waiter initiated a metadata RPC")
	}
	// The leader is still blocked and owns its own context.
	select {
	case err := <-first:
		t.Fatalf("leader ended early: %v", err)
	default:
	}
}
