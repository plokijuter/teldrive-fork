package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/tgdrive/teldrive/internal/cache"
)

func TestExpiredReferenceRefreshContract(t *testing.T) {
	for _, repeatFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("fresh_reference_fails=%v", repeatFailure), func(t *testing.T) {
			c := cache.NewMemoryCache(1024 * 1024)
			key := "test-location"
			if err := c.Set(key, &tg.InputDocumentFileLocation{ID: 9, FileReference: []byte("old")}, time.Hour); err != nil {
				t.Fatal(err)
			}
			var calls []string
			invoker := telegram.InvokeFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
				switch req := input.(type) {
				case *tg.UploadGetFileRequest:
					ref := string(req.Location.(*tg.InputDocumentFileLocation).FileReference)
					calls = append(calls, "read:"+ref)
					if req.Offset != 1024 || req.Limit != 1024 {
						return fmt.Errorf("retry changed range")
					}
					if ref == "old" || repeatFailure {
						return tgerr.New(400, "FILE_REFERENCE_EXPIRED")
					}
					output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte("verified bytes")}
				case *tg.ChannelsGetChannelsRequest:
					calls = append(calls, "channels")
					output.(*tg.MessagesChatsBox).Chats = &tg.MessagesChats{Chats: []tg.ChatClass{&tg.Channel{ID: 7, AccessHash: 8}}}
				case *tg.ChannelsGetMessagesRequest:
					calls = append(calls, "messages")
					output.(*tg.MessagesMessagesBox).Messages = &tg.MessagesChannelMessages{Messages: []tg.MessageClass{&tg.Message{ID: 10, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 9, FileReference: []byte("fresh")}}}}}
				default:
					return fmt.Errorf("unexpected RPC %T", req)
				}
				return nil
			})
			src := &chunkSource{channelId: 7, partId: 10, key: key, cache: c, useCache: true, client: tg.NewClient(invoker)}
			got, err := src.Chunk(context.Background(), 1024, 1024)
			if repeatFailure {
				if !tgerr.Is(err, "FILE_REFERENCE_EXPIRED") {
					t.Fatalf("fresh failure lost: %v", err)
				}
			} else if err != nil || !bytes.Equal(got, []byte("verified bytes")) {
				t.Fatalf("bytes=%q err=%v", got, err)
			}
			if fmt.Sprint(calls) != "[read:old channels messages read:fresh]" {
				t.Fatalf("refresh sequence: %v", calls)
			}
			var stored *tg.InputDocumentFileLocation
			if err := c.Get(key, &stored); err != nil || stored == nil || string(stored.FileReference) != "fresh" {
				t.Fatalf("fresh location not cached: %+v err=%v", stored, err)
			}
		})
	}
}

func TestReferenceRefreshCancellationAfterRPCStarts(t *testing.T) {
	c := cache.NewMemoryCache(1024 * 1024)
	if err := c.Set("location", &tg.InputDocumentFileLocation{ID: 9, FileReference: []byte("old")}, time.Hour); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	invoker := telegram.InvokeFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		switch input.(type) {
		case *tg.UploadGetFileRequest:
			return tgerr.New(400, "FILE_REFERENCE_EXPIRED")
		case *tg.ChannelsGetChannelsRequest:
			close(started)
			<-ctx.Done()
			return ctx.Err()
		default:
			return fmt.Errorf("unexpected RPC after cancellation: %T", input)
		}
	})
	src := &chunkSource{channelId: 7, partId: 10, key: "location", cache: c, useCache: true, client: tg.NewClient(invoker)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := src.Chunk(ctx, 0, 1024); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("refresh never started")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("refresh ignored cancellation")
	}
}
