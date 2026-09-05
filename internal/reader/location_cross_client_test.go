package reader

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/tgdrive/teldrive/internal/cache"
)

// Models references valid only for the client that obtained them. Both readers
// share the production location key, as separate bot-backed requests can do.
func TestRefreshDoesNotTrustOtherClientsNewerReference(t *testing.T) {
	c := cache.NewMemoryCache(1024 * 1024)
	if err := c.Set("shared", &tg.InputDocumentFileLocation{ID: 9, FileReference: []byte("expired")}, time.Hour); err != nil {
		t.Fatal(err)
	}
	oldRead, release := make(chan struct{}), make(chan struct{})
	source := func(name string) *chunkSource {
		inv := telegram.InvokeFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
			switch req := input.(type) {
			case *tg.UploadGetFileRequest:
				ref := string(req.Location.(*tg.InputDocumentFileLocation).FileReference)
				if name == "A" && ref == "expired" {
					close(oldRead)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				if ref != name {
					return tgerr.New(400, "FILE_REFERENCE_EXPIRED")
				}
				output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte("valid:" + name)}
			case *tg.ChannelsGetChannelsRequest:
				output.(*tg.MessagesChatsBox).Chats = &tg.MessagesChats{Chats: []tg.ChatClass{&tg.Channel{ID: 7, AccessHash: 8}}}
			case *tg.ChannelsGetMessagesRequest:
				output.(*tg.MessagesMessagesBox).Messages = &tg.MessagesChannelMessages{Messages: []tg.MessageClass{&tg.Message{ID: 10, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 9, FileReference: []byte(name)}}}}}
			default:
				return fmt.Errorf("unexpected RPC %T", req)
			}
			return nil
		})
		return &chunkSource{channelId: 7, partId: 10, key: "shared", cache: c, useCache: true, client: tg.NewClient(inv)}
	}
	a, b := source("A"), source("B")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		data, err := a.Chunk(ctx, 0, 1024)
		if err == nil && string(data) != "valid:A" {
			err = fmt.Errorf("bad bytes %q", data)
		}
		result <- err
	}()
	<-oldRead
	// B refreshes the shared expired entry while A's failed read is in flight.
	data, err := b.Chunk(ctx, 0, 1024)
	if err != nil || string(data) != "valid:B" {
		close(release)
		t.Fatalf("reader B: %q %v", data, err)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("reader A must refresh using its own client: %v", err)
	}
}

func TestRefreshDoesNotReuseLocalReferenceFromEarlierChunk(t *testing.T) {
	var generation int
	inv := telegram.InvokeFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		switch req := input.(type) {
		case *tg.ChannelsGetChannelsRequest:
			output.(*tg.MessagesChatsBox).Chats = &tg.MessagesChats{Chats: []tg.ChatClass{&tg.Channel{ID: 7, AccessHash: 8}}}
		case *tg.ChannelsGetMessagesRequest:
			generation++
			output.(*tg.MessagesMessagesBox).Messages = &tg.MessagesChannelMessages{Messages: []tg.MessageClass{&tg.Message{ID: 10, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 9, FileReference: []byte(fmt.Sprint(generation))}}}}}
		case *tg.UploadGetFileRequest:
			ref := string(req.Location.(*tg.InputDocumentFileLocation).FileReference)
			if (req.Offset == 0 && ref != "1") || (req.Offset != 0 && ref != "2") {
				return tgerr.New(400, "FILE_REFERENCE_EXPIRED")
			}
			output.(*tg.UploadFileBox).File = &tg.UploadFile{Bytes: []byte("valid")}
		default:
			return fmt.Errorf("unexpected RPC %T", req)
		}
		return nil
	})
	src := &chunkSource{channelId: 7, partId: 10, key: "shared", cache: cache.NewMemoryCache(1024 * 1024), useCache: true, client: tg.NewClient(inv)}
	if _, err := src.Chunk(context.Background(), 0, 1024); err != nil {
		t.Fatal(err)
	}
	if err := src.cache.Set(src.key, &tg.InputDocumentFileLocation{ID: 9, FileReference: []byte("other-client")}, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Chunk(context.Background(), 1024, 1024); err != nil {
		t.Fatalf("must obtain new own-client reference: %v", err)
	}
	if generation != 2 {
		t.Fatalf("metadata fetches=%d want=2", generation)
	}
}
