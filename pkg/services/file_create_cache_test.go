package services

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/tgdrive/teldrive/internal/api"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/events"
	"github.com/tgdrive/teldrive/pkg/types"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// The driver exercises FilesCreate's actual successful/failed SQL boundary
// without a PostgreSQL server or Telegram access.
type createCacheDriver struct{}
type createCacheConn struct{ fail bool }
type createCacheRows struct{ done bool }

func (createCacheDriver) Open(name string) (driver.Conn, error) {
	return &createCacheConn{fail: name == "fail"}, nil
}
func (*createCacheConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (*createCacheConn) Close() error              { return nil }
func (*createCacheConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c *createCacheConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(q, "INSERT INTO teldrive.files") {
		return nil, errors.New("unexpected query")
	}
	if c.fail {
		return nil, errors.New("injected write failure")
	}
	return &createCacheRows{}, nil
}
func (*createCacheRows) Columns() []string {
	return []string{"id", "name", "type", "size", "encrypted", "parent_id", "parts", "channel_id"}
}
func (*createCacheRows) Close() error { return nil }
func (r *createCacheRows) Next(v []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	copy(v, []driver.Value{"same-file", "renamed.bin", "file", int64(128), false, "parent", `[{"id":202}]`, int64(7)})
	return nil
}

type createCacheContext struct{ context.Context }

func (c createCacheContext) Value(key any) any {
	if value := c.Context.Value(key); value != nil {
		return value
	}
	return &types.JWTClaims{RegisteredClaims: jwt.RegisteredClaims{Subject: "42"}}
}
func init() { sql.Register("teldrive-create-cache-test", createCacheDriver{}) }

func TestFilesCreateInvalidatesOverwriteCachesOnlyAfterSuccess(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "fail"
		}
		t.Run(name, func(t *testing.T) {
			connection, err := sql.Open("teldrive-create-cache-test", name)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: connection}), &gorm.Config{DisableAutomaticPing: true, SkipDefaultTransaction: true, Logger: logger.Default.LogMode(logger.Silent)})
			if err != nil {
				t.Fatal(err)
			}
			c := cache.NewMemoryCache(1024 * 1024)
			staleKeys := []string{cache.Key("files", "same-file"), cache.Key("files", "messages", "same-file")}
			untouched := []string{cache.Key("files", "other-file"), cache.Key("files", "messages", "other-file"), cache.Key("files", "location", "same-file", 101)}
			for _, key := range append(append([]string{}, staleKeys...), untouched...) {
				if err := c.Set(key, "old", time.Hour); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			service := apiService{db: db, cache: c, events: events.NewRecorder(ctx, db, zap.NewNop())}
			_, err = service.FilesCreate(createCacheContext{context.Background()}, &api.File{Name: "renamed.bin", Type: "file", ParentId: api.NewOptString("parent"), ChannelId: api.NewOptInt64(7), Size: api.NewOptInt64(128)})
			if (err != nil) != fail {
				t.Fatalf("error = %v, want failure %v", err, fail)
			}
			for _, key := range staleKeys {
				var value string
				err := c.Get(key, &value)
				if fail && err != nil {
					t.Errorf("failed write evicted %s", key)
				}
				if !fail && err == nil {
					t.Errorf("successful replacement retained stale %s", key)
				}
			}
			for _, key := range untouched {
				var value string
				if err := c.Get(key, &value); err != nil || value != "old" {
					t.Errorf("unrelated/versioned cache %s modified", key)
				}
			}
		})
	}
}
