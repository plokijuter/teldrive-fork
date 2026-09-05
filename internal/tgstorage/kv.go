package tgstorage

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/tgdrive/teldrive/internal/cache"
	"gorm.io/gorm"

	"github.com/gotd/contrib/auth/kv"
)

type KeyValue struct {
	Key       string    `gorm:"type:text;primaryKey"`
	Value     []byte    `gorm:"type:bytea;not null"`
	CreatedAt time.Time `gorm:"default:timezone('utc'::text, now())"`
}

func (KeyValue) TableName() string {
	return "teldrive.kv"
}

type kvStorage struct {
	db    *gorm.DB
	cache cache.Cacher
}

func (s kvStorage) Set(ctx context.Context, k, v string) error {
	// L'ancienne version faisait un Get d'abord et NE REECRIVAIT PAS si la cle
	// existait deja : toute mise a jour de session gotd (rotation du sel serveur)
	// etait perdue en silence. Ca "marchait" parce que gotd renegocie via
	// bad_server_salt a la reconnexion -- au prix d'un aller-retour de plus a
	// chaque connexion a froid. On ecrit toujours.
	if err := s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(&KeyValue{
			Key:       k,
			Value:     []byte(v),
			CreatedAt: time.Now().UTC(),
		}).Error; err != nil {
			return errors.Wrap(err, "save value")
		}
		return nil
	}); err != nil {
		return err
	}
	// Get met en cache 60 min : sans invalidation on continuerait a servir
	// l'ancienne valeur qu'on vient justement de remplacer.
	s.cache.Delete(cache.Key(k))
	return nil
}

func (s kvStorage) Get(ctx context.Context, key string) (string, error) {
	return cache.Fetch(s.cache, cache.Key(key), 60*time.Minute, func() (string, error) {
		var entry KeyValue
		if err := s.db.First(&entry, "key = ?", key).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return "", kv.ErrKeyNotFound
			}
			return "", errors.Wrap(err, "query")
		}
		return string(entry.Value), nil
	})

}

func (s kvStorage) Delete(ctx context.Context, k string) error {
	if err := s.db.Where("key = ?", k).Delete(&KeyValue{}).Error; err != nil {
		return errors.Wrap(err, "delete key")
	}
	s.cache.Delete(k)
	return nil
}
