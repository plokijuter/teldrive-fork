package tgc

import (
	"context"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram"
	"github.com/tgdrive/teldrive/internal/logging"
	"go.uber.org/zap"
)

// ttlAuth : duree pendant laquelle on fait confiance a un bot deja vu autorise.
const ttlAuth = 10 * time.Minute

// autorisesRecemment : jeton de bot -> instant du dernier Status() concluant.
//
// client.Auth().Status() n'est PAS un controle local : gotd le traduit en
// users.getUsers(InputUserSelf), donc un vrai aller-retour vers Telegram, paye
// a CHAQUE ouverture de flux a froid et a chaque part envoyee. Or la session
// vit en base : une fois qu'un jeton a repondu "autorise", tout client ulterieur
// chargeant la meme session l'est aussi.
//
// Le seul comportement perdu serait la reconnexion automatique d'une session
// revoquee ; on le recupere en invalidant l'entree des qu'une erreur
// d'autorisation remonte du travail lui-meme.
var autorisesRecemment sync.Map

func RunWithAuth(ctx context.Context, client *telegram.Client, token string, f func(ctx context.Context) error) error {
	return client.Run(ctx, func(ctx context.Context) error {
		if token != "" {
			if v, ok := autorisesRecemment.Load(token); ok {
				if vu, ok := v.(time.Time); ok && time.Since(vu) < ttlAuth {
					err := f(ctx)
					if err != nil && IsAuthKeyUnregistered(err) {
						autorisesRecemment.Delete(token)
					}
					return err
				}
			}
		}

		status, err := client.Auth().Status(ctx)
		logger := logging.FromContext(ctx)
		if err != nil {
			return err
		}
		if token == "" {
			if !status.Authorized {
				return errors.Errorf("not authorized. please login first")
			}
			logger.Debug("User Session",
				zap.Int64("id", status.User.ID),
				zap.String("username", status.User.Username))
		} else {
			if !status.Authorized {
				logger.Debug("creating bot session")
				_, err := client.Auth().Bot(ctx, token)
				if err != nil {
					return err
				}
				status, _ = client.Auth().Status(ctx)
				if status != nil && status.User != nil {
					logger.Debug("Bot Session",
						zap.Int64("id", status.User.ID),
						zap.String("username", status.User.Username))
				} else {
					logger.Debug("Bot Session created (user info not available)")
				}
			}
		}

		if token != "" && status != nil && status.Authorized {
			autorisesRecemment.Store(token, time.Now())
		}

		err = f(ctx)
		if err != nil && token != "" && IsAuthKeyUnregistered(err) {
			autorisesRecemment.Delete(token)
		}
		return err
	})
}
