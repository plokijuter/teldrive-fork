package tgc

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/config"
	"github.com/tgdrive/teldrive/internal/logging"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// CLIENTS MTPROTO PERSISTANTS (ajoute le 2026-08-24).
//
// LE PROBLEME. teldrive construisait un *telegram.Client NEUF a chaque requete
// HTTP, puis appelait client.Run() -- qui se connecte, execute, et DECONNECTE.
// Chaque requete payait donc une poignee de main MTProto complete : dial TCP,
// poignee de main du transport, invokeWithLayer, users.getUsers. Mesure du
// 2026-08-24 sur le montage rclone : ~1,3 s par aller-retour a froid. Or
// ffmpeg en enchaine 3 a 4 pour demarrer une lecture (en-tete du conteneur,
// index en fin de fichier, position cible, puis assez d'octets pour decoder),
// ce qui donnait les 4 a 6 secondes ressenties a chaque saut.
//
// LA SOLUTION. Garder un client vivant PAR BOT. client.Run() bloque tant que sa
// closure n'a pas rendu la main : on la garde donc ouverte dans une goroutine
// dediee, et les requetes HTTP se contentent d'utiliser client.API(). La
// connexion, la session et l'autorisation du bot sont payees UNE fois.
//
// SUR LA SECURITE DE PARTAGER UN CLIENT. Le moteur RPC de gotd indexe les
// reponses par msg_id dans une table protegee par mutex : plusieurs requetes
// concurrentes sur un meme client sont donc sures, et c'est deja ce que fait
// le pool d'envoi. gotd se reconnecte seul en cas de coupure
// (ReconnectionBackoff), a l'interieur du Run que nous maintenons ouvert.
//
// PRUDENCE. Active uniquement par [tg.stream] client-cache. A false, le
// comportement d'origine (un client par requete) est conserve a l'identique.

const (
	// delai d'attente pour qu'un client neuf soit connecte ET autorise
	clientReadyTimeout = 30 * time.Second
	// au-dela de cette inactivite, un client est ferme pour ne pas garder
	// des sockets ouverts sur des bots qui ne servent plus
	clientIdleTimeout = 10 * time.Minute
)

type cachedClient struct {
	client    *telegram.Client
	cancel    context.CancelFunc
	ready     chan struct{} // ferme des que le client est utilisable
	readyOnce sync.Once
	err       error     // renseigne avant la fermeture de ready en cas d'echec
	lastAt    time.Time // derniere utilisation, pour la peremption
}

func (cc *cachedClient) signalReady(err error) {
	cc.readyOnce.Do(func() {
		cc.err = err
		close(cc.ready)
	})
}

type clientCache struct {
	mu sync.Mutex
	m  map[string]*cachedClient
}

var liveClients = &clientCache{m: map[string]*cachedClient{}}

// GetLiveClient rend un client MTProto DEJA CONNECTE et autorise pour ce bot,
// en le creant au premier appel. Le client reste vivant entre les requetes.
//
// The shared client owns a stable middleware policy. Request-specific policies
// must not depend on whether an upload or download populated the cache first.
func GetLiveClient(ctx context.Context, db *gorm.DB, c cache.Cacher, cnf *config.TGConfig,
	token, proxyUrl string) (*telegram.Client, error) {

	if token == "" {
		return nil, errors.New("client cache: jeton vide (session utilisateur non geree ici)")
	}
	botID := strings.Split(token, ":")[0]
	key := botID + "|" + proxyUrl

	liveClients.mu.Lock()
	if cc, ok := liveClients.m[key]; ok {
		cc.lastAt = time.Now()
		liveClients.mu.Unlock()
		select {
		case <-cc.ready:
			if cc.err != nil {
				liveClients.drop(key, cc)
				return nil, cc.err
			}
			return cc.client, nil
		case <-time.After(clientReadyTimeout):
			return nil, errors.New("client cache: delai depasse en attendant un client existant")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	client, err := BotClient(ctx, db, c, cnf, token, proxyUrl, liveClientMiddlewares(db, c, cnf, botID)...)
	if err != nil {
		liveClients.mu.Unlock()
		return nil, err
	}

	// Contexte DETACHE de la requete HTTP : le client doit survivre a celle-ci.
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	cc := &cachedClient{client: client, cancel: cancel, ready: make(chan struct{}), lastAt: time.Now()}
	liveClients.m[key] = cc
	liveClients.mu.Unlock()

	logger := logging.FromContext(ctx)
	go func() {
		err := client.Run(runCtx, func(rc context.Context) error {
			if err := authorizeBot(rc, client, token, logger); err != nil {
				cc.signalReady(err)
				return err
			}
			logger.Debug("client cache: bot connecte et maintenu ouvert", zap.String("bot", botID))
			cc.signalReady(nil)
			// On garde la closure ouverte : c'est ce qui maintient la
			// connexion. gotd gere les reconnexions a l'interieur.
			<-rc.Done()
			return nil
		})
		// Sortie de Run : la connexion est morte, on retire l'entree pour
		// qu'un appel suivant en recree une.
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Debug("client cache: client termine", zap.String("bot", botID), zap.Error(err))
		}
		// Run may fail before invoking its callback (dial/session error).
		// Wake waiters immediately instead of leaving them asleep for 30s.
		readyErr := err
		if readyErr == nil {
			readyErr = errors.New("client cache: client stopped before becoming ready")
		}
		cc.signalReady(readyErr)
		liveClients.drop(key, cc)
	}()

	select {
	case <-cc.ready:
		if cc.err != nil {
			return nil, cc.err
		}
		return cc.client, nil
	case <-time.After(clientReadyTimeout):
		liveClients.drop(key, cc)
		return nil, errors.New("client cache: delai depasse a la connexion du bot")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func liveClientMiddlewares(db *gorm.DB, c cache.Cacher, cnf *config.TGConfig, botID string) []telegram.Middleware {
	middlewares := NewMiddleware(cnf, WithFloodWait(), WithRecovery(), WithRetry(5), WithRateLimit())
	sessionKey := cache.Key("sessions", cnf.SessionInstance, botID)
	return append(middlewares, NewAuthRecovery(db, c, sessionKey))
}

// authorizeBot reprend la logique de RunWithAuth, cote bot uniquement.
func authorizeBot(ctx context.Context, client *telegram.Client, token string, logger *zap.Logger) error {
	status, err := client.Auth().Status(ctx)
	if err != nil {
		return err
	}
	if !status.Authorized {
		logger.Debug("client cache: creation de la session bot")
		if _, err := client.Auth().Bot(ctx, token); err != nil {
			return err
		}
	}
	return nil
}

func (cc *clientCache) drop(key string, expected *cachedClient) {
	cc.mu.Lock()
	entry, ok := cc.m[key]
	ok = ok && entry == expected
	if ok {
		delete(cc.m, key)
	}
	cc.mu.Unlock()
	if ok && entry.cancel != nil {
		entry.cancel()
	}
}

// CloseLiveClients ferme tous les clients maintenus ouverts.
func CloseLiveClients() {
	liveClients.mu.Lock()
	entries := liveClients.m
	liveClients.m = map[string]*cachedClient{}
	liveClients.mu.Unlock()
	for _, entry := range entries {
		if entry.cancel != nil {
			entry.cancel()
		}
	}
}

// ReapIdleClients ferme les clients inutilises depuis clientIdleTimeout.
func ReapIdleClients() {
	now := time.Now()
	liveClients.mu.Lock()
	var stale []*cachedClient
	for k, v := range liveClients.m {
		if now.Sub(v.lastAt) > clientIdleTimeout {
			delete(liveClients.m, k)
			stale = append(stale, v)
		}
	}
	liveClients.mu.Unlock()
	for _, entry := range stale {
		if entry.cancel != nil {
			entry.cancel()
		}
	}
}

// LiveClientCount rend le nombre de clients maintenus ouverts (diagnostic).
func LiveClientCount() int {
	liveClients.mu.Lock()
	defer liveClients.mu.Unlock()
	return len(liveClients.m)
}
