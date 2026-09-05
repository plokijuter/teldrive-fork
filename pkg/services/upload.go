package services

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gotd/td/tgerr"
	"github.com/tgdrive/teldrive/internal/api"
	"github.com/tgdrive/teldrive/internal/auth"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/crypt"
	"github.com/tgdrive/teldrive/internal/logging"
	"github.com/tgdrive/teldrive/internal/pool"
	"github.com/tgdrive/teldrive/internal/tgc"
	"go.uber.org/zap"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/tgdrive/teldrive/pkg/mapper"
	"github.com/tgdrive/teldrive/pkg/models"
)

var (
	saltLength      = 32
	ErrUploadFailed = errors.New("upload failed")
)

// RateLimitError represents a rate limit error with retry information
type RateLimitError struct {
	RetryAfter time.Duration
	Message    string
}

func (e *RateLimitError) Error() string {
	return e.Message
}

func (a *apiService) UploadsDelete(ctx context.Context, params api.UploadsDeleteParams) error {
	if err := a.db.Where("upload_id = ?", params.ID).Delete(&models.Upload{}).Error; err != nil {
		return &api.ErrorStatusCode{StatusCode: 500, Response: api.Error{Message: err.Error(), Code: 500}}
	}
	return nil
}

func (a *apiService) UploadsPartsById(ctx context.Context, params api.UploadsPartsByIdParams) ([]api.UploadPart, error) {
	parts := []models.Upload{}
	if err := a.db.Model(&models.Upload{}).Order("part_no").Where("upload_id = ?", params.ID).
		// Le filtre etait "created_at < now + retention" : toujours vrai, donc
		// sans effet. On veut au contraire ECARTER les parts perimees, sur
		// lesquelles rclone reprendrait alors que leurs messages Telegram vont
		// disparaitre -> fichier commite orphelin.
		Where("created_at > ?", time.Now().UTC().Add(-a.cnf.TG.Uploads.Retention)).
		Find(&parts).Error; err != nil {
		return nil, &apiError{err: err}
	}
	return mapper.ToUploadOut(parts), nil
}

func (a *apiService) UploadsStats(ctx context.Context, params api.UploadsStatsParams) ([]api.UploadStats, error) {
	userId := auth.GetUser(ctx)
	var stats []api.UploadStats
	err := a.db.Raw(`
    SELECT
    dates.upload_date::date AS upload_date,
    COALESCE(SUM(files.size), 0)::bigint AS total_uploaded
    FROM
        generate_series(
            (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date - INTERVAL '1 day' * @days,
            (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date,
            '1 day'
        ) AS dates(upload_date)
    LEFT JOIN
    teldrive.files AS files
    ON
        dates.upload_date = DATE_TRUNC('day', files.created_at)::date
        AND files.type = 'file'
        AND files.user_id = @userId
    GROUP BY
        dates.upload_date
    ORDER BY
        dates.upload_date
  `, sql.Named("days", params.Days-1), sql.Named("userId", userId)).Scan(&stats).Error

	if err != nil {
		return nil, &apiError{err: err}

	}
	return stats, nil
}

func (a *apiService) UploadsUpload(ctx context.Context, req *api.UploadsUploadReqWithContentType, params api.UploadsUploadParams) (*api.UploadPart, error) {
	var (
		channelId int64
		err       error
		out       api.UploadPart
	)

	if params.Encrypted.Value && a.cnf.TG.Uploads.EncryptionKey == "" {
		return nil, &apiError{err: errors.New("encryption is not enabled"), code: 400}
	}

	userId := auth.GetUser(ctx)
	fileSize := params.ContentLength
	logger := logging.FromContext(ctx)

	if params.ChannelId.Value == 0 {
		channelId, err = a.channelManager.GetChannelForUpload(ctx, userId)
		if err != nil {
			return nil, err
		}
	} else {
		channelId = params.ChannelId.Value
	}

	tokenProxies, err := a.channelManager.BotTokensWithProxies(userId)
	if err != nil {
		return nil, err
	}

	// Extract tokens and build proxy map
	tokens := make([]string, len(tokenProxies))
	proxyMap := make(map[string]string)
	for i, tp := range tokenProxies {
		tokens[i] = tp.Token
		proxyMap[tp.Token] = tp.ProxyUrl
	}

	// Le corps est stocke sur DISQUE et non en RAM. Le io.ReadAll precedent
	// couplait la concurrence a la memoire : transfers x upload_concurrency x
	// chunk_size octets vivants a la fois (18 x 500 Mio = ~9 Go mesures), donc
	// GOMEMLIMIT=8GiB atteint des qu'on monte le parallelisme, GC en boucle et
	// debit effondre. Le fichier temporaire permet de garder la rotation de
	// bots (chaque tentative reouvre le fichier) sans payer en RAM.
	tmpFile, err := os.CreateTemp("", "teldrive-part-*.tmp")
	if err != nil {
		return nil, &apiError{err: fmt.Errorf("failed to create temp file: %w", err), code: 500}
	}
	contentPath := tmpFile.Name()
	defer func() {
		tmpFile.Close()
		os.Remove(contentPath)
	}()
	if _, err := io.Copy(tmpFile, req.Content.Data); err != nil {
		return nil, &apiError{err: fmt.Errorf("failed to read file: %w", err), code: 500}
	}

	// Apply chunk delay to avoid flood (configurable)
	if a.cnf.TG.Uploads.ChunkDelay > 0 {
		logger.Debug("applying chunk delay",
			zap.Duration("delay", a.cnf.TG.Uploads.ChunkDelay),
			zap.String("fileName", params.FileName),
			zap.Int("chunkNo", params.PartNo))
		time.Sleep(a.cnf.TG.Uploads.ChunkDelay)
	}

	// No bots configured - use user session (no retry)
	if len(tokens) == 0 {
		client, err := tgc.AuthClient(ctx, &a.cnf.TG, auth.GetJWTUser(ctx).TgSession)
		if err != nil {
			return nil, err
		}
		channelUser := strconv.FormatInt(userId, 10)
		return a.doUpload(ctx, client, "", channelUser, 0, channelId, contentPath, fileSize, params, userId, false, &out, logger)
	}

	// Set up bot rotation with proxies
	a.worker.SetWithProxies(tokens, proxyMap, userId)

	// Track tried bots to avoid infinite loops
	triedBots := make(map[string]bool)
	maxRetries := len(tokens)
	var lastErr error
	var minWaitDuration time.Duration // Track minimum wait time for 429 response
	maxWaitCycles := 3                // Maximum number of times to wait and retry

	for waitCycle := 0; waitCycle <= maxWaitCycles; waitCycle++ {
		// Reset for new cycle
		if waitCycle > 0 {
			triedBots = make(map[string]bool)
			minWaitDuration = 0
		}

		for retry := 0; retry < maxRetries; retry++ {
			token, index := a.worker.Next(userId)

			// Skip if we already tried this bot
			if triedBots[token] {
				continue
			}
			triedBots[token] = true

			channelUser := strings.Split(token, ":")[0]

			logger.Debug("uploading chunk",
				zap.String("fileName", params.FileName),
				zap.String("partName", params.PartName),
				zap.String("bot", channelUser),
				zap.Int("botNo", index),
				zap.Int("chunkNo", params.PartNo),
				zap.Int64("partSize", fileSize),
				zap.Int("retryAttempt", retry),
			)

			proxyUrl := a.worker.GetProxy(token)
			if proxyUrl != "" {
				logger.Debug("using proxy for upload", zap.String("bot", channelUser), zap.String("proxy", proxyUrl))
			}
			// Add AuthRecovery middleware
			sessionKey := cache.Key("sessions", a.cnf.TG.SessionInstance, channelUser)
			uploadMiddlewares := []telegram.Middleware{tgc.NewAuthRecovery(a.db, a.cache, sessionKey)}

			// Cache de clients MTProto persistants ([tg.stream] client-cache).
			// Sans lui, CHAQUE part payait une poignee de main complete plus un
			// aller-retour Auth().Status() vers Telegram. En cas d'echec on
			// retombe silencieusement sur le client par requete : le cache ne
			// doit jamais empecher un envoi.
			clientIsLive := false
			var client *telegram.Client
			var err error
			if a.cnf.TG.Stream.ClientCache {
				if liveClient, liveErr := tgc.GetLiveClient(ctx, a.db, a.cache, &a.cnf.TG, token, proxyUrl); liveErr == nil {
					client = liveClient
					clientIsLive = true
				} else {
					logger.Debug("client cache indisponible a l'envoi, repli sur un client par requete",
						zap.String("bot", channelUser), zap.Error(liveErr))
				}
			}
			if !clientIsLive {
				client, err = tgc.BotClient(ctx, a.db, a.cache, &a.cnf.TG, token, proxyUrl, uploadMiddlewares...)
			}
			if err != nil {
				// Check if it's a proxy error - retry without proxy
				if proxyUrl != "" && isProxyError(err) {
					logger.Warn("proxy connection failed, retrying without proxy",
						zap.String("bot", channelUser),
						zap.String("proxy", proxyUrl),
						zap.Error(err))
					client, err = tgc.BotClient(ctx, a.db, a.cache, &a.cnf.TG, token, "", uploadMiddlewares...)
				}
				if err != nil {
					// Check if it's AUTH_KEY_UNREGISTERED - block bot and try another
					if tgc.IsAuthKeyUnregistered(err) {
						tgc.BlockBot(channelUser, 5*time.Minute)
						logger.Warn("bot AUTH_KEY_UNREGISTERED during upload client creation, trying another bot",
							zap.String("bot", channelUser),
							zap.Int("retryAttempt", retry),
							zap.Error(err))
						lastErr = err
						continue
					}
					// Check if it's a FLOOD_WAIT during client creation
					if waitDuration, ok := tgerr.AsFloodWait(err); ok {
						a.worker.MarkBlocked(token, waitDuration)
						// Track minimum wait duration
						if minWaitDuration == 0 || waitDuration < minWaitDuration {
							minWaitDuration = waitDuration
						}
						logger.Warn("bot blocked during client creation, trying another bot",
							zap.String("bot", channelUser),
							zap.Duration("waitDuration", waitDuration),
							zap.Int("retryAttempt", retry))
						lastErr = err
						continue
					}
					return nil, err
				}
			}

			result, err := a.doUpload(ctx, client, token, channelUser, index, channelId, contentPath, fileSize, params, userId, clientIsLive, &out, logger)
			if err != nil {
				// Check if it's AUTH_KEY_UNREGISTERED - block bot and try another
				if tgc.IsAuthKeyUnregistered(err) {
					tgc.BlockBot(channelUser, 5*time.Minute)
					logger.Warn("bot AUTH_KEY_UNREGISTERED during upload, trying another bot",
						zap.String("bot", channelUser),
						zap.Int("retryAttempt", retry),
						zap.Error(err))
					lastErr = err
					continue
				}
				// Check if it's a FLOOD_WAIT error - try another bot
				if waitDuration, ok := tgerr.AsFloodWait(err); ok {
					a.worker.MarkBlocked(token, waitDuration)
					// Track minimum wait duration
					if minWaitDuration == 0 || waitDuration < minWaitDuration {
						minWaitDuration = waitDuration
					}
					logger.Warn("bot blocked during upload, trying another bot",
						zap.String("bot", channelUser),
						zap.Duration("waitDuration", waitDuration),
						zap.Int("retryAttempt", retry))
					lastErr = err
					continue
				}
				// Non-recoverable error - return immediately
				return nil, err
			}
			return result, nil
		}

		// Inner loop exhausted all bots - check if we should wait and retry
		if minWaitDuration > 0 && waitCycle < maxWaitCycles {
			logger.Warn("all bots rate-limited, waiting before retry",
				zap.String("fileName", params.FileName),
				zap.Int("chunkNo", params.PartNo),
				zap.Int("waitCycle", waitCycle+1),
				zap.Int("maxWaitCycles", maxWaitCycles),
				zap.Duration("waitDuration", minWaitDuration))
			time.Sleep(minWaitDuration)
			continue // Retry with fresh bot list
		}

		// No more retries or no wait duration
		break
	}

	// All bots exhausted after all wait cycles
	logger.Error("all bots exhausted, upload failed",
		zap.String("fileName", params.FileName),
		zap.String("partName", params.PartName),
		zap.Int("chunkNo", params.PartNo),
		zap.Int("botsTriedCount", len(triedBots)),
		zap.Duration("minWaitDuration", minWaitDuration))

	// Return 429 if rate-limited
	if minWaitDuration > 0 {
		return nil, &api.ErrorStatusCode{
			StatusCode: 429,
			Response: api.Error{
				Message: fmt.Sprintf("All bots rate-limited. Retry after %d seconds", int(minWaitDuration.Seconds())),
				Code:    429,
			},
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ErrUploadFailed
}

// doUpload performs the actual upload with a specific bot/client
func (a *apiService) doUpload(ctx context.Context, client *telegram.Client, token string, channelUser string, index int, channelId int64, contentPath string, fileSize int64, params api.UploadsUploadParams, userId int64, clientIsLive bool, out *api.UploadPart, logger *zap.Logger) (*api.UploadPart, error) {

	middlewares := tgc.NewMiddleware(&a.cnf.TG, tgc.WithFloodWait(),
		tgc.WithRecovery(),
		tgc.WithRetry(a.cnf.TG.Uploads.MaxRetries),
		tgc.WithRateLimit())

	uploadPool := pool.NewPool(client, int64(a.cnf.TG.PoolSize), middlewares...)
	defer uploadPool.Close()

	var uploadErr error

	uploadWork := func(ctx context.Context) error {

		channel, err := tgc.GetChannelById(ctx, client.API(), channelId)
		if err != nil {
			return err
		}

		// Relecture depuis le fichier temporaire : chaque tentative de bot
		// repart du debut sans que le morceau vive en memoire.
		partFile, err := os.Open(contentPath)
		if err != nil {
			return err
		}
		defer partFile.Close()
		fileStream := io.Reader(partFile)
		actualFileSize := fileSize

		var salt string

		if params.Encrypted.Value {
			salt, _ = generateRandomSalt()
			cipher, err := crypt.NewCipher(a.cnf.TG.Uploads.EncryptionKey, salt)
			if err != nil {
				return err
			}
			actualFileSize = crypt.EncryptedSize(fileSize)
			fileStream, err = cipher.EncryptData(fileStream)
			if err != nil {
				return err
			}
		}

		poolClient := uploadPool.Default(ctx)

		u := uploader.NewUploader(poolClient).WithThreads(a.cnf.TG.Uploads.Threads).WithPartSize(512 * 1024)

		upload, err := u.Upload(ctx, uploader.NewUpload(params.PartName, fileStream, actualFileSize))
		if err != nil {
			return err
		}

		document := message.UploadedDocument(upload).Filename(params.PartName).ForceFile(true)

		sender := message.NewSender(poolClient)

		target := sender.To(&tg.InputPeerChannel{ChannelID: channel.ChannelID,
			AccessHash: channel.AccessHash})

		res, err := target.Media(ctx, document)
		if err != nil {
			return err
		}

		updates := res.(*tg.Updates)

		var msg *tg.Message

		for _, update := range updates.Updates {
			channelMsg, ok := update.(*tg.UpdateNewChannelMessage)
			if ok {
				msg = channelMsg.Message.(*tg.Message)
				break
			}
		}

		if msg.ID == 0 {
			return fmt.Errorf("upload failed")
		}

		partUpload := &models.Upload{
			Name:      params.PartName,
			UploadId:  params.ID,
			PartId:    msg.ID,
			ChannelId: channelId,
			Size:      actualFileSize,
			PartNo:    int(params.PartNo),
			UserId:    userId,
			Encrypted: params.Encrypted.Value,
			Salt:      salt,
		}

		if err := a.db.Create(partUpload).Error; err != nil {
			return err
		}

		v, err := poolClient.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{Channel: channel, ID: []tg.InputMessageClass{&tg.InputMessageID{ID: msg.ID}}})
		if err != nil || v == nil {
			return ErrUploadFailed
		}

		switch msgs := v.(type) {
		case *tg.MessagesChannelMessages:
			if len(msgs.Messages) == 0 {
				return ErrUploadFailed
			}
			doc, ok := msgDocument(msgs.Messages[0])
			if !ok {
				return ErrUploadFailed
			}
			if doc.Size != actualFileSize {
				poolClient.ChannelsDeleteMessages(ctx, &tg.ChannelsDeleteMessagesRequest{Channel: channel, ID: []int{msg.ID}})
				return ErrUploadFailed
			}
		default:
			return ErrUploadFailed
		}

		*out = api.UploadPart{
			Name:      partUpload.Name,
			PartId:    partUpload.PartId,
			ChannelId: partUpload.ChannelId,
			PartNo:    partUpload.PartNo,
			Size:      partUpload.Size,
			Encrypted: partUpload.Encrypted,
		}
		out.SetSalt(api.NewOptString(partUpload.Salt))
		return nil
	}

	// Meme raisonnement que sur le chemin de lecture (file.go) : un client
	// issu du cache est deja connecte et autorise. Le passer a RunWithAuth
	// ouvrirait une SECONDE connexion (poignee de main MTProto ~1,3 s) puis
	// la fermerait en sortie -- paye a CHAQUE part envoyee.
	if clientIsLive {
		uploadErr = uploadWork(ctx)
	} else {
		uploadErr = tgc.RunWithAuth(ctx, client, token, uploadWork)
	}

	if uploadErr != nil {
		logger.Error("upload failed", zap.String("fileName", params.FileName),
			zap.String("partName", params.PartName),
			zap.String("bot", channelUser),
			zap.Int("botNo", index),
			zap.Int("chunkNo", params.PartNo),
			zap.Error(uploadErr))
		return nil, uploadErr
	}

	logger.Debug("upload finished", zap.String("fileName", params.FileName),
		zap.String("partName", params.PartName),
		zap.String("bot", channelUser),
		zap.Int("chunkNo", params.PartNo))
	return out, nil
}

func msgDocument(m tg.MessageClass) (*tg.Document, bool) {
	res, ok := m.AsNotEmpty()
	if !ok {
		return nil, false
	}
	msg, ok := res.(*tg.Message)
	if !ok {
		return nil, false
	}

	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok {
		return nil, false
	}
	return media.Document.AsNotEmpty()
}

func generateRandomSalt() (string, error) {
	randomBytes := make([]byte, saltLength)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return "", err
	}

	hasher := sha256.New()
	hasher.Write(randomBytes)
	hashedSalt := base64.URLEncoding.EncodeToString(hasher.Sum(nil))

	return hashedSalt, nil
}
