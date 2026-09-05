package services

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/tgdrive/teldrive/internal/api"
	"github.com/tgdrive/teldrive/internal/auth"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/category"
	"github.com/tgdrive/teldrive/internal/database"
	"github.com/tgdrive/teldrive/internal/events"
	"github.com/tgdrive/teldrive/internal/http_range"
	"github.com/tgdrive/teldrive/internal/logging"
	"github.com/tgdrive/teldrive/internal/md5"
	"github.com/tgdrive/teldrive/internal/pool"
	"github.com/tgdrive/teldrive/internal/reader"
	"github.com/tgdrive/teldrive/internal/tgc"
	"github.com/tgdrive/teldrive/internal/utils"
	"github.com/tgdrive/teldrive/pkg/mapper"
	"github.com/tgdrive/teldrive/pkg/models"
	"github.com/tgdrive/teldrive/pkg/types"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrorStreamAbandoned = errors.New("stream abandoned")
	defaultContentType   = "application/octet-stream"
)

// isProxyError checks if an error is likely related to proxy connectivity issues
func isProxyError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	// Check for common proxy-related error patterns
	proxyErrors := []string{
		"connection refused",
		"timeout",
		"dial tcp",
		"dial socks",
		"proxy",
		"socks",
		"i/o timeout",
		"no route to host",
		"network is unreachable",
	}
	for _, pattern := range proxyErrors {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	return false
}

type buffer struct {
	Buf []byte
}

func (b *buffer) long() (int64, error) {
	v, err := b.uint64()
	if err != nil {
		return 0, err
	}
	return int64(v), nil

}
func (b *buffer) uint64() (uint64, error) {
	const size = 8
	if len(b.Buf) < size {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.LittleEndian.Uint64(b.Buf)
	b.Buf = b.Buf[size:]
	return v, nil
}

func randInt64() (int64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(rand.Reader, buf[:]); err != nil {
		return 0, err
	}
	b := &buffer{Buf: buf[:]}
	return b.long()
}
func isUUID(str string) bool {
	_, err := uuid.Parse(str)
	return err == nil
}

type fullFileDB struct {
	models.File
	Path string
}

func (a *apiService) getFileFromPath(path string, userId int64) (*models.File, error) {

	var res []models.File

	if err := a.db.Raw("select * from teldrive.get_file_from_path(?, ?, ?)", path, userId, true).
		Scan(&res).Error; err != nil {
		return nil, err

	}
	if len(res) == 0 {
		return nil, database.ErrNotFound
	}
	return &res[0], nil
}

func (a *apiService) FilesCategoryStats(ctx context.Context) ([]api.CategoryStats, error) {
	userId := auth.GetUser(ctx)
	var stats []api.CategoryStats
	if err := a.db.Model(&models.File{}).Select("category", "COUNT(*) as total_files", "coalesce(SUM(size),0) as total_size").
		Where(&models.File{UserId: userId, Type: "file", Status: "active"}).
		Order("category ASC").Group("category").Find(&stats).Error; err != nil {
		return nil, &apiError{err: err}
	}

	return stats, nil
}

func (a *apiService) FilesCopy(ctx context.Context, req *api.FileCopy, params api.FilesCopyParams) (*api.File, error) {
	userId := auth.GetUser(ctx)

	client, _ := tgc.AuthClient(ctx, &a.cnf.TG, auth.GetJWTUser(ctx).TgSession, a.middlewares...)

	var res []models.File

	if err := a.db.Model(&models.File{}).Where("id = ?", params.ID).Find(&res).Error; err != nil {
		return nil, &apiError{err: err}
	}
	if len(res) == 0 {
		return nil, &apiError{err: errors.New("file not found"), code: 404}
	}

	file := res[0]

	newIds := []api.Part{}

	channelId, err := a.channelManager.CurrentChannel(userId)
	if err != nil {
		return nil, &apiError{err: err}
	}

	err = tgc.RunWithAuth(ctx, client, "", func(ctx context.Context) error {

		ids := utils.Map(file.Parts, func(part api.Part) int { return part.ID })
		messages, err := tgc.GetMessages(ctx, client.API(), ids, *file.ChannelId)

		if err != nil {
			return err
		}

		channel, err := tgc.GetChannelById(ctx, client.API(), channelId)

		if err != nil {
			return err
		}
		for i, message := range messages {
			item := message.(*tg.Message)
			media := item.Media.(*tg.MessageMediaDocument)
			document := media.Document.(*tg.Document)

			id, _ := randInt64()
			request := tg.MessagesSendMediaRequest{
				Silent:   true,
				Peer:     &tg.InputPeerChannel{ChannelID: channel.ChannelID, AccessHash: channel.AccessHash},
				Media:    &tg.InputMediaDocument{ID: document.AsInput()},
				RandomID: id,
			}
			res, err := client.API().MessagesSendMedia(ctx, &request)

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
			p := api.Part{ID: msg.ID}
			if file.Parts[i].Salt.Value != "" {
				p.Salt = file.Parts[i].Salt
			}
			newIds = append(newIds, p)

		}
		return nil
	})

	if err != nil {
		return nil, &apiError{err: err}
	}

	if len(newIds) != len(file.Parts) {
		return nil, &apiError{err: errors.New("failed to copy all file parts")}
	}

	var parentId string
	if !isUUID(req.Destination) {
		var destRes []models.File
		if err := a.db.Raw("select * from teldrive.create_directories(?, ?)", userId, req.Destination).
			Scan(&destRes).Error; err != nil {
			return nil, &apiError{err: err}
		}
		parentId = destRes[0].ID
	} else {
		parentId = req.Destination
	}

	dbFile := models.File{}

	dbFile.Name = req.NewName.Or(file.Name)
	dbFile.Size = file.Size
	dbFile.Type = string(file.Type)
	dbFile.MimeType = file.MimeType
	if len(newIds) > 0 {
		dbFile.Parts = datatypes.NewJSONSlice(newIds)
	}
	dbFile.UserId = userId
	dbFile.Status = "active"
	dbFile.ParentId = utils.Ptr(parentId)
	dbFile.ChannelId = &channelId
	dbFile.Encrypted = file.Encrypted
	dbFile.Category = string(file.Category)
	if req.UpdatedAt.IsSet() && !req.UpdatedAt.Value.IsZero() {
		dbFile.UpdatedAt = req.UpdatedAt.Value
	} else {
		dbFile.UpdatedAt = time.Now().UTC()
	}

	if err := a.db.Create(&dbFile).Error; err != nil {
		return nil, &apiError{err: err}
	}

	a.events.Record(events.OpCopy, userId, &models.Source{
		ID:       dbFile.ID,
		Type:     dbFile.Type,
		Name:     dbFile.Name,
		ParentID: parentId,
	})
	return mapper.ToFileOut(dbFile), nil
}

func (a *apiService) FilesCreate(ctx context.Context, fileIn *api.File) (*api.File, error) {
	userId := auth.GetUser(ctx)

	var (
		fileDB    models.File
		parent    *models.File
		err       error
		path      string
		channelId int64
	)

	if fileIn.Path.Value == "" && fileIn.ParentId.Value == "" {
		return nil, &apiError{err: errors.New("parent id or path is required"), code: 409}
	}

	if fileIn.Path.Value != "" {
		path = strings.ReplaceAll(fileIn.Path.Value, "//", "/")
		if path != "/" {
			path = strings.TrimSuffix(path, "/")
		}
	}

	if path != "" && fileIn.ParentId.Value == "" {
		parent, err = a.getFileFromPath(path, userId)
		if err != nil {
			return nil, &apiError{err: err, code: 404}
		}
		fileDB.ParentId = utils.Ptr(parent.ID)
	} else if fileIn.ParentId.Value != "" {
		fileDB.ParentId = utils.Ptr(fileIn.ParentId.Value)

	}

	switch fileIn.Type {
	case "folder":
		fileDB.MimeType = "drive/folder"
		fileDB.Parts = nil
	case "file":
		if fileIn.ChannelId.Value == 0 {
			channelId, err = a.channelManager.CurrentChannel(userId)
			if err != nil {
				return nil, &apiError{err: err}
			}
		} else {
			channelId = fileIn.ChannelId.Value
		}
		fileDB.ChannelId = &channelId
		fileDB.MimeType = fileIn.MimeType.Value
		fileDB.Category = string(category.GetCategory(fileIn.Name))
		if len(fileIn.Parts) > 0 {
			fileDB.Parts = datatypes.NewJSONSlice(mapParts(fileIn.Parts))
		}
		fileDB.Size = utils.Ptr(fileIn.Size.Value)
	}
	fileDB.Name = fileIn.Name
	fileDB.Type = string(fileIn.Type)
	fileDB.UserId = userId
	fileDB.Status = "active"
	fileDB.Encrypted = utils.Ptr(fileIn.Encrypted.Value)
	if fileIn.UpdatedAt.IsSet() && !fileIn.UpdatedAt.Value.IsZero() {
		fileDB.UpdatedAt = fileIn.UpdatedAt.Value
	} else {
		fileDB.UpdatedAt = time.Now().UTC()
	}

	//For some reason, gorm conflict clauses are not working with partial index so using raw query

	if err := a.db.Raw(`
    INSERT INTO teldrive.files (
        name, parent_id, user_id, mime_type, category, parts,
        size, type, encrypted, updated_at, channel_id, status
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (name, COALESCE(parent_id, '00000000-0000-0000-0000-000000000000'::uuid), user_id)
    WHERE status = 'active'
    DO UPDATE SET
        mime_type = EXCLUDED.mime_type,
        category = EXCLUDED.category,
        parts = EXCLUDED.parts,
        size = EXCLUDED.size,
        type = EXCLUDED.type,
        encrypted = EXCLUDED.encrypted,
        updated_at = EXCLUDED.updated_at,
        channel_id = EXCLUDED.channel_id,
        status = EXCLUDED.status
    RETURNING *
`,
		fileDB.Name, fileDB.ParentId, fileDB.UserId, fileDB.MimeType,
		fileDB.Category, fileDB.Parts, fileDB.Size, fileDB.Type,
		fileDB.Encrypted, fileDB.UpdatedAt, fileDB.ChannelId, fileDB.Status,
	).Scan(&fileDB).Error; err != nil {
		return nil, &apiError{err: err}
	}
	// POST also replaces an existing active file through the upsert above.
	// Its ID stays the same, so both metadata and resolved message parts must
	// expire after the write succeeds. Locations are keyed by part ID and do
	// not need a broad eviction when replacement uploads use new messages.
	a.cache.Delete(cache.Key("files", fileDB.ID), cache.Key("files", "messages", fileDB.ID))
	a.events.Record(events.OpCreate, userId, &models.Source{
		ID:       fileDB.ID,
		Type:     fileDB.Type,
		Name:     fileDB.Name,
		ParentID: parentIDOuVide(fileDB.ParentId),
	})
	return mapper.ToFileOut(fileDB), nil
}

// parentIDOuVide rend l'identifiant du parent, ou une chaine vide pour la racine.
// parent_id EST nullable et vaut NULL pour exactement une ligne : le dossier
// racine. Sans cette garde, toute operation visant la racine faisait paniquer le
// serveur sur le deref d'un pointeur nil, dans quatre enregistrements d'evenement.
func parentIDOuVide(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func (a *apiService) FilesCreateShare(ctx context.Context, req *api.FileShareCreate, params api.FilesCreateShareParams) error {
	userId := auth.GetUser(ctx)

	var fileShare models.FileShare

	if req.Password.Value != "" {
		bytes, err := bcrypt.GenerateFromPassword([]byte(req.Password.Value), bcrypt.MinCost)
		if err != nil {
			return &apiError{err: err}
		}
		fileShare.Password = utils.Ptr(string(bytes))
	}

	fileShare.FileId = params.ID
	if req.ExpiresAt.IsSet() {
		fileShare.ExpiresAt = utils.Ptr(req.ExpiresAt.Value)
	}
	fileShare.UserId = userId

	if err := a.db.Create(&fileShare).Error; err != nil {
		return &apiError{err: err}
	}

	return nil
}

func (a *apiService) FilesDelete(ctx context.Context, req *api.FileDelete) error {
	userId := auth.GetUser(ctx)

	if len(req.Ids) == 0 {
		return &apiError{err: errors.New("ids should not be empty"), code: 409}
	}

	var fileDB models.File

	if err := a.db.Model(&models.File{}).Where("id = ?", req.Ids[0]).Where("user_id = ?", userId).
		First(&fileDB).Error; err != nil {
		return &apiError{err: err}
	}

	if err := a.db.Exec("call teldrive.delete_files_bulk($1 , $2)", req.Ids, userId).Error; err != nil {
		return &apiError{err: err}
	}

	// Le cache "files/<id>" est pose avec une expiration de 0, ce qui vaut
	// PERMANENT chez freecache. Sans purge explicite, FilesStream continuerait a
	// servir les metadonnees d'un fichier qui n'existe plus. FilesUpdate le fait
	// deja ; ni Delete ni Move ne le faisaient.
	for _, id := range req.Ids {
		a.cache.Delete(cache.Key("files", id))
	}

	a.events.Record(events.OpDelete, userId, &models.Source{
		ID:       fileDB.ID,
		Type:     fileDB.Type,
		Name:     fileDB.Name,
		ParentID: parentIDOuVide(fileDB.ParentId),
	})

	return nil
}

func (a *apiService) FilesDeleteShare(ctx context.Context, params api.FilesDeleteShareParams) error {
	userId := auth.GetUser(ctx)

	var deletedShare models.FileShare

	if err := a.db.Clauses(clause.Returning{}).Where("file_id = ?", params.ID).Where("user_id = ?", userId).
		Delete(&deletedShare).Error; err != nil {
		return &apiError{err: err}
	}
	if deletedShare.ID != "" {
		a.cache.Delete(cache.Key("shared", deletedShare.ID))
	}

	return nil
}

func (a *apiService) FilesEditShare(ctx context.Context, req *api.FileShareCreate, params api.FilesEditShareParams) error {
	userId := auth.GetUser(ctx)

	var fileShareUpdate models.FileShare

	if req.Password.Value != "" {
		bytes, err := bcrypt.GenerateFromPassword([]byte(req.Password.Value), bcrypt.MinCost)
		if err != nil {
			return &apiError{err: err}
		}
		fileShareUpdate.Password = utils.Ptr(string(bytes))
	}
	if req.ExpiresAt.IsSet() {
		fileShareUpdate.ExpiresAt = utils.Ptr(req.ExpiresAt.Value)
	}

	if err := a.db.Model(&models.FileShare{}).Where("file_id = ?", params.ID).Where("user_id = ?", userId).
		Updates(fileShareUpdate).Error; err != nil {
		return &apiError{err: err}
	}

	return nil
}

func (a *apiService) FilesGetById(ctx context.Context, params api.FilesGetByIdParams) (*api.File, error) {
	var result []fullFileDB
	if err := a.db.Model(&models.File{}).Select("*",
		"(select get_path_from_file_id as path from teldrive.get_path_from_file_id(id))").
		Where("id = ?", params.ID).Scan(&result).Error; err != nil {
		return nil, &apiError{err: err}
	}
	if len(result) == 0 {
		return nil, &apiError{err: errors.New("file not found"), code: 404}
	}
	res := mapper.ToFileOut(result[0].File)
	res.Path = api.NewOptString(result[0].Path)
	if result[0].ChannelId != nil {
		res.ChannelId = api.NewOptInt64(*result[0].ChannelId)
	}

	return res, nil
}

func (a *apiService) FilesList(ctx context.Context, params api.FilesListParams) (*api.FileList, error) {
	userId := auth.GetUser(ctx)

	queryBuilder := &fileQueryBuilder{db: a.db}

	return queryBuilder.execute(&params, userId)
}

func (a *apiService) FilesMkdir(ctx context.Context, req *api.FileMkDir) error {
	userId := auth.GetUser(ctx)

	if err := a.db.Exec("select * from teldrive.create_directories(?, ?)", userId, req.Path).Error; err != nil {
		return &apiError{err: err}
	}
	return nil
}

func (a *apiService) FilesMove(ctx context.Context, req *api.FileMove) error {
	userId := auth.GetUser(ctx)

	if !isUUID(req.DestinationParent) {
		r, err := a.getFileFromPath(req.DestinationParent, userId)
		if err != nil {
			return &apiError{err: err}
		}
		req.DestinationParent = r.ID
	}

	err := a.db.Transaction(func(tx *gorm.DB) error {
		var srcFile models.File
		if err := tx.Where("id = ? AND user_id = ?", req.Ids[0], userId).First(&srcFile).Error; err != nil {
			return err
		}
		if len(req.Ids) == 1 && req.DestinationName.Value != "" {
			var existing models.File
			if err := tx.Where("name = ? AND parent_id = ? AND user_id = ? AND status = 'active'",
				req.DestinationName.Value, req.DestinationParent, userId).First(&existing).Error; err == nil {
				if srcFile.Type == "folder" && existing.Type == "folder" {
					if err := tx.Model(&models.File{}).
						Where("parent_id = ? AND status = 'active'", existing.ID).
						Where("name NOT IN (?)",
							tx.Model(&models.File{}).
								Select("name").
								Where("parent_id = ? AND status = 'active'", srcFile.ID),
						).
						Update("parent_id", srcFile.ID).Error; err != nil {
						return err
					}
				}
				if err := tx.Exec("call teldrive.delete_files_bulk($1 , $2)", []string{existing.ID}, userId).Error; err != nil {
					return err
				}
			}
			return tx.Model(&models.File{}).
				Where("id = ? AND user_id = ?", req.Ids[0], userId).
				Updates(map[string]any{
					"parent_id": req.DestinationParent,
					"name":      req.DestinationName.Value,
				}).Error
		}
		items := pgtype.Array[string]{
			Elements: req.Ids,
			Valid:    true,
			Dims:     []pgtype.ArrayDimension{{Length: int32(len(req.Ids)), LowerBound: 1}},
		}
		// tx et non a.db : sinon l'UPDATE de masse s'execute HORS de la
		// transaction et n'est pas annule si l'enregistrement d'evenement echoue.
		if err := tx.Model(&models.File{}).Where("id = any(?)", items).Where("user_id = ?", userId).
			Update("parent_id", req.DestinationParent).Error; err != nil {
			return err
		}
		// meme raison que dans FilesDelete : apres un deplacement ou un renommage,
		// le Content-Disposition servi par FilesStream resterait l'ancien.
		for _, id := range req.Ids {
			a.cache.Delete(cache.Key("files", id))
		}

		a.events.Record(events.OpMove, userId, &models.Source{
			ID:           req.DestinationParent,
			Type:         srcFile.Type,
			Name:         srcFile.Name,
			ParentID:     parentIDOuVide(srcFile.ParentId),
			DestParentID: req.DestinationParent,
		})
		return nil

	})
	if err != nil {
		return &apiError{err: err}
	}
	return nil

}

func (a *apiService) FilesShareByid(ctx context.Context, params api.FilesShareByidParams) (*api.FileShare, error) {
	userId := auth.GetUser(ctx)
	var result []models.FileShare

	notFoundErr := &apiError{err: errors.New("invalid share"), code: 404}
	if err := a.db.Model(&models.FileShare{}).Where("file_id = ?", params.ID).Where("user_id = ?", userId).
		Find(&result).Error; err != nil {
		if database.IsRecordNotFoundErr(err) {
			return nil, notFoundErr
		}
		return nil, &apiError{err: err}
	}

	if len(result) == 0 {
		return nil, notFoundErr
	}
	res := &api.FileShare{
		ID: result[0].ID,
	}
	if result[0].Password != nil {
		res.Protected = true
	}
	if result[0].ExpiresAt != nil {
		res.ExpiresAt = api.NewOptDateTime(*result[0].ExpiresAt)
	}
	return res, nil
}

func (a *apiService) FilesStream(ctx context.Context, params api.FilesStreamParams) (api.FilesStreamRes, error) {
	return nil, nil
}

func (a *apiService) FilesUpdate(ctx context.Context, req *api.FileUpdate, params api.FilesUpdateParams) (*api.File, error) {

	userId := auth.GetUser(ctx)

	updateDb := models.File{}
	if req.Name.Value != "" {
		updateDb.Name = req.Name.Value
	}
	if len(req.Parts) > 0 {
		updateDb.Parts = datatypes.NewJSONSlice(mapParts(req.Parts))
	}
	if req.Size.Value != 0 {
		updateDb.Size = utils.Ptr(req.Size.Value)
	}

	updateDb.UpdatedAt = req.UpdatedAt.Value

	if req.UpdatedAt.Value.IsZero() {
		updateDb.UpdatedAt = time.Now().UTC()
	}

	if err := a.db.Model(&models.File{}).Where("id = ?", params.ID).Updates(updateDb).Error; err != nil {
		return nil, &apiError{err: err}
	}

	a.cache.Delete(cache.Key("files", params.ID))

	file := models.File{}
	if err := a.db.Where("id = ?", params.ID).First(&file).Error; err != nil {
		return nil, &apiError{err: err}
	}

	a.events.Record(events.OpUpdate, userId, &models.Source{
		ID:       file.ID,
		Type:     file.Type,
		Name:     file.Name,
		ParentID: parentIDOuVide(file.ParentId),
	})
	return mapper.ToFileOut(file), nil
}

func (a *apiService) FilesUpdateParts(ctx context.Context, req *api.FilePartsUpdate, params api.FilesUpdatePartsParams) error {

	userId := auth.GetUser(ctx)

	var file models.File

	updatePayload := models.File{
		Size: utils.Ptr(req.Size),
	}
	if req.ChannelId.Value == 0 {
		channelId, err := a.channelManager.CurrentChannel(userId)
		if err != nil {
			return &apiError{err: err}
		}
		updatePayload.ChannelId = &channelId
	} else {
		updatePayload.ChannelId = &req.ChannelId.Value
	}
	if len(req.Parts) > 0 {
		updatePayload.Parts = datatypes.NewJSONSlice(mapParts(req.Parts))
	}
	if req.Name.Value != "" {
		updatePayload.Name = req.Name.Value
	}
	if req.ParentId.Value != "" {
		updatePayload.ParentId = utils.Ptr(req.ParentId.Value)
	}

	updatePayload.UpdatedAt = req.UpdatedAt
	updatePayload.Encrypted = utils.Ptr(req.Encrypted.Value)

	err := a.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("id = ?", params.ID).First(&file).Error; err != nil {
			return err
		}
		if err := tx.Model(models.File{}).Where("id = ?", params.ID).Updates(updatePayload).Error; err != nil {
			return err
		}
		if req.UploadId.Value != "" {
			if err := tx.Where("upload_id = ?", req.UploadId.Value).Delete(&models.Upload{}).Error; err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return &apiError{err: err}
	}

	keys := []string{cache.Key("files", params.ID)}
	if len(file.Parts) > 0 && file.ChannelId != nil {
		ids := utils.Map(file.Parts, func(part api.Part) int { return part.ID })
		client, _ := tgc.AuthClient(ctx, &a.cnf.TG, auth.GetJWTUser(ctx).TgSession, a.middlewares...)
		tgc.DeleteMessages(ctx, client, *file.ChannelId, ids)
		keys = append(keys, cache.Key("files", "messages", params.ID))
		for _, part := range file.Parts {
			keys = append(keys, cache.Key("files", "location", params.ID, part.ID))
		}

	}
	a.cache.Delete(keys...)

	return nil
}

// deleteOrphanFile marks a file whose Telegram messages are gone as
// pending_deletion (using the file owner's user_id, not the requester's)
// and drops every cache entry so it stops being served immediately.
func (e *extendedService) deleteOrphanFile(file *models.File, fileId string, logger *zap.Logger) {
	if err := e.api.db.Exec("call teldrive.delete_files_bulk($1, $2)", []string{fileId}, file.UserId).Error; err != nil {
		logger.Error("failed to delete orphan file from database",
			zap.String("fileId", fileId),
			zap.Error(err))
	}
	keys := []string{cache.Key("files", fileId), cache.Key("files", "messages", fileId)}
	for _, part := range file.Parts {
		keys = append(keys, cache.Key("files", "location", fileId, part.ID))
	}
	e.api.cache.Delete(keys...)
}

func (e *extendedService) FilesStream(w http.ResponseWriter, r *http.Request, fileId string, userId int64) {
	ctx := r.Context()
	var (
		session *models.Session
		err     error
		user    *types.JWTClaims
	)
	if userId == 0 {

		authHash := r.URL.Query().Get("hash")
		if authHash == "" {
			cookie, err := r.Cookie(authCookieName)
			if err != nil {
				http.Error(w, "missing token or authash", http.StatusUnauthorized)
				return
			}
			user, err = auth.VerifyUser(e.api.db, e.api.cache, e.api.cnf.JWT.Secret, cookie.Value)
			if err != nil {
				http.Error(w, "invalid token", http.StatusUnauthorized)
			}
			userId, _ = strconv.ParseInt(user.Subject, 10, 64)
			session = &models.Session{UserId: userId, Session: user.TgSession}
		} else {
			session, err = auth.GetSessionByHash(e.api.db, e.api.cache, authHash)
			if err != nil {
				http.Error(w, "invalid hash", http.StatusBadRequest)
				return
			}
			userId = session.UserId
		}
	} else {
		session = &models.Session{UserId: userId}
	}

	file, err := cache.Fetch(e.api.cache, cache.Key("files", fileId), 0, func() (*models.File, error) {
		var result models.File
		if err := e.api.db.Model(&result).Where("id = ?", fileId).First(&result).Error; err != nil {
			return nil, err
		}
		return &result, nil
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// A part with message id 0 can never exist on Telegram: streaming it is
	// guaranteed to fail with MESSAGE_IDS_EMPTY. Reject before any header is
	// written so the client receives a real 410 instead of a truncated 206.
	for _, part := range file.Parts {
		if part.ID == 0 {
			l := logging.FromContext(ctx)
			l.Warn("file has invalid telegram message id 0, deleting from database",
				zap.String("fileId", fileId),
				zap.String("name", file.Name))
			e.deleteOrphanFile(file, fileId, l)
			http.Error(w, "File no longer exists on Telegram", http.StatusGone)
			return
		}
	}

	w.Header().Set("Accept-Ranges", "bytes")

	var start, end int64

	rangeHeader := r.Header.Get("Range")
	contentType := defaultContentType

	if file.MimeType != "" {
		contentType = file.MimeType
	}

	if file.Size == nil || *file.Size == 0 {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Length", "0")
		w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": file.Name}))
		w.WriteHeader(http.StatusOK)
		return
	}

	status := http.StatusOK
	if rangeHeader == "" {
		start = 0
		end = *file.Size - 1
	} else {
		ranges, err := http_range.Parse(rangeHeader, *file.Size)
		if err == http_range.ErrNoOverlap {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", *file.Size))
			http.Error(w, http_range.ErrNoOverlap.Error(), http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(ranges) > 1 {
			http.Error(w, "multiple ranges are not supported", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start = ranges[0].Start
		end = ranges[0].End
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, *file.Size))
		status = http.StatusPartialContent

	}

	contentLength := end - start + 1

	w.Header().Set("Content-Type", contentType)

	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", md5.FromString(fileId+strconv.FormatInt(*file.Size, 10))))
	w.Header().Set("Last-Modified", file.UpdatedAt.UTC().Format(http.TimeFormat))

	disposition := "inline"

	download := r.URL.Query().Get("download") == "1"

	if download {
		disposition = "attachment"
	}

	w.Header().Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": file.Name}))

	w.WriteHeader(status)

	if r.Method == "HEAD" {
		return
	}

	tokenProxies, err := e.api.channelManager.BotTokensWithProxies(session.UserId)

	if err != nil {
		http.Error(w, "failed to get bots", http.StatusInternalServerError)
		return
	}

	// Extract tokens and build proxy map
	tokens := make([]string, len(tokenProxies))
	proxyMap := make(map[string]string)
	for i, tp := range tokenProxies {
		tokens[i] = tp.Token
		proxyMap[tp.Token] = tp.ProxyUrl
	}

	var (
		lr           io.ReadCloser
		client       *telegram.Client
		multiThreads int
		token        string
		// clientIsLive : le client vient du cache de clients persistants,
		// il est DEJA connecte et autorise -- il ne faut donc pas
		// l'enrober dans RunWithAuth, qui le deconnecterait a la fin.
		clientIsLive bool
	)

	logger := logging.FromContext(ctx)
	multiThreads = e.api.cnf.TG.Stream.MultiThreads
	middlewares := tgc.NewMiddleware(&e.api.cnf.TG, tgc.WithFloodWait(),
		tgc.WithRecovery(),
		tgc.WithRetry(5),
		tgc.WithRateLimit())
	if e.api.cnf.TG.DisableStreamBots || len(tokens) == 0 {
		client, err = tgc.AuthClient(ctx, &e.api.cnf.TG, session.Session, middlewares...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		multiThreads = 0

	} else {
		e.api.worker.SetWithProxies(tokens, proxyMap, session.UserId)

		// Filter out blocked bots
		availableTokens := tgc.GetAvailableBots(tokens)
		if len(availableTokens) == 0 {
			logger.Warn("all bots are blocked, using user session as fallback", zap.String("fileId", fileId))
			client, err = tgc.AuthClient(ctx, &e.api.cnf.TG, session.Session, middlewares...)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			multiThreads = 0
		} else {
			// Try bots until one works
			var lastErr error
			for i := 0; i < len(availableTokens); i++ {
				token, _ = e.api.worker.Next(session.UserId)
				botId := strings.Split(token, ":")[0]

				// Skip if this specific bot is blocked
				if tgc.IsBotBlocked(botId) {
					logger.Debug("skipping blocked bot", zap.String("bot", botId), zap.String("fileId", fileId))
					continue
				}

				proxyUrl := e.api.worker.GetProxy(token)
				if proxyUrl != "" {
					logger.Debug("using proxy for streaming", zap.String("bot", botId), zap.String("proxy", proxyUrl), zap.String("fileId", fileId))
				}

				// Add AuthRecovery middleware to handle AUTH_KEY_UNREGISTERED errors
				sessionKey := cache.Key("sessions", e.api.cnf.TG.SessionInstance, botId)
				streamMiddlewares := append(middlewares, tgc.NewAuthRecovery(e.api.db, e.api.cache, sessionKey))

				// Cache de clients MTProto persistants ([tg.stream] client-cache).
				// On evite ainsi une poignee de main complete par requete HTTP.
				// En cas d'echec on retombe silencieusement sur le chemin
				// historique : le cache ne doit jamais empecher une lecture.
				if e.api.cnf.TG.Stream.ClientCache {
					liveClient, liveErr := tgc.GetLiveClient(ctx, e.api.db, e.api.cache, &e.api.cnf.TG, token, proxyUrl)
					if liveErr == nil {
						client = liveClient
						clientIsLive = true
						lastErr = nil
						break
					}
					logger.Debug("client cache indisponible, repli sur un client par requete",
						zap.String("bot", botId), zap.Error(liveErr))
				}

				client, err = tgc.BotClient(ctx, e.api.db, e.api.cache, &e.api.cnf.TG, token, proxyUrl, streamMiddlewares...)
				if err != nil {
					// Check if it's a proxy error - retry without proxy first
					if proxyUrl != "" && isProxyError(err) {
						logger.Warn("proxy connection failed for streaming, retrying without proxy",
							zap.String("bot", botId),
							zap.String("proxy", proxyUrl),
							zap.String("fileId", fileId),
							zap.Error(err))
						client, err = tgc.BotClient(ctx, e.api.db, e.api.cache, &e.api.cnf.TG, token, "", streamMiddlewares...)
					}

					// If still error, check if AUTH_KEY_UNREGISTERED
					if err != nil {
						if tgc.IsAuthKeyUnregistered(err) {
							logger.Warn("bot AUTH_KEY_UNREGISTERED, blocking and trying next bot",
								zap.String("bot", botId),
								zap.String("fileId", fileId),
								zap.Error(err))
							tgc.BlockBot(botId, 5*time.Minute)
							lastErr = err
							continue // Try next bot
						}
						lastErr = err
						continue // Try next bot for other errors too
					}
				}

				// Success - we have a working client
				lastErr = nil
				break
			}

			// If all bots failed, fallback to user session
			if !clientIsLive && (client == nil || lastErr != nil) {
				logger.Warn("all bots failed, falling back to user session",
					zap.String("fileId", fileId),
					zap.Error(lastErr))
				client, err = tgc.AuthClient(ctx, &e.api.cnf.TG, session.Session, middlewares...)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				multiThreads = 0
				token = "" // Clear token since we're using user session
			}
		}
	}
	if download {
		multiThreads = 0
	}

	if r.Method != "HEAD" {
		// Create streaming function with retry support
		streamWithRetry := func(streamClient *telegram.Client, streamToken string) error {
			handleStream := func() error {
				// Add bot ID to context for MTProto logging
				streamCtx := ctx
				if streamToken != "" {
					botId := strings.Split(streamToken, ":")[0]
					streamCtx = tgc.WithBotID(ctx, botId)
				} else {
					streamCtx = tgc.WithBotID(ctx, "user_session")
				}

				parts, err := getParts(streamCtx, streamClient, e.api.cache, file)
				if err != nil {
					return err
				}
				// Pool de connexions en LECTURE (drapeau [tg.stream] pool-size).
				// L'envoi ouvre deja PoolSize connexions ; la lecture n'en
				// ouvrait qu'une, partagee par les N goroutines de
				// fillBatch -- or MTProto serialise sur une connexion.
				// A 0, comportement inchange. Le pool est ferme ici meme :
				// lr est entierement consomme par le io.CopyN ci-dessous,
				// dans cette meme closure.
				streamAPI := streamClient.API()
				if e.api.cnf.TG.Stream.PoolSize > 0 {
					readPool := pool.NewPool(streamClient, int64(e.api.cnf.TG.Stream.PoolSize), middlewares...)
					defer readPool.Close()
					streamAPI = readPool.Default(streamCtx)
				}

				lr, err = reader.NewLinearReader(streamCtx, streamAPI, e.api.cache, file, parts, start, end, &e.api.cnf.TG, multiThreads)
				if err != nil {
					return err
				}
				if lr == nil {
					return errors.New("failed to initialise reader")
				}

				_, err = io.CopyN(w, lr, contentLength)
				if err != nil {
					lr.Close()
					return err
				}
				return nil
			}
			// Un client issu du cache tourne deja dans sa propre goroutine :
			// le passer a RunWithAuth ouvrirait une SECONDE connexion puis
			// la fermerait en sortie, ce qui annulerait tout le benefice.
			if clientIsLive {
				return handleStream()
			}
			return tgc.RunWithAuth(ctx, streamClient, streamToken, func(ctx context.Context) error {
				return handleStream()
			})
		}

		// Try streaming with current client
		streamErr := streamWithRetry(client, token)

		// If MESSAGE_IDS_EMPTY, the file no longer exists on Telegram - delete from DB and return 410 Gone
		if streamErr != nil && tgc.IsMessageIdsEmpty(streamErr) {
			logger.Warn("file no longer exists on Telegram (MESSAGE_IDS_EMPTY), deleting from database",
				zap.String("fileId", fileId),
				zap.Error(streamErr))
			e.deleteOrphanFile(file, fileId, logger)
			http.Error(w, "File no longer exists on Telegram", http.StatusGone)
			return
		}

		// If getParts found fewer real Telegram messages than the DB expects,
		// the file is orphaned the same way MESSAGE_IDS_EMPTY files are - clean
		// it up instead of leaving the client to loop on a stream that starts
		// (headers already sent above) but never delivers any data.
		if streamErr != nil && tgc.IsFilePartsMismatch(streamErr) {
			logger.Warn("file has parts mismatch (Telegram messages missing), deleting from database",
				zap.String("fileId", fileId),
				zap.Error(streamErr))
			e.deleteOrphanFile(file, fileId, logger)
			http.Error(w, "File no longer exists on Telegram", http.StatusGone)
			return
		}

		// If AUTH_KEY_UNREGISTERED during streaming, retry with other bots
		if streamErr != nil && tgc.IsAuthKeyUnregistered(streamErr) && token != "" {
			botId := strings.Split(token, ":")[0]
			logger.Warn("AUTH_KEY_UNREGISTERED during streaming, trying other bots",
				zap.String("bot", botId),
				zap.String("fileId", fileId),
				zap.Error(streamErr))
			tgc.BlockBot(botId, 5*time.Minute)

			// Try remaining bots
			availableTokens := tgc.GetAvailableBots(tokens)
			for _, tryToken := range availableTokens {
				tryBotId := strings.Split(tryToken, ":")[0]
				if tgc.IsBotBlocked(tryBotId) {
					continue
				}

				proxyUrl := proxyMap[tryToken]
				sessionKey := cache.Key("sessions", e.api.cnf.TG.SessionInstance, tryBotId)
				streamMiddlewares := append(middlewares, tgc.NewAuthRecovery(e.api.db, e.api.cache, sessionKey))

				retryClient, err := tgc.BotClient(ctx, e.api.db, e.api.cache, &e.api.cnf.TG, tryToken, proxyUrl, streamMiddlewares...)
				if err != nil {
					if tgc.IsAuthKeyUnregistered(err) {
						tgc.BlockBot(tryBotId, 5*time.Minute)
						continue
					}
					continue
				}

				logger.Info("retrying stream with different bot",
					zap.String("bot", tryBotId),
					zap.String("fileId", fileId))

				streamErr = streamWithRetry(retryClient, tryToken)
				if streamErr == nil {
					break // Success!
				}
				if tgc.IsAuthKeyUnregistered(streamErr) {
					tgc.BlockBot(tryBotId, 5*time.Minute)
					continue
				}
				break // Non-auth error, stop retrying
			}

			// Last resort: try user session
			if streamErr != nil && tgc.IsAuthKeyUnregistered(streamErr) {
				logger.Warn("all bots failed during streaming, trying user session",
					zap.String("fileId", fileId))
				userClient, err := tgc.AuthClient(ctx, &e.api.cnf.TG, session.Session, middlewares...)
				if err == nil {
					streamErr = streamWithRetry(userClient, "")
				}
			}
		}

		if streamErr != nil && streamErr != ErrorStreamAbandoned {
			// Don't log client disconnect errors
			if !strings.Contains(streamErr.Error(), "context canceled") &&
				!strings.Contains(streamErr.Error(), "broken pipe") {
				logger.Error("streaming failed",
					zap.String("fileId", fileId),
					zap.Error(streamErr))
			}
		}
	}
}

func (e *extendedService) SharesStream(w http.ResponseWriter, r *http.Request, shareId, fileId string) {
	share, err := e.api.validFileShare(r, shareId)
	if err != nil && errors.Is(err, ErrEmptyAuth) {
		w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	e.FilesStream(w, r, fileId, share.UserId)
}

func mapParts(_parts []api.Part) []api.Part {
	return utils.Map(_parts, func(part api.Part) api.Part {
		p := api.Part{ID: part.ID}
		if part.Salt.Value != "" {
			p.Salt = part.Salt
		}
		return p
	})

}
