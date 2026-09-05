package cron

import (
	"context"
	"runtime/debug"
	"time"

	gormlock "github.com/go-co-op/gocron-gorm-lock/v2"
	"github.com/go-co-op/gocron/v2"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/tgdrive/teldrive/internal/api"
	"github.com/tgdrive/teldrive/internal/config"
	"github.com/tgdrive/teldrive/internal/logging"
	"github.com/tgdrive/teldrive/internal/tgc"
	"github.com/tgdrive/teldrive/pkg/models"
	"go.uber.org/zap"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type file struct {
	ID    string     `json:"id"`
	Parts []api.Part `json:"parts"`
}

type result struct {
	Files     datatypes.JSONSlice[file]
	ChannelId int64
	UserId    int64
	Session   string
}

type uploadResult struct {
	Parts     datatypes.JSONSlice[int]
	Session   string
	UserId    int64
	ChannelId int64
}

type CronService struct {
	db     *gorm.DB
	cnf    *config.ServerCmdConfig
	logger *zap.SugaredLogger
}

func StartCronJobs(ctx context.Context, db *gorm.DB, cnf *config.ServerCmdConfig) error {

	err := db.AutoMigrate(&gormlock.CronJobLock{})
	if err != nil {
		return err
	}

	locker, err := gormlock.NewGormLocker(db, cnf.CronJobs.LockerInstance,
		gormlock.WithCleanInterval(time.Hour*12))

	if err != nil {
		return err
	}

	scheduler, err := gocron.NewScheduler(gocron.WithLocation(time.UTC),
		gocron.WithDistributedLocker(locker))

	if err != nil {
		return err
	}

	cron := CronService{db: db, cnf: cnf, logger: logging.DefaultLogger().Sugar()}

	// Defence in depth for tgdrive/teldrive#580. Fixing the known nil-client
	// dereference is necessary but not sufficient: gocron runs every job on
	// its own goroutine, and an unrecovered panic there takes the entire
	// process down -- HTTP handlers are covered by chi's Recoverer, cron jobs
	// are covered by nothing. A background cleanup job failing must never be
	// able to kill a media server. Any future panic is logged and the job is
	// simply skipped until its next tick.
	guard := func(name string, fn func(context.Context)) func(context.Context) {
		return func(c context.Context) {
			defer func() {
				if r := recover(); r != nil {
					cron.logger.Errorw("cron job panicked, server kept alive",
						"job", name, "panic", r, "stack", string(debug.Stack()))
				}
			}()
			fn(c)
		}
	}
	guard0 := func(name string, fn func()) func() {
		return func() { guard(name, func(context.Context) { fn() })(ctx) }
	}

	scheduler.NewJob(gocron.DurationJob(cnf.CronJobs.CleanFilesInterval),
		gocron.NewTask(guard("clean_files", cron.cleanFiles), ctx))
	scheduler.NewJob(gocron.DurationJob(cnf.CronJobs.FolderSizeInterval),
		gocron.NewTask(guard0("update_folder_size", cron.updateFolderSize)))
	scheduler.NewJob(gocron.DurationJob(cnf.CronJobs.CleanUploadsInterval),
		gocron.NewTask(guard("clean_uploads", cron.cleanUploads), ctx))
	scheduler.NewJob(gocron.DurationJob(time.Hour*12),
		gocron.NewTask(guard0("clean_old_events", cron.cleanOldEvents)))

	scheduler.Start()
	return nil
}

func (c *CronService) cleanFiles(ctx context.Context) {
	c.logger.Debugf("running clean-files")
	var results []result
	if err := c.db.Table("teldrive.files as f").
		Select("JSONB_AGG(jsonb_build_object('id', f.id, 'parts', f.parts)) as files,f.channel_id,f.user_id,s.session").
		Joins("LEFT JOIN teldrive.users as u ON u.user_id = f.user_id").
		Joins(`LEFT JOIN (
        SELECT user_id, session
        FROM teldrive.sessions
        WHERE created_at = (
            SELECT MAX(created_at)
            FROM teldrive.sessions s2
            WHERE s2.user_id = sessions.user_id
        )
    ) as s ON u.user_id = s.user_id`).
		Where("f.type = ?", "file").
		Where("f.status = ?", "pending_deletion").
		Group("f.channel_id").
		Group("f.user_id").
		Group("s.session").
		Scan(&results).Error; err != nil {
		return
	}

	middlewares := tgc.NewMiddleware(&c.cnf.TG, tgc.WithFloodWait(), tgc.WithRateLimit())

	for _, row := range results {

		// Un LEFT JOIN peut rendre une ligne sans session : la sauter, PAS
		// abandonner la boucle -- sinon plus rien n'est nettoye ensuite.
		if row.Session == "" {
			continue
		}
		ids := []int{}

		fileIds := []string{}

		for _, file := range row.Files {
			fileIds = append(fileIds, file.ID)
			for _, part := range file.Parts {
				ids = append(ids, int(part.ID))
			}

		}

		// Upstream discarded this error. When AuthClient fails, client is
		// nil, DeleteMessages calls RunWithAuth -> (*telegram.Client).Run on
		// a nil receiver, and the resulting panic is NOT recovered: cron jobs
		// run on their own goroutines, outside chi's Recoverer, so it takes
		// the whole server down. Reported upstream as tgdrive/teldrive#580,
		// still open. This job runs hourly whenever anything sits in
		// pending_deletion, which the orphan self-heal path now produces.
		// Skip the row instead of killing the process; another user's rows
		// are unaffected by one bad session.
		client, err := tgc.AuthClient(ctx, &c.cnf.TG, row.Session, middlewares...)
		if err != nil || client == nil {
			c.logger.Errorw("clean_files: cannot build telegram client, skipping row",
				"user", row.UserId, "channel", row.ChannelId, "error", err)
			continue
		}

		if err := tgc.DeleteMessages(ctx, client, row.ChannelId, ids); err != nil {
			// continue et non return : une ligne qui resiste ne doit pas faire
			// abandonner tout le lot jusqu'au tick suivant.
			c.logger.Errorw("failed to delete messages", err)
			continue
		}

		items := pgtype.Array[string]{
			Elements: fileIds,
			Valid:    true,
			Dims:     []pgtype.ArrayDimension{{Length: int32(len(fileIds)), LowerBound: 1}},
		}

		c.db.Where("id = any($1)", items).Delete(&models.File{})

		c.logger.Infow("cleaned files", "user", row.UserId, "channel", row.ChannelId)
	}
}

func (c *CronService) cleanUploads(ctx context.Context) {
	c.logger.Debugf("running clean-uploads")
	var results []uploadResult
	if err := c.db.Table("teldrive.uploads as up").
		Select("JSONB_AGG(up.part_id) as parts,up.channel_id,up.user_id,s.session").
		Joins("LEFT JOIN teldrive.users as u ON u.user_id = up.user_id").
		Joins(`LEFT JOIN (
        SELECT user_id, session
        FROM teldrive.sessions
        WHERE created_at = (
            SELECT MAX(created_at)
            FROM teldrive.sessions s2
            WHERE s2.user_id = sessions.user_id
        )
    ) as s ON u.user_id = s.user_id`).
		Where("up.created_at < ?", time.Now().UTC().Add(-c.cnf.TG.Uploads.Retention)).
		Group("up.channel_id").
		Group("up.user_id").
		Group("s.session").
		Scan(&results).Error; err != nil {
		return
	}

	middlewares := tgc.NewMiddleware(&c.cnf.TG, tgc.WithFloodWait(), tgc.WithRateLimit())
	for _, result := range results {

		if result.Session != "" && len(result.Parts) > 0 {
			// Same nil-client crash as in cleanFiles above (#580).
			client, err := tgc.AuthClient(ctx, &c.cnf.TG, result.Session, middlewares...)
			if err != nil || client == nil {
				c.logger.Errorw("clean_uploads: cannot build telegram client, skipping row",
					"user", result.UserId, "channel", result.ChannelId, "error", err)
				continue
			}

			if err := tgc.DeleteMessages(ctx, client, result.ChannelId, result.Parts); err != nil {
				// idem : ne pas sacrifier le reste du lot.
				c.logger.Errorw("failed to delete messages", err)
				continue
			}
		}
		items := pgtype.Array[int]{
			Elements: result.Parts,
			Valid:    true,
			Dims:     []pgtype.ArrayDimension{{Length: int32(len(result.Parts)), LowerBound: 1}},
		}
		// un seul Delete : le second executait la meme suppression une deuxieme fois.
		c.db.Where("part_id = any(?)", items).Where("channel_id = ?", result.ChannelId).
			Where("user_id = ?", result.UserId).Delete(&models.Upload{})

	}
}

func (c *CronService) updateFolderSize() {
	c.logger.Debugf("running folder-size")
	c.db.Exec("call teldrive.update_size();")
}

func (c *CronService) cleanOldEvents() {
	c.db.Exec("DELETE FROM teldrive.events WHERE created_at < NOW() - INTERVAL '5 days';")
}
