-- +goose Up
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'teldrive'
        AND table_name = 'bots'
        AND column_name = 'proxy_url'
    ) THEN
        ALTER TABLE "teldrive"."bots" ADD COLUMN "proxy_url" text;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE "teldrive"."bots" DROP COLUMN "proxy_url";
-- +goose StatementEnd
