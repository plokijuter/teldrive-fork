# What differs in this fork?

This unofficial fork of [tgdrive/teldrive](https://github.com/tgdrive/teldrive)
collects streaming, retry and upload changes for a media-serving workload. It
builds on the original maintainers' work and preserves their authorship and license.

Teldrive handles HTTP requests and Telegram access; the
[companion rclone fork](https://github.com/plokijuter/rclone/tree/fix/streaming-upload-v36)
handles the filesystem mount, VFS seek behavior and upload client. Their fixes
address different parts of the same transfer path.

## Which version is being compared?

| Item | Reference |
| --- | --- |
| Public base | `a64d26d0469c21bff2f49bc188e9c6da7448e2f6`, Teldrive 1.7.1 |
| Modified branch | `fix/streaming-upload-v24` |
| Custom build label | v24; this is not an official Teldrive release number |
| Tested companion | rclone custom v36 |

The differences below include the cumulative custom changes since that base.
This is not a port to current upstream main or v2. It does not establish that
these issues still exist in those branches, or that these patches apply to them
without changes. v24 and v36 label two different programs, not matching protocol versions.

## Before / after and the corresponding fixes

| Area | Before (documented public base) | After (this fork) | Correction / evidence |
| --- | --- | --- | --- |
| Telegram file locations | Repeated resolution of the same file part adds metadata RPCs. | Repairs cached-location use and coalesces overlapping resolutions, while keeping client-specific location handling separate. | [Coalescing tests](internal/reader/location_coalesce_test.go), [cross-client tests](internal/reader/location_cross_client_test.go) |
| Expired file references | A cached Telegram reference can expire during reading. | Refreshes stale references and coordinates overlapping refresh requests. | [Refresh tests](internal/reader/reference_refresh_test.go) |
| Reader pipeline | Batch boundaries can leave available download capacity idle. | Maintains an ordered, bounded window of chunk requests and refills slots as data is delivered. | [Pipeline tests](internal/reader/pipeline_test.go) |
| Persistent clients | Creating a client for each HTTP request repeats connection/authentication work. | Adds an opt-in live-client cache, used by streaming and uploads, with fallback to a per-request client. | [Client-cache tests](internal/tgc/clientcache_test.go) |
| Client recovery lifetime | Recovery attached to one request can outlive that request when a client is reused. | Uses the invocation context for recovery and bounds retry behavior. | [Recovery lifetime tests](internal/recovery/recovery_lifetime_test.go) |
| Telegram timeouts | Retry matching could miss transient errors or lose their underlying cause. | Preserves causes, adds cancelable backoff, and retries `-503 Timeout` only for selected idempotent reads, up to three total attempts within the configured budget. | [Retry tests](internal/retry/retry_test.go) |
| Invalid authentication / flood waits | One unavailable bot can interrupt a transfer. | Adds session recovery and bot selection with temporary blocking and bounded waiting. Telegram-imposed wait times remain relevant. | [Authentication recovery](internal/tgc/auth_recovery.go), [bot selection](internal/tgc/workers.go) |
| Upload replay | Retrying with a different bot needs the original input again. | Stages the incoming part in a temporary file and reopens it for upload attempts; this requires temporary disk space and I/O. | [Upload staging and replay](pkg/services/upload.go) |
| File metadata cache | Replacing, moving or deleting files can leave stale cached metadata. | Invalidates affected entries, including file creation/replacement paths. | [Cache invalidation tests](pkg/services/file_create_cache_test.go) |
| Missing Telegram parts | Inconsistent part metadata can cause repeated stream failures or an index panic. | Adds bounds checks and orphan handling. Some missing-message/parts errors mark the file for deletion and invalidate its caches; this is a metadata-changing policy, not recovery of lost content. | [Part bounds checks](internal/reader/reader.go), [orphan policy](pkg/services/file.go) |
| Library scans | Bursts of short requests can compete with playback. | Adds configurable scan detection and throttling. Classification is heuristic and can delay matching requests. | [Scan middleware](internal/middleware/scan_detector.go) |
| Proxy support | Different bots may need different configured routes. | Adds proxy enablement, a proxy pool, per-bot mappings and a database migration for the bot proxy field. | [Proxy migration](internal/database/migrations/20251214000000_add_proxy_url_to_bots.sql), [configuration](internal/config/config.go) |

Implementation and tests: [reader](internal/reader), [client/recovery](internal/tgc),
[retry middleware](internal/retry), [file service](pkg/services/file.go), and
[upload service](pkg/services/upload.go).

## Added configuration and important defaults

These are code defaults, not the settings of a particular deployment.

| Configuration field | Default | Purpose / tradeoff |
| --- | --- | --- |
| `tg.stream.location-cache` | `true` | Reuses Telegram file locations; can be disabled for comparison. |
| `tg.stream.client-cache` | `false` | Opt-in persistent clients; changes connection lifetime and resource use. Also used on the upload path. |
| `tg.stream.pool-size` | `0` | Optional read connection pool; zero retains the historical shared-connection behavior. |
| `tg.uploads.chunk-delay` | `0s` | Configurable delay between chunk uploads. |
| `scan-detector.enabled` | `true` | Enables scan throttling; default base delay is 200 ms and threshold is 10 requests/second. |
| `tg.proxy-enabled` | `false` | Explicitly enables proxy usage; a proxy pool and per-bot mappings are available. |
| `tg.mtproto-log-file` | empty | Optional protocol diagnostics; no destination is enabled by default. |

The description of `tg.rate` is corrected: it represents an interval in
milliseconds, not a count of requests per minute. Its default remains 100 ms.
Additional connections and smaller intervals do not remove Telegram limits.
See [configuration declarations](internal/config/config.go) for the full settings.

## Measured throughput: before / after

See [BENCHMARKS.md](BENCHMARKS.md) for the measured comparison, individual passes,
limits and interpretation. The throughput table is separate from the correctness
changes above: fixing a failure does not automatically increase steady-state speed.

## What has been validated?

Race-detector tests passed for these packages:

```sh
go test -race ./internal/cache ./internal/reader ./internal/recovery ./internal/retry ./internal/tgc ./pkg/services
```

The build requires generated `internal/api` code. The validation used the generated
API from the tested build. The inherited generator references an external schema,
so running it against a newer schema does not necessarily reproduce that build.

Tests cover location caching/coalescing, cross-client reference handling, ordered
pipelining and cancellation, reference refresh, retry budgets, recovery lifetime,
client-cache policy and file-cache invalidation. Windows checks with the companion
rclone build also verified upload/readback content integrity.

These results do not establish a universal upload/download speed gain, lower ping,
or a fixed media-player seek time. No media compression or transcoding feature is
added by this branch. Performance depends on Telegram, connection reuse, workload,
concurrency and local storage; temporary upload staging still consumes disk I/O.

## Publication and compatibility limits

This publication contains source and tests, without server credentials, production
configuration, startup/watchdog scripts or compiled executables. A standalone
diagnostic program was excluded. Runtime source otherwise matches the tested
custom build, apart from whitespace cleanup.

Three privacy passes checked the publication files, new Git history/metadata and
an independently reconstructed publication bundle. No personal secret was detected
in the published changes. This does not certify that runtime diagnostic logs are
safe to share; logs can contain request and deployment details.

The proxy database migration and orphan-file policy should be reviewed when
adopting this branch. Compatibility with Teldrive v2 and unrelated deployment
configurations has not been established. This is an experimental, reviewable fork,
not an official upstream release.
