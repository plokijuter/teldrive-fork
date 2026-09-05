# Before / after measurements

Measured on 2026-09-05. The client comparison below does **not demonstrate a general
throughput improvement**. Correctness fixes and throughput are reported separately.

## Controlled comparison of the rclone clients

- **Before (A):** public tgdrive/rclone base `4e101c1c77eb5e4145435c60bd80a8b049db5e08`.
- **After (B):** custom v36 source `9c7a904fdbb7771a3c1219b8475fe4b1028c254d`.
- **Server held constant:** custom Teldrive v24, with unchanged configuration.
- Both benchmark clients were built with Go 1.24.4, Windows/amd64, CGO disabled.
  These are direct-transfer test binaries, not a comparison of mounted filesystems.

| Measurement | Before: median MiB/s (range) | After: median MiB/s (range) | Median change |
| --- | ---: | ---: | ---: |
| Upload, separate 8 MiB/s per-file ceiling | 5.16 (5.12–5.16) | 5.15 (5.14–5.16) | -0.1% |
| Direct download, no bandwidth ceiling | 18.03 (17.74–18.11) | 15.60 (15.38–15.98) | -13.5% |

The fork was slower in this short direct-download series. This is an observed
regression for this workload; its cause has not been isolated. It is not evidence
that the VFS seek fixes make media seeking slower, since no VFS mount was involved.
The limited upload test cannot establish either client's maximum upload capacity.
Three samples per variant are insufficient for a universal performance claim.

## Protocol and individual passes

Each sample transfers 128 MiB. Uploads use one file transfer, two multipart streams
and 100 MiB parts. The elapsed upload time ends when the copy command confirms
success, rather than when the HTTP request has merely accepted the input bytes.
Each uploaded object is read back completely and checked against the source SHA-256.

Downloads read the same remote object, use `threaded_streams=true` for both clients,
and stream to an in-memory hash sink. They bypass the filesystem mount and its VFS
cache. One full warm-up per variant precedes the timed series; server and Telegram
cache effects remain possible. Elapsed time includes process startup, metadata
requests, transfer and command completion. MiB/s means 2^20 bytes per second.

Both comparisons use the order **A, B, B, A, A, B**, three measurements per variant.

| Operation | Pass | Variant | Seconds | MiB/s |
| --- | ---: | --- | ---: | ---: |
| upload | 1 | A | 25.007 | 5.12 |
| upload | 2 | B | 24.912 | 5.14 |
| upload | 3 | B | 24.792 | 5.16 |
| upload | 4 | A | 24.818 | 5.16 |
| upload | 5 | A | 24.798 | 5.16 |
| upload | 6 | B | 24.834 | 5.15 |
| download | 1 | A | 7.216 | 17.74 |
| download | 2 | B | 8.206 | 15.60 |
| download | 3 | B | 8.009 | 15.98 |
| download | 4 | A | 7.100 | 18.03 |
| download | 5 | A | 7.067 | 18.11 |
| download | 6 | B | 8.325 | 15.38 |

[Machine-readable measurements](benchmark-results.json) contain the individual
samples and method, without deployment addresses, credentials or private filenames.

## Storage controls and limits of attribution

The production upload worker was paused under its existing lock for the comparisons.
The server, mount and production configuration were unchanged, and the worker was
restarted afterwards. Existing playback protections remained active. A disk-queue
monitor stopped the test if either benchmark storage volume stayed busy; it did not
trigger during the completed series. Source generation was limited to 2 MiB/s.

A preliminary upload series was discarded from the comparison because an external
controller changed the global bandwidth limit during measurement. The reported
upload series adds `--bwlimit-file=8Mi`, which is independent of changes to the global
RC bandwidth limit. The global controller still operated normally. The initial pilot
with smaller parts was rejected before transfer by the public client's 100 MiB
minimum and is not a speed result.

Other machine/network activity was not completely isolated. The working set is small
and may be cached by the OS or remote service. These checks do not measure sustained
mechanical-disk throughput, maximum Telegram capacity, ping, transcoding, or a fixed
Plex/Jellyfin seek time. No claim about those metrics follows from this table.

The server is v24 in **both** columns: these numbers cannot measure the gain of
Teldrive 1.7.1 versus the Teldrive fork, nor compatibility/performance of v2.

## Separate controlled measurement: Telegram metadata work

The current source also has a deterministic test with eight simultaneous chunk
reads. Re-running it with the race detector produced:

| Scenario | Metadata RPCs for eight chunks |
| --- | ---: |
| Cache-disabled control | 16 |
| Cold location cache with request coalescing | 2 |
| Simultaneously expired references with coordinated refresh | 2 |

That is **87.5% fewer metadata RPCs** than the cache-disabled control in these
scenarios. This is a count of metadata operations, not an 87.5% bandwidth gain.
Data-transfer RPCs are still needed. This controlled test is separate from the
live client comparison above and is not an end-to-end old-server/new-server trial.

The [test and assertions](internal/reader/location_coalesce_test.go) are reproducible:

```sh
go test -race -count=1 -v ./internal/reader -run '^TestConcurrentLocationMetadataRPCs$'
```

Generated API code is required, as described in [the fork notes](FORK_NOTES.md).
