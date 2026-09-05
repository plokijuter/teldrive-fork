# Experimental streaming and upload fixes

This is an unofficial fork of [tgdrive/teldrive](https://github.com/tgdrive/teldrive).
Original authorship and licensing are preserved.

This branch is based on public upstream commit `a64d26d0469c21bff2f49bc188e9c6da7448e2f6`
(version 1.7.1). It collects the source changes used for the custom v24 build. It
is not a port to the latest main or v2 branch, and v24 is a local build label,
not an upstream release.

Changes cover Telegram reader pipelining and file-location caching, reference
refresh and authentication recovery, client lifetime management, retry handling,
and upload buffering. Regression tests are included alongside the affected packages.

The runtime source matches the tested custom build. A standalone diagnostic tool
is omitted from this publication. Validation included local regression tests and
Windows streaming/upload content-integrity checks together with the custom rclone
build. These checks do not establish compatibility with every deployment or with v2.
No server configuration or compiled executable is included.
