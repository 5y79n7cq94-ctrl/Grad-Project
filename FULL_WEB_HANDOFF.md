# Full-Web Heat Analysis Handoff

This repository now includes a separate `Full-Web Heat Analysis` feature that is
independent from the original official heat leaderboard.

## What stays untouched

- Original `Heat Leaderboard`
- Original `Negative Monitor`
- Original `macau_analytics.db`

## What you need

1. A local copy of this repository
2. A Full-Web analytics database file named `social_media_analytics.db`

## Quick start

```bash
./bootstrap_full_web.sh
```

Then either:

- place the database at `data/social_media_analytics.db`, or
- export `FULL_WEB_ANALYTICS_DB_PATH=/absolute/path/to/social_media_analytics.db`

Then run:

```bash
./run_full_web_sidecar.sh
```

## Routes

- Main system: `http://127.0.0.1:9038/operation_panel.html`
- Full-Web Heat Analysis: `http://127.0.0.1:9038/full-web-heat-analysis`
- Full-Web Trend Analysis: `http://127.0.0.1:9038/full-web-heat-analysis/trends`

## Optional update commands

If you want `Update Database` to trigger platform-specific crawling, configure:

```bash
export FULL_WEB_WB_UPDATE_COMMAND='...'
export FULL_WEB_FB_UPDATE_COMMAND='...'
```

Without these commands:

- viewing existing data works
- `Run Analysis` works on imported data
- `Update Database` will show a clear configuration error instead of silently failing
