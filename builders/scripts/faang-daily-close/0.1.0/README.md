# faang-daily-close

Fetches NYSE daily close prices for FAANG stocks (META, AAPL, AMZN, NFLX, GOOGL) from the [EODHD API](https://eodhd.com).

- **Calendar**: `nyse-daily` — only builds on NYSE trading days
- **Granularity**: daily (`1d`)
- **Start date**: 2025-01-01
- **Output**: up to 5 rows per timestamp, one per ticker (`ticker`, `close`)

## Setup

1. Copy `.env.template` to `.env`:
   ```
   cp .env.template .env
   ```
2. Fill in your EODHD API key:
   ```
   EODHD_API_KEY=your_key_here
   ```
   Get a key from your [EODHD account dashboard](https://eodhd.com/cp/settings).

The per-builder venv (`eodhd` library) is created automatically on server startup.

## Triggering a build

```
POST /build/faang-daily-close/0.1.0?start=2024-01-15&end=2024-01-15
```

Requesting a weekend or NYSE holiday returns `422` (no valid timestamps in range).
