# Spotify Data Engineering

End-to-end data engineering project: extract data from the **Spotify API**,
transform it, and load it into a database for analytics.

> 🚧 Built step by step. This README and structure will grow as the pipeline develops.

## 📖 Start here

- **[docs/AWS_PIPELINE_GUIDE.md](./docs/AWS_PIPELINE_GUIDE.md)** — the complete build guide:
  every AWS service explained, exact commands to recreate the whole pipeline from scratch,
  errors we hit + how we fixed them, why the design is good (and alternatives),
  interview Q&A, and what a data engineer does with this in real life.
- **[dashboards/spotify_dashboard.html](./dashboards/spotify_dashboard.html)** — open in a
  browser; analytics dashboard (popularity / followers / genres) built from the Athena output.

## What we built (AWS)

```
Spotify JSON → S3 staging/raw → Glue ETL (PySpark) → S3 datawarehouse (Parquet)
             → Glue Crawler → Glue Data Catalog (spotify_db) → Athena SQL → dashboard
```
IAM user `spotify-project` + Glue service role authorize it. Region `us-west-2`.

## Goal

Move Spotify data (tracks, artists, albums, playlists, listening history) through a
proper **ELT/ETL pipeline** — Extract → Transform → Load — into PostgreSQL, then
model it for analytics.

## Project structure

```
spotify-data-engineering/
├── src/          # pipeline code (extract / transform / load)
├── config/       # configuration (non-secret)
├── sql/          # DDL, models, analytics queries
├── data/         # local data (raw/staging are git-ignored)
├── notebooks/    # exploration
├── tests/        # tests
├── .env.example  # copy to .env and fill in your Spotify + DB credentials
└── requirements.txt
```

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env      # then fill in your real credentials (never committed)
```

Get Spotify API credentials at the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard).

## Architecture (planned)

```
Spotify API  ──extract──▶  raw JSON  ──transform──▶  clean tables  ──load──▶  PostgreSQL  ──▶  analytics
```

## Status

- [x] Repo scaffold
- [ ] Spotify API auth
- [ ] Extract
- [x] Transform (Glue job: staging/raw JSON -> datawarehouse Parquet)
- [x] Load to warehouse (S3 Parquet)
- [x] Catalog (Glue crawler -> spotify_db) + Athena SQL
- [ ] Orchestration
