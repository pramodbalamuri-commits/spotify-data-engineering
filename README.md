# Spotify Data Engineering

End-to-end data engineering project: extract data from the **Spotify API**,
transform it, and load it into a database for analytics.

> 🚧 Built step by step. This README and structure will grow as the pipeline develops.

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
- [ ] Transform
- [ ] Load to Postgres
- [ ] Analytics / modeling
- [ ] Orchestration
