-- Athena queries over the Spotify data warehouse (Glue Catalog database: spotify_db)
-- Tables cataloged by the crawler from s3://spotify-de-810995308424/datawarehouse/tables/
-- Set query result location to s3://spotify-de-810995308424/athena-results/

-- 1) Sanity: row counts
SELECT 'tracks'  AS tbl, COUNT(*) n FROM tracks
UNION ALL SELECT 'artists', COUNT(*) FROM artists
UNION ALL SELECT 'albums',  COUNT(*) FROM albums;

-- 2) Star-join: each track with its artist and album, most popular first
SELECT t.track_name,
       ar.artist_name,
       al.album_name,
       al.release_date,
       t.popularity,
       ROUND(t.duration_ms / 60000.0, 2) AS duration_min
FROM tracks t
JOIN artists ar ON ar.artist_id = t.primary_artist_id
JOIN albums  al ON al.album_id  = t.album_id
ORDER BY t.popularity DESC;

-- 3) Most-followed artists
SELECT artist_name, followers, popularity, genres
FROM artists
ORDER BY followers DESC;

-- 4) Average track popularity by artist
SELECT ar.artist_name,
       COUNT(*)              AS n_tracks,
       ROUND(AVG(t.popularity), 1) AS avg_popularity
FROM tracks t
JOIN artists ar ON ar.artist_id = t.primary_artist_id
GROUP BY ar.artist_name
ORDER BY avg_popularity DESC;

-- 5) Albums by release decade
SELECT (CAST(substr(release_date, 1, 4) AS integer) / 10) * 10 AS decade,
       COUNT(*) AS n_albums
FROM albums
GROUP BY (CAST(substr(release_date, 1, 4) AS integer) / 10) * 10
ORDER BY decade;
