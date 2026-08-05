"""
AWS Glue ETL job: Spotify staging -> data warehouse.

Reads raw Spotify JSON from  s3://<bucket>/<staging>/raw/
Flattens into 3 clean tables and writes Parquet to
                             s3://<bucket>/<warehouse>/tables/{tracks,artists,albums}/

Job arguments (passed via --KEY value):
  --JOB_NAME          (set automatically by Glue)
  --S3_BUCKET         e.g. spotify-de-810995308424
  --STAGING_PREFIX    e.g. staging/
  --WAREHOUSE_PREFIX  e.g. datawarehouse/
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col, current_timestamp

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "S3_BUCKET", "STAGING_PREFIX", "WAREHOUSE_PREFIX"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["S3_BUCKET"]
raw = f"s3://{bucket}/{args['STAGING_PREFIX']}raw/"
tables = f"s3://{bucket}/{args['WAREHOUSE_PREFIX']}tables/"

print(f"[spotify-etl] reading from {raw}")
print(f"[spotify-etl] writing to   {tables}")


def write_parquet(df, name):
    out = tables + name + "/"
    count = df.count()
    df.write.mode("overwrite").parquet(out)
    print(f"[spotify-etl] wrote {count} rows -> {out}")


# ---------- TRACKS ----------
tracks_raw = spark.read.option("multiLine", "true").json(raw + "spotify_tracks_*.json")
tracks = (
    tracks_raw.select(explode("items").alias("i"))
    .select(
        col("i.track.id").alias("track_id"),
        col("i.track.name").alias("track_name"),
        col("i.track.duration_ms").cast("int").alias("duration_ms"),
        col("i.track.popularity").cast("int").alias("popularity"),
        col("i.track.explicit").alias("explicit"),
        col("i.track.album_id").alias("album_id"),
        col("i.track.artist_ids").getItem(0).alias("primary_artist_id"),
        col("i.track.added_at").alias("added_at"),
    )
    .withColumn("processed_at", current_timestamp())
)
write_parquet(tracks, "tracks")

# ---------- ARTISTS ----------
artists_raw = spark.read.option("multiLine", "true").json(raw + "spotify_artists_*.json")
artists = (
    artists_raw.select(explode("artists").alias("a"))
    .select(
        col("a.id").alias("artist_id"),
        col("a.name").alias("artist_name"),
        col("a.genres").alias("genres"),
        col("a.popularity").cast("int").alias("popularity"),
        col("a.followers").cast("long").alias("followers"),
    )
    .withColumn("processed_at", current_timestamp())
)
write_parquet(artists, "artists")

# ---------- ALBUMS ----------
albums_raw = spark.read.option("multiLine", "true").json(raw + "spotify_albums_*.json")
albums = (
    albums_raw.select(explode("albums").alias("al"))
    .select(
        col("al.id").alias("album_id"),
        col("al.name").alias("album_name"),
        col("al.release_date").alias("release_date"),
        col("al.total_tracks").cast("int").alias("total_tracks"),
        col("al.label").alias("label"),
        col("al.artist_id").alias("artist_id"),
    )
    .withColumn("processed_at", current_timestamp())
)
write_parquet(albums, "albums")

job.commit()
print("[spotify-etl] done.")
