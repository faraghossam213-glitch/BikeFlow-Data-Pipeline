import os
import psycopg2
from pyspark.sql import SparkSession, functions as F, types as T

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PG_HOST, PG_DB = os.getenv("POSTGRES_HOST", "postgres"), os.getenv("POSTGRES_DB", "bikeflow")
PG_USER, PG_PASS = os.getenv("POSTGRES_USER", "bikeflow"), os.getenv("POSTGRES_PASSWORD", "change_me_locally")
schema = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("station_id", T.StringType()),
    T.StructField("event_time", T.StringType()), T.StructField("available_bikes", T.IntegerType()),
    T.StructField("docks_available", T.IntegerType()), T.StructField("source", T.StringType()),
    T.StructField("ingest_time", T.StringType()),
])
spark = SparkSession.builder.appName("BikeFlowStructuredStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
          .option("subscribe", "bikeflow.raw").option("startingOffsets", "latest").load())
decoded = source.select(F.col("value").cast("string").alias("raw_json"), F.from_json(F.col("value").cast("string"), schema).alias("e"))
events = decoded.select("raw_json", "e.*").withColumn("event_time", F.to_timestamp("event_time")).withColumn("ingest_time", F.to_timestamp("ingest_time"))
condition = (F.col("event_id").isNotNull() & F.col("station_id").isNotNull() & F.col("event_time").isNotNull() & F.col("available_bikes").isNotNull() & (F.col("available_bikes") >= 0) & F.col("docks_available").isNotNull() & (F.col("docks_available") >= 0))
good = events.filter(condition).drop("raw_json").withWatermark("event_time", "10 minutes").dropDuplicates(["event_id"])
bad = events.filter(~condition).select(F.to_json(F.struct("raw_json", F.lit("required_field_missing_or_out_of_range").alias("reason"))).alias("value"))

def write_parquet(batch, epoch, path, date_column):
    if batch.rdd.isEmpty():
        return
    source_time = "event_time" if date_column == "event_date" else "ingest_time"
    batch.withColumn(date_column, F.to_date(source_time)).write.mode("append").partitionBy(date_column).parquet(path)

def write_aggregates(batch, epoch):
    if batch.rdd.isEmpty():
        return
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        with conn.cursor() as cur:
            for row in batch.collect():
                cur.execute("""INSERT INTO live_aggregates(window_start,window_end,station_id,avg_bikes,min_bikes,max_bikes,event_count,updated_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,now()) ON CONFLICT(window_start,window_end,station_id)
                DO UPDATE SET avg_bikes=EXCLUDED.avg_bikes,min_bikes=EXCLUDED.min_bikes,max_bikes=EXCLUDED.max_bikes,event_count=EXCLUDED.event_count,updated_at=now()""",
                (row.window_start,row.window_end,row.station_id,row.avg_bikes,row.min_bikes,row.max_bikes,row.event_count))
        conn.commit()
    finally:
        conn.close()

def write_alerts(batch, epoch):
    if batch.rdd.isEmpty():
        return
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        with conn.cursor() as cur:
            for row in batch.collect():
                cur.execute("INSERT INTO alerts(alert_id,station_id,event_time,severity,message,available_bikes) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(alert_id) DO NOTHING",
                    (row.alert_id,row.station_id,row.event_time,row.severity,row.message,row.available_bikes))
        conn.commit()
    finally:
        conn.close()
    (batch.select(F.col("station_id").cast("string").alias("key"), F.to_json(F.struct("*")).alias("value"))
     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka")
     .option("kafka.bootstrap.servers", BOOTSTRAP).option("topic", "bikeflow.alerts").save())

bronze = decoded.select("raw_json", F.current_timestamp().alias("ingest_time"))
bronze_q = bronze.writeStream.foreachBatch(lambda b, e: write_parquet(b, e, "/lake/bronze", "ingest_date")) \
    .option("checkpointLocation", "/checkpoints/bronze").start()
silver_q = good.writeStream.foreachBatch(lambda b, e: write_parquet(b, e, "/lake/silver", "event_date")) \
    .option("checkpointLocation", "/checkpoints/silver").start()
windowed = good.groupBy(F.window("event_time", "1 minute", "30 seconds"), "station_id").agg(
    F.avg("available_bikes").alias("avg_bikes"), F.min("available_bikes").alias("min_bikes"),
    F.max("available_bikes").alias("max_bikes"), F.count("*").alias("event_count"))
agg_q = windowed.select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
    "station_id", "avg_bikes", "min_bikes", "max_bikes", "event_count").writeStream.foreachBatch(write_aggregates) \
    .option("checkpointLocation", "/checkpoints/aggregates").start()
alerts = good.filter(F.col("available_bikes") <= 1).select(
    F.sha2(F.concat_ws("|", "event_id", F.lit("low_stock")), 256).alias("alert_id"), "station_id", "event_time",
    F.lit("warning").alias("severity"), F.lit("Station is nearly empty").alias("message"), "available_bikes")
alert_q = alerts.writeStream.foreachBatch(write_alerts).option("checkpointLocation", "/checkpoints/alerts").start()
dlq_q = bad.writeStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP).option("topic", "bikeflow.dlq") \
    .option("checkpointLocation", "/checkpoints/dlq").start()
spark.streams.awaitAnyTermination()
