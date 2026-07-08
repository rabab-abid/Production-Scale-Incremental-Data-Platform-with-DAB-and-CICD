from pyspark import pipelines as dp
from pyspark.sql.functions import col

# DimUser — SCD Type 2 with expectations
expectations = {"rule1": "user_id IS NOT NULL"}

@dp.table
@dp.expect_all_or_drop(expectations)
def dimuser_stg():
    return spark.readStream.table("musicmeta_dev.silver.DimUser")

dp.create_streaming_table(
    name="dimuser",
    expect_all_or_drop=expectations)

dp.create_auto_cdc_flow(
    target="dimuser", 
    source="dimuser_stg",
    keys=["user_id"], 
    sequence_by=col("updated_at"),
    stored_as_scd_type=2)

# DimTrack — SCD Type 2
@dp.table
def dimtrack_stg():
    return spark.readStream.table("musicmeta_dev.silver.DimTrack")

dp.create_streaming_table(name="dimtrack")

dp.create_auto_cdc_flow(
    target="dimtrack", 
    source="dimtrack_stg",
    keys=["track_id"], 
    sequence_by=col("updated_at"),
    stored_as_scd_type=2)

# DimDate — SCD Type 2
@dp.table
def dimdate_stg():
    return spark.readStream.table("musicmeta_dev.silver.DimDate")

dp.create_streaming_table(name="dimdate")

dp.create_auto_cdc_flow(
    target="dimdate", source="dimdate_stg",
    keys=["date_key"], sequence_by=col("date"),
    stored_as_scd_type=2)

# FactStream — SCD Type 1
@dp.table
def factstream_stg():
    return spark.readStream.table("musicmeta_dev.silver.FactStream")

dp.create_streaming_table(name="factstream")

dp.create_auto_cdc_flow(
    target="factstream", 
    source="factstream_stg",
    keys=["stream_id"], 
    sequence_by=col("stream_timestamp"),
    stored_as_scd_type=1)