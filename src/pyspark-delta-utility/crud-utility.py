import pyspark.sql.functions as F
from delta.tables import *
from pyspark.sql import Window

def remove_duplicates(df, partition_by, order_by):
    order_by.append("tiebreak")
    window = Window.partitionBy([F.col(x) for x in partition_by]).orderBy(*order_by)
    df = df\
        .withColumn("tiebreak", F.monotonically_increasing_id())\
        .withColumn("rank", F.rank().over(window))\
        .filter(F.col("rank") == 1).drop("rank", "tiebreak")\
        .dropDuplicates()
    return df

def update_delta_by_cdc(spark, delta_path, cdc_path, merge_condition, cdc_partition_condition):
    cdc_df = spark.read.format("parquet").load(cdc_path)
    delta_tbl = DeltaTable.forPath(spark, delta_path)

    cdc_df = remove_duplicates(cdc_df, cdc_partition_condition, ["pro2modified"])

    delete_cdc_df = cdc_df.filter((F.col("__$operation") == F.lit(1))).drop("__$start_lsn", "__$seqval", "__$operation", "__$update_mask")
    insert_cdc_df = cdc_df.filter((F.col("__$operation") == F.lit(2))).drop("__$start_lsn", "__$seqval", "__$operation", "__$update_mask")
    upsert_cdc_df = cdc_df.filter((F.col("__$operation") == F.lit(3)) | (F.col("__$operation") == F.lit(4))).drop("__$start_lsn", "__$seqval", "__$operation", "__$update_mask")

    delta_tbl.alias("delta").merge(upsert_cdc_df.alias("cdc"), merge_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    delta_tbl.alias("delta").merge(insert_cdc_df.alias("cdc"),merge_condition) \
        .whenNotMatchedInsertAll() \
        .execute()

    delta_tbl.alias("delta").merge(delete_cdc_df.alias("cdc"),merge_condition) \
        .whenMatchedDelete() \
        .execute()