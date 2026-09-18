from pyspark import pipelines as dp

# First, declare the target streaming table that both flows will write into
dp.create_streaming_table("silver_all_orders")

# Flow 1: bring in in-store orders from bronze_orders
@dp.append_flow(target="silver_all_orders", name="in_store_orders_flow")
def in_store_orders_flow():
    return spark.readStream.table("bronze_orders")

# Flow 2: bring in online orders directly from their own raw folder
@dp.append_flow(target="silver_all_orders", name="online_orders_flow")
def online_orders_flow():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/workspace/icecream/raw_data_online/")
    )