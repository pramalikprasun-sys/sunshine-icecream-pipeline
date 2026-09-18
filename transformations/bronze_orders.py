from pyspark import pipelines as dp

@dp.table(
    name="bronze_orders",
    comment="Raw ice cream orders, ingested incrementally with Auto Loader."
)
def bronze_orders():
    return (
        spark.readStream                              # <-- streaming read, not spark.read
        .format("cloudFiles")                         # <-- tells Spark to use Auto Loader
        .option("cloudFiles.format", "csv")           # the files underneath are CSVs
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/workspace/icecream/raw_data/")
    )