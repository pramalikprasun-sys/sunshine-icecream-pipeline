from pyspark import pipelines as dp

@dp.table(name="bronze_customers", comment="Raw customer change events.")
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/workspace/icecream/raw_data_customers/")
    )