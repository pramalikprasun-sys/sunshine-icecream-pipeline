from pyspark import pipelines as dp
from pyspark.sql.functions import sum, count, round as spark_round, lit

@dp.table(
    name="gold_flavor_sales_summary",
    comment="Total revenue and order count per flavor, tagged by branch."
)
def gold_flavor_sales_summary():
    branch = spark.conf.get("branch_name", "unknown_branch")  # <-- reads the parameter

    df = spark.read.table("silver_all_orders")
    return (
        df.groupBy("flavor")
        .agg(
            count("order_id").alias("total_orders"),
            spark_round(sum("price"), 2).alias("total_revenue")
        )
        .withColumn("branch_name", lit(branch))   # <-- tags every row with the branch
        .orderBy("total_revenue", ascending=False)   # <-- fixed: sort by the column we already made
    )