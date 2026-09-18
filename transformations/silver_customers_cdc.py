from pyspark import pipelines as dp

# Declare the destination table — always up-to-date customer info
dp.create_streaming_table("silver_customers")

dp.create_auto_cdc_flow(
    target="silver_customers",
    source="bronze_customers",
    keys=["customer_id"],                # how to identify "same" customer across events
    sequence_by="changed_at",             # which event is newest, so order is correct
    apply_as_deletes="operation = 'DELETE'",   # treat DELETE rows as actual deletions
    except_column_list=["operation", "changed_at"],  # don't keep these helper columns in final table
    stored_as_scd_type=1                  # Type 1 = only keep the LATEST version (overwrite history)
)