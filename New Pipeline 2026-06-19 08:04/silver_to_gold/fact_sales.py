from pyspark.sql.functions import col,trim,upper
from pyspark import pipelines as dp


@dp.table(
    name = "retail_q.retail_gold.fact_sales"
)
def fact_sales():

    transaction_df = spark.read.table("retail_q.retail_silver.transactions")
    oppertunity_df = spark.read.table("retail_q.retail_silver.opportunity")


    joined_df = transaction_df.alias("t").join(
        oppertunity_df.alias("o"),
        upper(trim(transaction_df.opportunity_name)) == upper(trim(oppertunity_df.name)),
        how="left"
    )

    selected_df = joined_df.select(
        
        "t.transaction_id",
        "t.opportunity_name",
        "t.product_id",
        "t.store_id",
        "t.quantity",
        "t.selling_price",
        "t.discount_amount",
        "t.transaction_timestamp",
        col("t.transaction_timestamp").cast("date").alias("transaction_date"),
        "t.payment_mode",
        "t.sales_channel",
        "o.name",
        "o.stage_name",
        "o.owner_id",
        "o.amount"
        
    )

    return selected_df
