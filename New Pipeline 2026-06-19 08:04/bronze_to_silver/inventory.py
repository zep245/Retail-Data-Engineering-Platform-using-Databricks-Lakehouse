from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.retail_silver.inventory",
    comment="Cleaned inventory data with quality constraints"
)
@dp.expect_or_drop("valid_inventory_id", "inventory_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dp.expect_or_drop("valid_stock_quantity", "stock_quantity >= 0")
@dp.expect_or_drop("valid_reorder_level", "reorder_level >= 0")
@dp.expect("has_last_update", "last_stock_update IS NOT NULL")
def inventory():
    source_df = spark.readStream.table("retail_q.postgress_bronze.inventory") 

    return (
        source_df.select(
            F.col("inventory_id"),
            F.col("product_id"),
            F.col("store_id"),
            F.col("stock_quantity"),
            F.col("reorder_level"),
            F.when(
                F.col("stock_quantity") < F.col("reorder_level") , "LOW_STOCK"
            ).otherwise("HEALTHY").alias("inventory_status"),
            F.col("warehouse_location"),
            F.col("last_stock_update")
        )
    )
