from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.retail_silver.product_catalog",
    comment="Silver layer product with standardized data and data quality rules"
)
@dp.expect_all_or_drop({
    "valid_product_id": "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0",
    "valid_product_name": "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0",
    "valid_category": "category IS NOT NULL",
    "valid_price": "unit_price > 0",
    "valid_launch_date": "launch_date IS NOT NULL"
})
@dp.expect(
    "valid_supplier", "supplier_name IS NOT NULL"
)
def product_catalog():
    return (
        spark.readStream.table("retail_q.postgress_bronze.product_catalog")
        .filter(F.col("is_active") == True)
        .select(
            F.col("product_id"),
            F.initcap(F.trim(F.col("product_name"))).alias("product_name"),
            F.initcap(F.trim(F.col("category"))).alias("category"),
            F.when(F.col("subcategory").isNotNull(),
                F.initcap(F.trim(F.col("subcategory")))
            ).otherwise(F.lit("Unknown")).alias("subcategory"),
            F.when(F.col("brand").isNotNull(),
                F.initcap(F.trim(F.col("brand")))
            ).otherwise(F.lit("Unknown")).alias("brand"),
            F.round(F.col("unit_price"), 2).alias("unit_price"),
            F.when(F.col("unit_price") > 50000, F.lit("PREMIUM"))
             .when(F.col("unit_price") > 10000, F.lit("MID_RANGE"))
             .otherwise(F.lit("BUDGET")).alias("product_segment"),
            F.initcap(F.trim(F.col("supplier_name"))).alias("supplier_name"),
            F.col("launch_date"),
            F.col("updated_at"),
            F.col("__START_AT").alias("start_at"),
            F.col("__END_AT").alias("end_at"),
            F.when(F.col("__END_AT").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_active"),
            F.current_timestamp().alias("processed_at")
        )
    )