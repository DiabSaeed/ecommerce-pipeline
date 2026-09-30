from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
# env
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT_SPARK")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER_SPARK")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD_SPARK")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE_SPARK", "ECOMMERCE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE_SPARK", "COMPUTE_WH")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE_SPARK", "ACCOUNTADMIN")
sf_url = f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
sf_options = {
    "sfURL": sf_url,
    "sfUser": SNOWFLAKE_USER,
    "sfPassword": SNOWFLAKE_PASSWORD,
    "sfDatabase": SNOWFLAKE_DATABASE,
    "sfSchema": SNOWFLAKE_SCHEMA,
    "sfWarehouse": SNOWFLAKE_WAREHOUSE,
    "sfRole": SNOWFLAKE_ROLE
}
KAFKA_BROKER = "kafka:9092"
def orders_to_snowflake(df,epoch_id):
    df.write\
        .format("snowflake")\
            .options(**sf_options)\
                .option("dbtable","RAW_ORDERS")\
                    .mode("append")\
                        .save()
def order_items_to_snowflake(df,epoch_id):
    df.write\
        .format("snowflake")\
            .options(**sf_options)\
                .option("dbtable","RAW_ORDER_ITEMS")\
                    .mode("append")\
                        .save()

spark = SparkSession.builder.appName("Ecommerce_streaming").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True)
])

order_items_schema = StructType([
    StructField("order_item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("total_price", FloatType(), True)
])

orders_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

items_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "order_items") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_orders = orders_df \
    .select(from_json(col("value").cast("string"), orders_schema).alias("data")) \
    .select("data.*")
    
parsed_items = items_df \
    .select(from_json(col("value").cast("string"), order_items_schema).alias("data")) \
    .select("data.*")
    
query_orders = parsed_orders.writeStream \
    .foreachBatch(orders_to_snowflake)\
        .start()

query_items = parsed_items.writeStream \
    .foreachBatch(order_items_to_snowflake)\
        .start()

query_orders.awaitTermination()
query_items.awaitTermination()