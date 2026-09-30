import os
import glob
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("CSV_to_Snowflake_Batch_Pipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT_SPARK")
sf_url = f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com" if SNOWFLAKE_ACCOUNT else None

sf_options = {
    "sfURL": sf_url,
    "sfUser": os.getenv("SNOWFLAKE_USER_SPARK"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD_SPARK"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE_SPARK", "ECOMMERCE_DB"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE_SPARK", "COMPUTE_WH"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE_SPARK", "INGESTION")
}


DATA_DIRECTORY = "/opt/spark/jobs/data"

csv_files = glob.glob(f"{DATA_DIRECTORY}/*.csv")

if not csv_files:
    print(f"لم يتم العثور على أي ملفات CSV في المسار: {DATA_DIRECTORY}")
else:
    print(f"تم العثور على {len(csv_files)} ملفات. جاري بدء المعالجة...")


for file_path in csv_files:

    base_name = os.path.basename(file_path)
    table_name = os.path.splitext(base_name)[0].upper() 
    
    print(f"⏳ جاري قراءة ملف: {base_name}...")
    

    df = spark.read.csv(
        file_path, 
        header=True, 
        inferSchema=True
    )
    
    print(f"🚀 جاري رفع البيانات إلى جدول: {table_name} في Snowflake...")
    
    print("========== SNOWFLAKE CONFIG ==========")
    print("ACCOUNT:", os.getenv("SNOWFLAKE_ACCOUNT"))
    print("USER:", os.getenv("SNOWFLAKE_USER"))
    print("DATABASE:", os.getenv("SNOWFLAKE_DATABASE"))
    print("SCHEMA:", os.getenv("SNOWFLAKE_SCHEMA"))
    print("WAREHOUSE:", os.getenv("SNOWFLAKE_WAREHOUSE"))
    print("=======================================")

    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()
        
    print(f"✅ تم رفع جدول {table_name} بنجاح!\n" + "-"*40)

print("🎉 اكتملت عملية الـ Batch بنجاح.")
spark.stop()