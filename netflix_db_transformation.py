from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType

#  Start Spark Session with JDBC jar
spark = SparkSession.builder \
    .appName("NetflixDataCleaning") \
    .config("spark.jars", "file:///C:/Users/giris/Downloads/nifi-1.25.0-bin/nifi-1.25.0/lib/postgresql-42.7.6.jar") \
    .getOrCreate()

print(" Spark session started")

#  Read raw Netflix data from PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/Netflix_db"
jdbc_properties = {
    "user": "postgres",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

try:
    raw_df = spark.read.jdbc(url=jdbc_url, table="netflix_raw", properties=jdbc_properties)
    print(" Raw DataFrame Schema:")
    raw_df.printSchema()
except Exception as e:
    print(" Error reading raw table:", e)
    spark.stop()
    exit(1)

#  Clean data
clean_df = raw_df \
    .withColumn("release_year", col("release_year").cast(IntegerType())) \
    .withColumn("date_added", to_date(col("date_added"), "MMMM d, yyyy"))

print(" Cleaned DataFrame Schema:")
clean_df.printSchema()

#  Write cleaned data back to PostgresSQL
#     No need for createTableColumnTypes — let Spark handle it
try:
    clean_df.write.jdbc(
        url=jdbc_url,
        table="netflix_clean",
        mode="overwrite",   # Overwrites if table already exists
        properties=jdbc_properties
    )
    print(" Cleaned data written to table: netflix_clean")
except Exception as e:
    print(" Error writing cleaned table:", e)

spark.stop()
