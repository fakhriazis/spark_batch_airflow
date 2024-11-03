from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, month, year
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
import argparse

# Argumen untuk koneksi PostgreSQL
parser = argparse.ArgumentParser()
parser.add_argument("--pg-url", help="PostgreSQL JDBC URL")
parser.add_argument("--pg-user", help="PostgreSQL username")
parser.add_argument("--pg-password", help="PostgreSQL password")
args = parser.parse_args()

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("Spark ETL Job with Wide Transformation") \
    .getOrCreate()

# Mendefinisikan schema 
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", DateType(), True),
    StructField("UnitPrice", FloatType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])

# Membaca data dari JSON dengan schema dan mode permissive
source_data = spark.read.json(
    "/data/online-retail-dataset.json",
    schema=schema,
    multiline=True,
    mode='PERMISSIVE'
)

# Cek struktur data
source_data.printSchema()
source_data.show(5)

# Menambah kolom 'TotalPrice' sebagai hasil dari Quantity * UnitPrice
transformed_data = source_data.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

# Wide Transformation 1: Agregasi total penjualan per negara
sales_by_country = transformed_data.groupBy("Country").agg(sum("TotalPrice").alias("TotalSales"))

# Wide Transformation 2: Agregasi total penjualan bulanan
sales_by_month = transformed_data \
    .withColumn("InvoiceMonth", month("InvoiceDate")) \
    .withColumn("InvoiceYear", year("InvoiceDate")) \
    .groupBy("InvoiceYear", "InvoiceMonth") \
    .agg(sum("TotalPrice").alias("MonthlySales"))

# Menyimpan hasil transformasi awal ke PostgreSQL (data per transaksi)
transformed_data.write \
    .format("jdbc") \
    .option("url", args.pg_url) \
    .option("dbtable", "transformed_data") \
    .option("user", args.pg_user) \
    .option("password", args.pg_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Menyimpan hasil agregasi total penjualan per negara ke PostgreSQL
sales_by_country.write \
    .format("jdbc") \
    .option("url", args.pg_url) \
    .option("dbtable", "sales_by_country") \
    .option("user", args.pg_user) \
    .option("password", args.pg_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Menyimpan hasil agregasi penjualan bulanan ke PostgreSQL
sales_by_month.write \
    .format("jdbc") \
    .option("url", args.pg_url) \
    .option("dbtable", "sales_by_month") \
    .option("user", args.pg_user) \
    .option("password", args.pg_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Menutup SparkSession
spark.stop()
