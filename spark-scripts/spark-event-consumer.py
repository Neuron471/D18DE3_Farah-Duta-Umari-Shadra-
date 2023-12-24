# Import modul dan pustaka yang diperlukan
import pyspark
import os
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
from pyspark.sql.functions import from_unixtime, from_json, col, window, sum, expr, lit

# Konfigurasi path dan variabel lingkungan
dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

# Konfigurasi untuk koneksi ke server Spark dan Kafka serta pengaturan Spark Streaming
spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

os.environ["spark.sql.streaming.forceDeleteTempCheckpointLocation"] = "true"

# Membuat sesi Spark
sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession.builder.master(spark_host).appName("DibimbingStreaming").getOrCreate()

# Definisi skema data yang diharapkan dari topik Kafka
kafka_schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", IntegerType()) \
    .add("furniture", StringType()) \
    .add("color", StringType()) \
    .add("price", IntegerType()) \
    .add("ts", IntegerType())

# Membaca data streaming dari topik Kafka ke dalam DataFrame 'stream_df'
stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# Memproses nilai dari kolom 'value' sebagai JSON sesuai dengan skema Kafka yang telah didefinisikan sebelumnya
parsed_df = stream_df.select(from_json(col("value").cast("string"), kafka_schema).alias("parsed_value")) \
    .select("parsed_value.*")

# Mengubah kolom 'ts' menjadi tipe data Timestamp
parsed_df = parsed_df.withColumn("ts", from_unixtime("ts").cast(TimestampType()))

# Definisi skema hasil akhir dari proses agregasi
result_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("furniture", StringType()),
    StructField("color", StringType()),
    StructField("price", IntegerType()),
    StructField("ts", TimestampType()),
    StructField("window_start", TimestampType()),
    StructField("window_end", TimestampType()),
    StructField("total_amount_sold", LongType())
])

# Inisialisasi DataFrame kosong 'result_df' sesuai dengan skema yang telah ditentukan
result_df = spark.createDataFrame([], result_schema)

# Fungsi untuk memproses setiap batch dari streaming data
def process_batch(df, epoch_id):
    global result_df
    
    # Menampilkan hasil batch yang diproses
    print(f"Results for Batch {epoch_id}:")
    df.show(truncate=False)

    '''Dalam kode yang diberikan, digunakan windowing dengan tipe 'Tumbling Window
    Dalam kasus aplikasi yang ingin menghitung total penjualan per hari seperti pada
    contoh kode yang diberikan, penggunaan tumbling window dapat menjadi pilihan yang
    baik. Karena perhitungan dilakukan untuk setiap jendela waktu diskrit yang tidak
    tumpang tindih (dalam contoh tersebut, per hari), ini memungkinkan untuk analisis
    agregatif yang cukup akurat tanpa membebani sumber daya komputasi secara berlebihan.
    '''
    # Melakukan agregasi data dalam jendela waktu 1 hari
    # Mengatur watermark pada kolom waktu ('ts') dengan toleransi keterlambatan 60 menit
    windowed_df = df \
        .withWatermark("ts", "60 minutes") \
        .withColumn("window", window("ts", "1 day")) \
        .groupBy(window("ts", "1 day", "1 day").alias("window")) \
        .agg(sum("price").alias("total_amount_sold")) \
        .selectExpr("window.start AS window_start", "window.end AS window_end", "total_amount_sold")
    
    # Menampilkan DataFrame hasil agregasi dalam jendela waktu
    print("Windowed DataFrame:")
    windowed_df.show(truncate=False)

    # Menambahkan kolom 'order_id' dengan nilai null ke DataFrame 'windowed_df'
    windowed_df = windowed_df.withColumn("order_id", lit(None).cast(StringType())) \
    .withColumn("customer_id", lit(None).cast(IntegerType())) \
    .withColumn("furniture", lit(None).cast(StringType())) \
    .withColumn("color", lit(None).cast(StringType())) \
    .withColumn("price", lit(None).cast(IntegerType())) \
    .withColumn("ts", lit(None).cast(IntegerType())) \
    .withColumn("ts", from_unixtime("ts").cast(TimestampType()))

    # Menggabungkan DataFrame 'windowed_df' ke dalam 'result_df' untuk akumulasi hasil agregasi
    result_df = result_df.union(windowed_df.select(
        "order_id", "customer_id", "furniture", "color", "price", "ts", 
        "window_start", "window_end", "total_amount_sold"
    ))

    # Menghitung ulang total jumlah penjualan per jendela waktu yang telah diperbarui
    updated_total_amount = result_df \
        .groupBy("window_start", "window_end") \
        .agg(sum("total_amount_sold").alias("updated_total_amount"))
    
    # Menampilkan total jumlah penjualan yang telah diperbarui
    print("Updated Total Amount Per Day:")
    updated_total_amount.show(truncate=False)

# Memulai operasi streaming menggunakan DataFrame 'parsed_df'
foreach_query = parsed_df.writeStream \
    .outputMode("update") \
    .trigger(processingTime="60 seconds") \
    .foreachBatch(process_batch) \
    .start()

# Menunggu hingga proses streaming berakhir
foreach_query.awaitTermination()