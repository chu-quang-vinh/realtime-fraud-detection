from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, BooleanType
from pyspark.sql.functions import from_unixtime, to_timestamp
import time

# --- Cấu hình ---
KAFKA_BROKER_URL = "kafka:9093"
KAFKA_TOPIC = "transactions"
ELASTICSEARCH_NODES = "elasticsearch" # Tên service của Elasticsearch trong Docker Compose
ELASTICSEARCH_PORT = "9200"
ELASTICSEARCH_INDEX = "transactions_index" # Tên index trong Elasticsearch
CHECKPOINT_LOCATION_ES = "/tmp/spark-checkpoint-es" # Thư mục checkpoint cho việc ghi vào ES
CHECKPOINT_LOCATION_PG = "/tmp/spark-checkpoint-pg" # Thư mục checkpoint cho PostgreSQL

# --- Schema cho dữ liệu JSON từ Kafka ---
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", LongType(), True), # Giữ là LongType (milliseconds)
    StructField("location", StringType(), True),
    StructField("merchant_id", IntegerType(), True)
])

# --- Hàm xử lý chính ---
def process_transactions():
    print("Khởi tạo Spark Session...")

    # IMPORTANT: Điều chỉnh package versions cho phù hợp với phiên bản Spark và Elasticsearch/Kafka của bạn!
    # Ví dụ cho Spark 3.4, Kafka 2.8+, ES 7.x
    spark = SparkSession \
        .builder \
        .appName("RealtimeFraudDetection") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:7.17.9,"
                "org.postgresql:postgresql:42.5.1") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION_ES) \
        .config("es.nodes", ELASTICSEARCH_NODES) \
        .config("es.port", ELASTICSEARCH_PORT) \
        .config("es.nodes.wan.only", "false") \
        .config("es.index.auto.create", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")  # Để nhìn thấy thông tin chi tiết hơn
    print("Spark Session đã tạo.")

    # Đọc dữ liệu từ Kafka
    print(f"Đọc dữ liệu từ Kafka topic: {KAFKA_TOPIC}")
    kafka_stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON data từ cột 'value' (dạng binary) của Kafka
    transaction_df = kafka_stream_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), transaction_schema).alias("data")) \
        .select("data.*")

    # Thêm cột timestamp chuẩn cho Elasticsearch và Grafana
    transaction_df = transaction_df.withColumn("@timestamp", 
        to_timestamp(from_unixtime(col("timestamp")/1000)))
    
    # Thêm cột datetime cho PostgreSQL
    transaction_df = transaction_df.withColumn("datetime", 
        to_timestamp(from_unixtime(col("timestamp")/1000)))

    # --- Áp dụng Rule-based Detection Đơn Giản ---
    # Ví dụ: Đánh dấu giao dịch có giá trị > 30,000 là nghi ngờ
    RULE_THRESHOLD = 30000.0
    processed_df = transaction_df.withColumn(
        "is_suspicious_rule",
        when(col("amount") > RULE_THRESHOLD, True).otherwise(False)
    )

    # --- Ghi kết quả vào Elasticsearch ---
    print(f"Ghi dữ liệu vào Elasticsearch index: {ELASTICSEARCH_INDEX}")
    es_write_query = processed_df \
        .writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.resource", f"{ELASTICSEARCH_INDEX}/_doc") \
        .option("es.mapping.id", "transaction_id") \
        .option("checkpointLocation", CHECKPOINT_LOCATION_ES) \
        .start()
    
    # --- Ghi dữ liệu vào PostgreSQL ---
    print("Cấu hình ghi dữ liệu vào PostgreSQL")
    
    # Hàm ghi dữ liệu vào PostgreSQL cho từng batch
    def write_to_postgres(batch_df, batch_id):
        print(f"Xử lý batch {batch_id} với {batch_df.count() if not batch_df.isEmpty() else 0} records")
        if not batch_df.isEmpty():
            try:
                # Lựa chọn và chuẩn bị dữ liệu để ghi vào PostgreSQL
                pg_df = batch_df.select(
                    "transaction_id", "user_id", "amount", "currency", 
                    "timestamp", "datetime", "location", "merchant_id", 
                    "is_suspicious_rule"
                )
                
                # Ghi vào PostgreSQL
                pg_df.write \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql://postgres_db:5432/fraud_db") \
                    .option("dbtable", "transactions_history") \
                    .option("user", "user") \
                    .option("password", "password") \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                
                print(f"Batch {batch_id}: Đã ghi {pg_df.count()} bản ghi vào PostgreSQL thành công")
            except Exception as e:
                print(f"Lỗi khi ghi batch {batch_id} vào PostgreSQL: {str(e)}")
    
    # Tạo stream ghi vào PostgreSQL
    pg_write_query = processed_df \
        .writeStream \
        .foreachBatch(write_to_postgres) \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", CHECKPOINT_LOCATION_PG) \
        .start()
    
    # Tùy chọn: Ghi ra console để debug
    console_query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("Tất cả streaming queries đã bắt đầu. Đang chờ termination...")
    
    # Chờ tất cả các queries kết thúc
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_transactions()