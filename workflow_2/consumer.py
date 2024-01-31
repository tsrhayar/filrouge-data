from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws, abs, to_date, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from cryptography.fernet import Fernet
from faker import Faker
import random

# Initialise la session Spark
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .getOrCreate()

# Définit la configuration de la source Kafka
kafka_source = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecom_topic")
    .load()
)

# Définit le schéma JSON pour correspondre au format des données fournies
json_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("credit_card", StringType(), True),
    StructField("full_address", StringType(), True),
    StructField("user_source_type", StringType(), True),
    StructField("user_status", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("date_purchase", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("comment", StringType(), True)
])

# Traite les données JSON des messages Kafka
kafka_data = (
    kafka_source.selectExpr("CAST(value AS STRING)")
    .select(from_json("value", json_schema).alias("data"))
    .select(
        col("data.name"),
        col("data.age"),
        col("data.gender"),
        col("data.city"),
        col("data.country"),
        col("data.credit_card"),
        col("data.full_address"),
        col("data.user_source_type"),
        col("data.user_status"),
        col("data.category"),
        col("data.subcategory"),
        col("data.product_name"),
        col("data.price"),
        col("data.date_purchase"),
        col("data.quantity"),
        col("data.rating"),
        col("data.comment")
    )
)

# Applique les transformations
kafka_data = kafka_data.withColumn("product_name", 
                                   when(col("product_name").isNull(), 
                                        concat_ws(":", col("category"), col("subcategory"))))
kafka_data = kafka_data.withColumn("price", abs(col("price")))

colonnes_date = ["date_purchase"]  # Supposant que la colonne de date est "date_purchase"

for col in colonnes_date:
    kafka_data = kafka_data.withColumn(col, to_date(col, "yyyy-MM-dd"))

def generer_age_aleatoire():
    return random.randint(18, 70)

kafka_data = kafka_data.withColumn("age", 
                                   when((col("age") < 18) | (col("age") > 120) | (col("age").isNull()), 
                                        generer_age_aleatoire()).otherwise(col("age")))

kafka_data = kafka_data.withColumn("price", col("price").cast("double"))
kafka_data = kafka_data.withColumn("quantity", col("quantity").cast("integer"))
kafka_data = kafka_data.withColumn("total", col("price") * col("quantity"))

# Crypte les données sensibles
cle = Fernet.generate_key()
suite_cipher = Fernet(cle)

fake = Faker()

kafka_data = kafka_data.withColumn("name", suite_cipher.encrypt(col("name").cast("string").encode()))
kafka_data = kafka_data.withColumn("full_address", suite_cipher.encrypt(col("full_address").cast("string").encode()))
kafka_data = kafka_data.withColumn("credit_card", suite_cipher.encrypt(col("credit_card").cast("string").encode()))

# Démarre la requête de streaming
query = kafka_data \
    .writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/pyspark") \
    .option("spark.mongodb.connection.uri", 'mongodb://localhost:27017/') \
    .option("spark.mongodb.database", 'ecom') \
    .option("spark.mongodb.collection", 'ecom_collection') \
    .outputMode("append")

query.start().awaitTermination()
