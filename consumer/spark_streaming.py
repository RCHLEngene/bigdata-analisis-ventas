from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ventas") \
    .load()

# Convertir a texto
df = df.selectExpr("CAST(value AS STRING)")

# Separar columnas
df = df.withColumn("data", split(col("value"), ",")) \
       .select(
           col("data")[0].alias("Producto"),
           col("data")[1].alias("Cantidad"),
           col("data")[2].alias("Precio")
       )

# Mostrar en consola
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
