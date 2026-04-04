from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StrucType, StringType, IntergerType

spark = SparkSession.builder \
  .appName("VentasStreaming") \
  .getOrCreate()

schema = StructType() \
    .add("producto", StringType()) \
    .add("categoria", StringType()) \
    .add("precio", IntegerType()) \
    .add("cantidad", IntegerType()) \
    .add("ciudad", StringType()) \
    .add("fecha", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ventas") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

ventas_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Cálculo de ingresos
ventas_df = ventas_df.withColumn("total", col("precio") * col("cantidad"))

resultado = ventas_df.groupBy("producto") \
    .agg(sum("total").alias("ingresos_totales"))

query = resultado.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
