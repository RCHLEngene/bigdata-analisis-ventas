from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession.builder.appName("BatchVentas").getOrCreate()

df = spark.read.json("data/ventas.json")

df = df.withColumn("total", col("precio") * col("cantidad"))

resultado = df.groupBy("producto") \
    .agg(sum("total").alias("ingresos_totales"))

resultado.show()
