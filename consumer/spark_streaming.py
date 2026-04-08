from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StreamingExample").getOrCreate()

# Leer stream (simulado con carpeta)
df_stream = spark.readStream.csv("stream_data/", header=True)

query = df_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
