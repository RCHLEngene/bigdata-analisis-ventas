from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc

# Crear sesión Spark
spark = SparkSession.builder.appName("AnalisisEcommerce").getOrCreate()

# Cargar dataset
df = spark.read.csv("online_retail.csv", header=True, inferSchema=True)

# Ver datos
df.show(5)

# Limpiar datos (eliminar nulos)
df = df.dropna()

# Crear columna total de venta
df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

# 🔹 1. Ventas por país
ventas_pais = df.groupBy("Country").sum("TotalPrice").orderBy(desc("sum(TotalPrice)"))
ventas_pais.show()

# 🔹 2. Productos más vendidos
productos = df.groupBy("Description").sum("Quantity").orderBy(desc("sum(Quantity)"))
productos.show(10)

# 🔹 3. Clientes que más compran
clientes = df.groupBy("CustomerID").sum("TotalPrice").orderBy(desc("sum(TotalPrice)"))
clientes.show(10)
