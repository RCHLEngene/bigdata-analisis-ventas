Sistema de Análisis de Ventas con Big Data

Descripción
Este proyecto implementa un sistema de análisis de ventas en comercio electrónico utilizando tecnologías Big Data como Apache Kafka y Apache Spark.

El sistema permite:
- Procesamiento de datos en tiempo real (streaming)
- Procesamiento por lotes (batch)
- Análisis de ventas

Tecnologías utilizadas
- Apache Kafka
- Apache Spark
- Python
- PySpark

Arquitectura
1. Un productor envía datos de ventas a Kafka
2. Kafka gestiona los datos en un tópico
3. Spark Streaming consume los datos en tiempo real
4. Se realiza análisis de ventas

Ejecución

### 1. Iniciar Kafka
```bash
docker-compose up -d
