from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

#configuración del prodcutor
producer = KafkaProducer(
  bootstrap_servers='localhost:9092',
  value_serializer=lamba v: json.dumps(v).encode('utf-8')
)

productos=[
    {"nombre": "Laptop", "categoria": "Tecnologia", "precio": 1200},
    {"nombre": "Mouse", "categoria": "Accesorios", "precio": 25},
    {"nombre": "Teclado", "categoria": "Accesorios", "precio": 45},
    {"nombre": "Monitor", "categoria": "Tecnologia", "precio": 300},
    {"nombre": "Audifonos", "categoria": "Tecnologia", "precio": 80},
    {"nombre": "Silla Gamer", "categoria": "Muebles", "precio": 250}
]

ciudades=["Bogotá", "Medellín", "Valledupar", "Barranquilla", "Cartagena"]

def generar_venta():
  producto = random.choice(productos)
  return{
        "producto": producto["nombre"],
        "categoria": producto["categoria"],
        "precio": producto["precio"],
        "cantidad": random.randint(1, 3),
        "ciudad": random.choice(ciudades),
        "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

print("Enviando datos a Kafka...")

while True:
    venta = generar_venta()
    producer.send('ventas', value=venta)
    print(f" Venta enviada: {venta}")
    time.sleep(random.uniform(1, 3))
