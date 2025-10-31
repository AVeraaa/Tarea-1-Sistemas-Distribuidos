import os
import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

print("Iniciando Fallback Module...")

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC_INPUT = 'fallback'

consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_INPUT,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            group_id='fallback-group'
        )
        print("Conectado a Kafka exitosamente.")
    except NoBrokersAvailable:
        print(
            f"Esperando a que el broker de Kafka en {KAFKA_BROKER} esté disponible...")
        time.sleep(2)

print("Fallback Module está listo y escuchando mensajes de error...")

try:
    for message in consumer:
        try:
            error_message = message.value.decode('utf-8')
            print("---------------------------------------------------------")
            print(f"[FALLBACK DETECTADO] Se recibió un error de procesamiento:")
            print(error_message)
            print("---------------------------------------------------------")

        except Exception as e:
            print(f"Error en el propio Fallback module: {e}")

except KeyboardInterrupt:
    print("Fallback Module detenido.")
finally:
    consumer.close()
