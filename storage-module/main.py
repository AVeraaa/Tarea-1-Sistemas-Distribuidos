import os
import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import psycopg2
from psycopg2 import OperationalError as Psycopg2Error
from redis import Redis, ConnectionError as RedisConnectionError

print("Iniciando Storage Module...")

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC_INPUT = 'respuestas_procesadas'

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'yahoo_answers_db')
DB_USER = os.getenv('DB_USER', 'myuser')
DB_PASS = os.getenv('DB_PASS', 'mypassword')

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')

consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_INPUT,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            group_id='storage-group'
        )
        print("Conectado a Kafka exitosamente.")
    except NoBrokersAvailable:
        print(
            f"Esperando a que el broker de Kafka en {KAFKA_BROKER} esté disponible...")
        time.sleep(2)

db_conn = None
while db_conn is None:
    try:
        db_conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Conectado a PostgreSQL exitosamente.")
    except Psycopg2Error:
        print(f"Esperando a que PostgreSQL en {DB_HOST} esté disponible...")
        time.sleep(2)

redis_client = None
while redis_client is None:
    try:
        redis_client = Redis(host=REDIS_HOST, port=6379,
                             db=0, decode_responses=True)
        redis_client.ping()
        print("Conectado a Redis exitosamente.")
    except RedisConnectionError:
        print(f"Esperando a que Redis en {REDIS_HOST} esté disponible...")
        time.sleep(2)

print("Storage Module está listo y escuchando mensajes...")

try:
    for message in consumer:
        try:

            data = json.loads(message.value.decode('utf-8'))
            print(f"Mensaje recibido para ID: {data.get('question_id')}")

            question_id = str(data.get('question_id'))
            llm_answer = data.get('llm_answer')
            score = data.get('score')

            cache_payload = json.dumps(data)

            with db_conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE questions 
                    SET llm_answer = %s, score = %s, access_count = access_count + 1 
                    WHERE id = %s
                    """,
                    (llm_answer, score, question_id)
                )
                db_conn.commit()
            print(f"PostgreSQL actualizado para ID: {question_id}")

            redis_client.set(question_id, cache_payload)
            print(
                f"Caché de Redis actualizada (calentada) para ID: {question_id}")

        except json.JSONDecodeError:
            print(
                f"Error: No se pudo decodificar el mensaje de Kafka: {message.value}")
        except Exception as e:
            print(f"Error procesando el mensaje: {e}. Datos: {message.value}")

except KeyboardInterrupt:
    print("Storage Module detenido.")
finally:
    if db_conn:
        db_conn.close()
    consumer.close()
