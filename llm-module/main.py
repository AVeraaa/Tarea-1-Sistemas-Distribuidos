import os
import time
import json
import pika
import requests
import pandas as pd
import spacy
from sqlalchemy import create_engine, text
import redis
import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted, DeadlineExceeded
from prometheus_client import start_http_server, Counter


GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
DB_HOST = os.environ.get('POSTGRES_HOST', 'postgres-db')
DB_USER = os.environ.get('POSTGRES_USER', 'postgres')
DB_PASS = os.environ.get('POSTGRES_PASSWORD', 'postgres')
DB_NAME = os.environ.get('POSTGRES_DB', 'yahoo_answers')
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

genai.configure(api_key=GEMINI_API_KEY)
MODEL_NAME = 'gemini-flash-latest'
NLP = None


REQUESTS_PROCESSED = Counter(
    'llm_requests_processed_total',
    'Total de peticiones LLM procesadas exitosamente'
)
REQUESTS_FAILED = Counter(
    'llm_requests_failed_total',
    'Total de peticiones LLM fallidas y enviadas a la DLQ',
    ['reason']
)


def connect_to_db(max_retries=10, delay_seconds=3):
    """Conexión a la base de datos con reintentos."""
    for i in range(1, max_retries + 1):
        try:
            engine = create_engine(DATABASE_URL)
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return engine
        except Exception as e:
            print(
                f"LLM-Module: DB no disponible. Reintentando en {delay_seconds}s... ({i}/{max_retries})")
            time.sleep(delay_seconds)
    raise ConnectionError(
        "No se pudo conectar a la base de datos después de varios intentos.")


def calculate_score(answer_a, answer_b):
    """Calcula la similitud semántica entre dos respuestas usando spaCy."""
    if not NLP:
        raise RuntimeError("Modelo spaCy no cargado.")

    doc_a = NLP(answer_a)
    doc_b = NLP(answer_b)

    score = doc_a.similarity(doc_b)
    return score


def process_message(ch, method, properties, body):
    """Función de callback que se ejecuta al recibir un mensaje de RabbitMQ."""
    global NLP, CLIENT

    try:
        data = json.loads(body.decode())
        question_id = data.get('question_id')
        question_text = data.get('question_text')

        print(
            f"\n[x] MENSAJE RECIBIDO (TAG: {method.delivery_tag}) para ID: {question_id}")
    except json.JSONDecodeError as e:
        print(f"Error al deserializar JSON: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        REQUESTS_FAILED.labels('json_error').inc()
        return

    # 2. Validación de Datos (Evitar NaN)
    if not question_text or question_text.lower() == 'nan':
        print(
            f"Error de Datos: Pregunta ID {question_id} es nula o 'nan'. Enviando a DLQ.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        REQUESTS_FAILED.labels('data_error').inc()
        return

    db_engine = None
    try:
        db_engine = connect_to_db()

        with db_engine.connect() as conn:
            query = text(
                f"SELECT best_answer, access_count FROM questions WHERE id = :id")
            result = conn.execute(query, {'id': question_id}).fetchone()

            if not result:
                raise ValueError(
                    f"Pregunta ID {question_id} no encontrada en DB.")

            best_answer = result[0]
            current_count = result[1]

            print(f"LLAMANDO a API Gemini para ID: {question_id}...")

            prompt = f"Responde a la siguiente pregunta de forma concisa: {question_text}"
            response = genai.GenerativeModel(
                MODEL_NAME).generate_content(prompt)
            llm_answer = response.text

            score = calculate_score(best_answer, llm_answer)

            new_count = current_count + 1

            update_query = text(f"""
                UPDATE questions
                SET llm_answer = :llm_answer, 
                    score = :score, 
                    access_count = :count
                WHERE id = :id
            """)

            conn.execute(update_query, {
                'llm_answer': llm_answer,
                'score': score,
                'count': new_count,
                'id': question_id
            })
            conn.commit()

            R = redis.Redis(host='redis-cache', port=6379, db=0)
            R.set(question_id, llm_answer)

            print(
                f"[✔] MENSAJE PROCESADO (ID: {question_id}). Score: {score:.2f}, Count: {new_count}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
            REQUESTS_PROCESSED.inc()

    except (ResourceExhausted, DeadlineExceeded) as e:
        print(
            f"ERROR: API Rate Limit/Timeout para ID {question_id}. Enviando a DLQ. Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        REQUESTS_FAILED.labels('rate_limit').inc()
    except Exception as e:
        print(
            f"ERROR inesperado al procesar ID {question_id}: {e}. Enviando a DLQ.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        REQUESTS_FAILED.labels('general_error').inc()


def main():
    global NLP

    if not GEMINI_API_KEY:
        print("ERROR: La variable GEMINI_API_KEY no está configurada. EL LLM-Module no puede operar.")
        exit(1)

    start_http_server(8001)
    print("Servidor de métricas Prometheus iniciado en el puerto 8001.")

    print(f"Modelo Gemini '{MODEL_NAME}' configurado exitosamente.")
    print("Cargando modelo spaCy (en_core_web_md)...")
    try:
        NLP = spacy.load("en_core_web_md")
        print("Modelo spaCy cargado exitosamente.")
    except Exception as e:
        print(
            f"ERROR: No se pudo cargar el modelo spaCy. Asegúrese de que esté instalado. {e}")
        return

    connection = None
    channel = None
    while True:
        try:
            credentials = pika.PlainCredentials('guest', 'guest')
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
            channel = connection.channel()

            channel.queue_declare(
                queue='processing_queue',
                durable=True,
                arguments={
                    'x-dead-letter-exchange': 'dlx_exchange',
                    'x-dead-letter-routing-key': 'dlq_key'
                }
            )

            channel.basic_consume(queue='processing_queue',
                                  on_message_callback=process_message,
                                  auto_ack=False)

            print(
                f" [*] Conectado a RabbitMQ. Esperando mensajes en la cola: 'processing_queue'.")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print(f"Error de conexión con RabbitMQ. Reintentando en 5 segundos...")
            time.sleep(5)
        except KeyboardInterrupt:
            print("Cerrando consumidor LLM-Module...")
            if connection:
                connection.close()
            break
        except Exception as e:
            print(
                f"Error inesperado en main loop: {e}. Reintentando en 5 segundos...")
            time.sleep(5)


if __name__ == '__main__':
    main()
