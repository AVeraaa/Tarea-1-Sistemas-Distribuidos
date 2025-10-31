import os
import time
import json
import logging
import psycopg2
import google.generativeai as genai
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import start_http_server, Counter

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
DB_HOST = os.environ.get('DB_HOST', 'postgres_db')
DB_USER = os.environ.get('DB_USER', 'myuser')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'mypassword')
DB_NAME = os.environ.get('DB_NAME', 'yahoo_answers')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
METRICS_PORT = 8001


TOPIC_PREGUNTAS = 'preguntas'
TOPIC_RESPUESTAS = 'respuestas'
TOPIC_FALLIDAS = 'fallidas'


LLM_SUCCESS_TOTAL = Counter('llm_requests_success_total',
                            'Total de peticiones a Gemini exitosas (enviadas a Flink)')
LLM_FAILED_TOTAL = Counter('llm_requests_failed_total',
                           'Total de peticiones a Gemini fallidas (enviadas a fallback)')
DB_ERRORS_TOTAL = Counter('llm_db_errors_total',
                          'Total de errores de consulta a BBDD')

if not GEMINI_API_KEY:
    logging.warning(
        "GEMINI_API_KEY no está configurada. El servicio no podrá llamar al LLM.")
else:
    genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-pro-latest')


def connect_to_kafka_consumer(topic):
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                group_id='llm-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logging.info(
                f"Conectado a Kafka (Consumidor) en el tópico '{topic}'")
        except NoBrokersAvailable:
            logging.warning(
                "No se pudo conectar a Kafka. Reintentando en 5 segundos...")
            time.sleep(5)
    return consumer


def connect_to_kafka_producer():
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            logging.info("Conectado a Kafka (Productor)")
        except NoBrokersAvailable:
            logging.warning(
                "No se pudo conectar a Kafka (Productor). Reintentando en 5 segundos...")
            time.sleep(5)
    return producer


def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logging.info("Conectado a PostgreSQL")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Error al conectar a PostgreSQL: {e}")
        return None


def get_question_from_db(conn, question_id):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT question_content, best_answer FROM questions WHERE id = %s", (question_id,))
            result = cur.fetchone()
            if result:
                return result[0], result[1]
            else:
                logging.warning(
                    f"No se encontró la pregunta con ID {question_id} en la BD.")
                return None, None
    except Exception as e:
        logging.error(f"Error al consultar la BD: {e}")
        conn.rollback()
        DB_ERRORS_TOTAL.inc()
        return None, None


def call_gemini_api(question_text):
    try:
        response = model.generate_content(question_text)
        if response.parts:
            return response.text
        else:
            logging.warning(
                f"Respuesta de Gemini recibida pero sin contenido: {response.prompt_feedback}")
            return None
    except Exception as e:
        logging.error(f"Error al llamar a la API de Gemini: {e}")
        raise e


def main():
    try:
        start_http_server(METRICS_PORT)
        logging.info(
            f"Servidor de métricas iniciado en el puerto {METRICS_PORT}")
    except Exception as e:
        logging.error(f"No se pudo iniciar el servidor de métricas: {e}")

    consumer = connect_to_kafka_consumer(TOPIC_PREGUNTAS)
    producer = connect_to_kafka_producer()
    db_conn = connect_to_postgres()

    if not db_conn:
        logging.fatal("No se pudo conectar a PostgreSQL. Abortando.")
        return
    if not GEMINI_API_KEY:
        logging.fatal(
            "GEMINI_API_KEY no está definida. El servicio no puede operar. Abortando.")
        return

    logging.info("Servicio LLM Consumer iniciado. Esperando mensajes...")

    for message in consumer:
        data = message.value
        question_id = data.get('question_id')

        if not question_id:
            logging.warning(f"Mensaje recibido sin 'question_id': {data}")
            continue

        logging.info(f"Procesando question_id: {question_id}")

        question_text, human_answer = get_question_from_db(
            db_conn, question_id)

        if not question_text or not human_answer:
            continue

        try:
            llm_answer = call_gemini_api(question_text)

            if llm_answer:
                logging.info(f"Éxito con ID {question_id}. Enviando a Flink.")
                output_data = {
                    'question_id': question_id,
                    'human_answer': human_answer,
                    'llm_answer': llm_answer
                }
                producer.send(TOPIC_RESPUESTAS, value=output_data)
                LLM_SUCCESS_TOTAL.inc()
            else:
                logging.warning(
                    f"Falló la generación de contenido para ID {question_id} (respuesta vacía). Enviando a fallback.")
                producer.send(TOPIC_FALLIDAS, value={
                              'question_id': question_id})
                LLM_FAILED_TOTAL.inc()

        except Exception as e:
            logging.error(
                f"Fallo con ID {question_id} (Probable Rate Limit). Enviando a fallback.")
            producer.send(TOPIC_FALLIDAS, value={'question_id': question_id})
            LLM_FAILED_TOTAL.inc()

        finally:
            producer.flush()


if __name__ == "__main__":
    main()
