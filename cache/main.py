import os
import time
import json
import logging
from flask import Flask, request, jsonify
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Counter

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

REDIS_HOST = os.environ.get('REDIS_HOST', 'redis_cache')
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29029')
TOPIC_NAME = 'preguntas'

app = Flask(__name__)

metrics = PrometheusMetrics(app)
CACHE_HITS = Counter('cache_hits_total', 'Total de aciertos de cache')
CACHE_MISSES = Counter('cache_misses_total',
                       'Total de fallos de cache (enviados a Kafka)')


def connect_to_redis():
    while True:
        try:
            redis_client = Redis(host=REDIS_HOST, port=6379,
                                 db=0, decode_responses=True)
            redis_client.ping()
            logging.info("Conectado a Redis exitosamente.")
            return redis_client
        except RedisConnectionError:
            logging.warning("Esperando a que Redis esté disponible...")
            time.sleep(3)


def connect_to_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logging.info(f"Conectado a Kafka exitosamente.")
            return producer
        except NoBrokersAvailable:
            logging.warning(
                f"Esperando a que el broker de Kafka en {KAFKA_BROKER} esté disponible...")
            time.sleep(3)


redis_client = connect_to_redis()
kafka_producer = connect_to_kafka()


@app.route('/ask', methods=['GET'])
def ask():
    question_id = request.args.get('question_id')

    if not question_id:
        return jsonify({"error": "No se proporcionó question_id"}), 400

    try:
        cached_response = redis_client.get(f"question:{question_id}")

        if cached_response:
            # CACHE HIT
            CACHE_HITS.inc()
            logging.info(f"Cache HIT para question_id: {question_id}")
            return jsonify({
                "question_id": question_id,
                "answer": cached_response,
                "source": "cache"
            }), 200
        else:
            CACHE_MISSES.inc()
            logging.info(
                f"Cache MISS para question_id: {question_id}. Enviando a Kafka.")

            message = {'question_id': question_id}
            kafka_producer.send(TOPIC_NAME, value=message)
            kafka_producer.flush()

            return jsonify({
                "message": "Pregunta recibida y encolada para procesamiento.",
                "question_id": question_id
            }), 202

    except Exception as e:
        logging.error(f"Error procesando question_id {question_id}: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
