import os
import time
import requests
import random
import psycopg2
from psycopg2 import OperationalError

DB_HOST = os.environ.get('DB_HOST', 'postgres_db')
DB_USER = os.environ.get('DB_USER', 'myuser')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'mypassword')
DB_NAME = os.environ.get('DB_NAME', 'yahoo_answers')


API_URL = os.environ.get('API_URL', 'http://cache_service:5000')
QPS = int(os.environ.get('QPS', 1))
DISTRIBUTION = os.environ.get('TRAFFIC_DISTRIBUTION', 'uniforme')


def wait_for_db():
    """Espera activa a que la base de datos PostgreSQL esté disponible."""
    print("Esperando a que la base de datos PostgreSQL esté disponible...")
    while True:
        try:
            conn = get_db_connection()
            conn.close()
            print("¡Conexión a la base de datos PostgreSQL exitosa!")
            break
        except OperationalError:
            time.sleep(1)


def get_db_connection():
    """Establece y retorna una conexión a la base de datos."""
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )


def get_all_question_ids(conn):
    """Obtiene todos los IDs de las preguntas de la base de datos."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM questions")
        return [row[0] for row in cur.fetchall()]


def create_hot_set(conn, size=50):
    """Crea un 'conjunto caliente' de IDs de preguntas populares."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM questions ORDER BY id ASC LIMIT %s", (size,))
        return [row[0] for row in cur.fetchall()]


def get_question_id(all_ids, hot_set):
    """Selecciona un ID de pregunta basado en la distribución de tráfico."""
    if DISTRIBUTION == 'sesgada' and hot_set:
        if random.random() < 0.8:
            return random.choice(hot_set)

    return random.choice(all_ids)


def start_traffic_generator(all_ids, hot_set):
    """Inicia el bucle principal de generación de tráfico."""
    print(f"Iniciando generador de tráfico hacia {API_URL} a {QPS} QPS...")
    sleep_time = 1.0 / QPS

    while True:
        question_id = get_question_id(all_ids, hot_set)

        try:
            response = requests.get(f"{API_URL}/ask?question_id={question_id}")

            if response.status_code == 200:
                print(f"Cache HIT para question_id: {question_id}")
            elif response.status_code == 202:
                print(
                    f"Cache MISS para question_id: {question_id} (Encolado en Kafka)")
            else:
                print(
                    f"Recibido estado {response.status_code} para question_id: {question_id}")

        except requests.exceptions.ConnectionError:
            print(
                f"Error de conexión: No se puede conectar a {API_URL}. Reintentando...")

        time.sleep(sleep_time)


if __name__ == "__main__":
    wait_for_db()
    conn = get_db_connection()

    if conn:
        all_ids = get_all_question_ids(conn)
        hot_set = None

        if DISTRIBUTION == 'sesgada':
            hot_set = create_hot_set(conn)
            print(
                f"Distribución 'sesgada'. Hot set de {len(hot_set)} IDs creado.")

        start_traffic_generator(all_ids, hot_set)

        conn.close()
