import os
import time
import random
import requests
import psycopg2

DB_NAME = "yahoo_answers"
DB_USER = "myuser"
DB_PASS = "mypassword"
DB_HOST = "storage"
DB_PORT = "5432"

CACHE_URL = "http://cache-logic:5000/query"
REQUEST_INTERVAL_SECONDS = 1
TRAFFIC_DISTRIBUTION = os.environ.get(
    'TRAFFIC_DISTRIBUTION', 'uniforme').lower()


def get_db_connection():
    """Establece una conexión con la base de datos."""
    while True:
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
            )
            return conn
        except psycopg2.OperationalError:
            print("Base de datos no disponible, esperando 5 segundos para reintentar...")
            time.sleep(5)


def get_question_from_db(query, params=None):
    """Ejecuta una consulta para obtener una pregunta."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()
    finally:
        if conn:
            conn.close()


def simulate_traffic():
    """Bucle principal que simula el tráfico de usuarios."""
    print(
        f"--- Iniciando Generador de Tráfico con Distribución: {TRAFFIC_DISTRIBUTION.upper()} ---")

    hot_set = []
    if TRAFFIC_DISTRIBUTION == 'sesgada':

        query = "SELECT id, question_title FROM questions ORDER BY RANDOM() LIMIT 50;"
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                hot_set = cur.fetchall()
            print(
                f"Distribución Sesgada: Creado un 'hot set' de {len(hot_set)} preguntas populares.")
        finally:
            if conn:
                conn.close()

    while True:
        question_data = None
        if TRAFFIC_DISTRIBUTION == 'sesgada' and hot_set:
            if random.random() < 0.8:
                question_data = random.choice(hot_set)
            else:
                question_data = get_question_from_db(
                    "SELECT id, question_title FROM questions ORDER BY RANDOM() LIMIT 1;")
        else:
            question_data = get_question_from_db(
                "SELECT id, question_title FROM questions ORDER BY RANDOM() LIMIT 1;")

        if question_data:
            question_id, question_title = question_data
            try:
                print(
                    f"\nEnviando pregunta ID {question_id}: '{question_title[:60]}...'")
                payload = {"id": question_id, "question": question_title}
                response = requests.post(CACHE_URL, json=payload)

                if response.status_code == 200:
                    source = response.json().get("source", "desconocido").upper()
                    if source == "CACHE":
                        print("Respuesta recibida. Fuente: CACHE (¡HIT!)")
                    else:
                        print(f"Respuesta recibida. Fuente: {source} (MISS)")
                else:
                    print(
                        f"Error del servicio de caché: {response.status_code} - {response.text}")

            except requests.exceptions.RequestException as e:
                print(
                    f"No se pudo conectar al servicio de caché, reintentando... ({e})")

        print(f"Esperando {REQUEST_INTERVAL_SECONDS} segundos...")
        time.sleep(REQUEST_INTERVAL_SECONDS)


if __name__ == "__main__":
    print("Generador de tráfico esperando 15 segundos para que los otros servicios arranquen...")
    time.sleep(15)
    simulate_traffic()
