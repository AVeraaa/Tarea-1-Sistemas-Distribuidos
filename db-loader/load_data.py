import os
import time
import pandas as pd
import psycopg2
from psycopg2 import OperationalError

DATA_PATH = '/data/test.csv'


def wait_for_db():
    """Espera a que la base de datos PostgreSQL esté disponible."""
    print("Esperando a que la base de datos PostgreSQL esté disponible...")
    db_host = os.getenv('DB_HOST', 'localhost')
    db_name = os.getenv('DB_NAME', 'yahoo_answers_db')
    db_user = os.getenv('DB_USER', 'myuser')
    db_pass = os.getenv('DB_PASS', 'mypassword')

    conn = None
    while not conn:
        try:
            conn = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_pass
            )
        except OperationalError:
            print("Base de datos no disponible, esperando 2 segundos...")
            time.sleep(2)
    print("¡Conexión a la base de datos PostgreSQL exitosa!")
    return conn


def load_data(conn):
    """Carga los datos desde el CSV a la tabla de PostgreSQL."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS questions (
                    id SERIAL PRIMARY KEY,
                    question_text TEXT,
                    best_answer TEXT,
                    llm_answer TEXT,
                    score FLOAT,
                    access_count INTEGER DEFAULT 0
                );
            """)
            conn.commit()
            print("Tabla 'questions' asegurada.")

            cursor.execute("SELECT COUNT(*) FROM questions;")
            if cursor.fetchone()[0] > 0:
                print("La tabla 'questions' ya contiene datos. Saltando la carga.")
                return

            print(f"Cargando datos desde {DATA_PATH}...")

            try:
                df = pd.read_csv(DATA_PATH, header=None, names=[
                                 'class', 'question_text', 'content', 'best_answer'])
            except FileNotFoundError:
                print(
                    f"Error: No se pudo encontrar el archivo CSV en {DATA_PATH}.")
                print("Verifica que el volumen en docker-compose.yml esté correcto.")
                return

            print(f"Insertando {len(df)} registros en la base de datos...")
            for index, row in df.iterrows():
                cursor.execute(
                    "INSERT INTO questions (question_text, best_answer) VALUES (%s, %s)",
                    (row['question_text'], row['best_answer'])
                )
            conn.commit()
            print(f"¡Se han insertado {len(df)} registros exitosamente!")

    except (Exception, psycopg2.Error) as error:
        print("Error durante la carga de datos:", error)
    finally:
        if conn:
            conn.close()
            print("Conexión a PostgreSQL cerrada.")


if __name__ == "__main__":
    connection = wait_for_db()
    if connection:
        load_data(connection)
