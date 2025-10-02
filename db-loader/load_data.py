import os
import time
import psycopg2
import pandas as pd

DB_NAME = "yahoo_answers"
DB_USER = "myuser"
DB_PASS = "mypassword"
DB_HOST = "storage"
DB_PORT = "5432"


def wait_for_db():
    """Espera a que la base de datos esté lista para aceptar conexiones."""
    retries = 10
    wait_time = 5
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
            )
            conn.close()
            print("¡La base de datos está lista!")
            return True
        except psycopg2.OperationalError:
            print(
                f"La base de datos no está lista. Reintentando en {wait_time} segundos... ({i+1}/{retries})")
            time.sleep(wait_time)
    print("No se pudo conectar a la base de datos después de varios intentos.")
    return False


def setup_database():
    """
    Se conecta a la base de datos, crea la tabla si no existe,
    y carga los datos desde el CSV.
    """
    if not wait_for_db():
        return

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        cur = conn.cursor()
        print("¡Conexión a la base de datos PostgreSQL exitosa!")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS questions (
                id SERIAL PRIMARY KEY,
                question_title TEXT NOT NULL,
                question_content TEXT,
                best_answer TEXT NOT NULL,
                llm_answer TEXT,
                score FLOAT,
                access_count INTEGER DEFAULT 0
            );
        """)
        conn.commit()
        print("Tabla 'questions' asegurada.")

        csv_path = '../data/test.csv'
        if not os.path.exists(csv_path):
            clean_path = os.path.abspath(csv_path)
            print(
                f"Error: El archivo no se encontró en la ruta esperada: {clean_path}")
            return

        print(f"Cargando datos desde {csv_path}...")
        df = pd.read_csv(csv_path, header=None, names=[
                         'class_index', 'question_title', 'question_content', 'best_answer'])

        df = df.head(10000)

        for index, row in df.iterrows():
            cur.execute(
                "INSERT INTO questions (question_title, question_content, best_answer) VALUES (%s, %s, %s)",
                (row['question_title'], row['question_content'], row['best_answer'])
            )

        conn.commit()
        cur.close()
        print(f"¡Se han insertado {len(df)} registros en la base de datos!")
        print("El servicio de carga de datos ha finalizado su trabajo.")

    except Exception as e:
        print(f"Ocurrió un error: {e}")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    setup_database()
