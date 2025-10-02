import os
import psycopg2
import spacy
from flask import Flask, jsonify, request

app = Flask(__name__)
DB_NAME = "yahoo_answers"
DB_USER = "myuser"
DB_PASS = "mypassword"
DB_HOST = "storage"
DB_PORT = "5432"

print("Cargando modelo de spaCy...")
nlp = spacy.load("en_core_web_md")
print("Modelo de spaCy cargado.")


def get_db_connection():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST, port=DB_PORT)


@app.route('/generate', methods=['POST'])
def generate_answer_mock():
    data = request.get_json()
    if not data or 'id' not in data:
        return jsonify({"error": "Se requiere 'id'"}), 400

    question_id = data['id']
    print(f"Recibida pregunta ID {question_id} para generar (MODO SIMULADO).")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            "SELECT best_answer FROM questions WHERE id = %s;", (question_id,))
        result = cur.fetchone()
        if not result:
            return jsonify({"error": f"No se encontró la pregunta con ID {question_id}"}), 404

        best_answer_human = result[0]
        llm_answer_mock = best_answer_human

        doc_human = nlp(best_answer_human)
        doc_llm = nlp(llm_answer_mock)
        score = doc_human.similarity(doc_llm)
        print(
            f"Score de similitud calculado para ID {question_id}: {score:.4f}")

        cur.execute(
            """
            UPDATE questions 
            SET llm_answer = %s, score = %s, access_count = access_count + 1 
            WHERE id = %s;
            """,
            (llm_answer_mock, score, question_id)
        )
        conn.commit()
        cur.close()
        print(
            f"Respuesta simulada para ID {question_id} guardada (score y contador actualizados).")

        return jsonify({"answer": llm_answer_mock, "score": score})

    except Exception as e:
        print(f"Ocurrió un error en llm-module (mock): {e}")
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
