import os
import redis
import requests
from flask import Flask, jsonify, request
from datetime import datetime

app = Flask(__name__)
redis_client = redis.Redis(host='cache-db', port=6379,
                           db=0, decode_responses=True)
LLM_SERVICE_URL = "http://llm-module:5001/generate"


@app.route('/query', methods=['POST'])
def query_cache():
    data = request.get_json()
    if not data or 'question' not in data or 'id' not in data:
        return jsonify({"error": "Se requiere 'id' y 'question'"}), 400

    question_id = data['id']
    question = data['question']

    timestamp = datetime.now().isoformat()

    cached_response = redis_client.get(question)

    if cached_response is not None:
        print(f"{timestamp} - HIT - ID:{question_id}")
        return jsonify({"source": "cache", "answer": cached_response})
    else:
        print(f"{timestamp} - MISS - ID:{question_id}")
        try:
            response = requests.post(LLM_SERVICE_URL, json=data)
            response.raise_for_status()

            llm_answer = response.json().get('answer')
            if llm_answer is not None:
                redis_client.set(question, llm_answer)
            else:
                print(
                    f"{timestamp} - WARN - ID:{question_id} - Respuesta nula del LLM.")

            return jsonify({"source": "llm-module", "answer": llm_answer})

        except requests.exceptions.RequestException as e:
            print(
                f"{timestamp} - ERROR - ID:{question_id} - No se pudo contactar a llm-service: {e}")
            return jsonify({"error": f"No se pudo comunicar con llm-service: {e}"}), 503


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
