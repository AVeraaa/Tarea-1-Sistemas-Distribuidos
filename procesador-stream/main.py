import os
import json
import time
import spacy
import psycopg2
import google.generativeai as genai
from psycopg2 import OperationalError as Psycopg2Error

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, ProcessFunction, OutputTag
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
)
from pyflink.common.serialization import DeserializationSchema, SerializationSchema, SimpleStringSchema

print("Iniciando Job de PyFlink...")

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC_INPUT = 'preguntas'
KAFKA_TOPIC_OUTPUT_SUCCESS = 'respuestas_procesadas'
KAFKA_TOPIC_OUTPUT_FALLBACK = 'fallback'

DB_HOST = os.getenv('DB_HOST', 'postgres-db')
DB_NAME = os.getenv('DB_NAME', 'yahoo_answers_db')
DB_USER = os.getenv('DB_USER', 'myuser')
DB_PASS = os.getenv('DB_PASS', 'mypassword')

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')


output_tag = OutputTag("errors", Types.STRING())


class ProcessQuestion(ProcessFunction):

    def __init__(self):
        self.db_conn = None
        self.gemini_model = None
        self.spacy_model = None
        print("ProcessQuestion instanciado.")

    def open(self, runtime_context):
        """
        Método de inicialización. Se llama una vez por instancia paralela.
        """
        print("Abriendo conexiones y cargando modelos...")

        while self.db_conn is None:
            try:
                self.db_conn = psycopg2.connect(
                    host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
                )
                print("Conectado a PostgreSQL (desde Flink).")
            except Psycopg2Error:
                print("Flink esperando a PostgreSQL...")
                time.sleep(2)

        try:
            genai.configure(api_key=GEMINI_API_KEY)
            self.gemini_model = genai.GenerativeModel('gemini-pro-latest')
            print("Modelo Gemini configurado.")
        except Exception as e:
            print(f"Error configurando Gemini: {e}")

        try:
            self.spacy_model = spacy.load("en_core_web_md-3.8.0")
            print("Modelo Spacy 'en_core_web_md-3.8.0' cargado.")
        except Exception as e:
            print(f"Error crítico: No se pudo cargar el modelo Spacy: {e}")

    def process_element(self, value, ctx: 'ProcessFunction.Context', collector):
        """
        Este método se llama para CADA mensaje en el stream.
        """
        question_id = None
        try:

            question_id = str(value.get('question_id'))
            print(f"Procesando ID: {question_id}")

            question_text = ""
            best_answer = ""
            with self.db_conn.cursor() as cursor:
                cursor.execute(
                    "SELECT question_text, best_answer FROM questions WHERE id = %s",
                    (question_id,)
                )
                result = cursor.fetchone()
                if result:
                    question_text, best_answer = result
                else:
                    raise Exception(
                        f"ID {question_id} no encontrado en la base de datos.")

            response = self.gemini_model.generate_content(question_text)
            llm_answer = response.text

            doc1 = self.spacy_model(best_answer)
            doc2 = self.spacy_model(llm_answer)
            score = doc1.similarity(doc2)

            result_data = {
                'question_id': question_id,
                'question_text': question_text,
                'best_answer': best_answer,
                'llm_answer': llm_answer,
                'score': score
            }

            collector.collect(json.dumps(result_data))

        except Exception as e:

            error_message = {
                'question_id': question_id or 'unknown',
                'error_time': time.time(),
                'error_message': str(e)
            }
            print(
                f"ERROR procesando ID {question_id}: {e}. Enviando a fallback.")
            collector.collect(json.dumps(error_message), output_tag)

    def close(self):
        """
        Cerrar conexiones al finalizar.
        """
        if self.db_conn:
            self.db_conn.close()
            print("Conexión PostgreSQL (desde Flink) cerrada.")


def main():
    print("Configurando el entorno de Flink...")
    env = StreamExecutionEnvironment.get_execution_environment()

    print("Los JARs de Kafka se cargan automáticamente desde /lib...")

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(KAFKA_TOPIC_INPUT) \
        .set_group_id("flink-question-processor-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    kafka_sink_success = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
        .set_topic(KAFKA_TOPIC_OUTPUT_SUCCESS)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ) \
        .build()

    kafka_sink_fallback = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
        .set_topic(KAFKA_TOPIC_OUTPUT_FALLBACK)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ) \
        .build()

    print("Definiendo el pipeline de Flink...")

    stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    parsed_stream = stream.map(lambda s: json.loads(
        s), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    processed_stream = parsed_stream.process(
        ProcessQuestion(), output_type=Types.STRING())

    fallback_stream = processed_stream.get_side_output(output_tag)

    print("Conectando streams a los sumideros de Kafka...")

    processed_stream.sink_to(kafka_sink_success).name("Kafka Sink (Success)")

    fallback_stream.sink_to(kafka_sink_fallback).name("Kafka Sink (Fallback)")

    print("Ejecutando el Job de Flink...")
    env.execute("Yahoo Answers LLM Processing Job")


if __name__ == "__main__":
    main()
