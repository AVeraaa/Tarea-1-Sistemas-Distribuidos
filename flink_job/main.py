import logging
from pyflink.common import SimpleStringSchema, Types
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, RuntimeContext
import json
import spacy

KAFKA_BROKER = 'kafka:29092'
TOPIC_RESPUESTAS = 'respuestas'
TOPIC_VALIDADAS = 'validadas'
TOPIC_PREGUNTAS = 'preguntas'


class ScoreCalculator(MapFunction):
    """
    Esta función UDF (User-Defined Function) carga el modelo 'spacy'
    una vez por 'task' y calcula el score para cada respuesta.
    """

    def __init__(self):
        self.nlp = None

    def open(self, runtime_context: RuntimeContext):
        try:
            logging.info("Cargando modelo spacy en_core_web_md...")
            self.nlp = spacy.load('en_core_web_md')
            logging.info("Modelo spacy cargado exitosamente.")
        except Exception as e:
            logging.error(f"Error cargando el modelo spacy: {e}")

    def map(self, value):
        """
        Procesa cada mensaje de Kafka.
        'value' es el JSON del tópico 'respuestas'.
        """
        try:
            data = json.loads(value)
            question_id = data.get('question_id')
            human_answer_str = data.get('human_answer')
            llm_answer_str = data.get('llm_answer')

            if not all([question_id, human_answer_str, llm_answer_str, self.nlp]):
                logging.warning(
                    f"Mensaje incompleto o modelo no cargado: {data.get('question_id')}")
                return json.dumps({"error": "Mensaje incompleto", "id": question_id})

            doc1 = self.nlp(human_answer_str)
            doc2 = self.nlp(llm_answer_str)
            score = doc1.similarity(doc2)

            output_data = {
                'question_id': question_id,
                'human_answer': human_answer_str,
                'llm_answer': llm_answer_str,
                'score': score
            }
            return json.dumps(output_data)

        except Exception as e:
            logging.error(f"Error en UDF map: {e} - Data: {value}")
            return json.dumps({"error": "Error en procesamiento", "data": value})


def run_flink_job():
    """
    Define y ejecuta el pipeline de Flink.
    """
    env = StreamExecutionEnvironment.get_execution_environment()

    kafka_consumer = FlinkKafkaConsumer(
        topics=TOPIC_RESPUESTAS,
        properties={'bootstrap.servers': KAFKA_BROKER,
                    'group.id': 'flink-score-group'},
        deserialization_schema=SimpleStringSchema()
    )
    input_stream = env.add_source(kafka_consumer).name(
        "Kafka Source (respuestas)")

    scored_stream = input_stream.map(
        ScoreCalculator(), output_type=Types.STRING()).name("Calcular Score (Spacy)")

    output_tag_low_score = OutputTag("low-score", Types.STRING())

    class Splitter(MapFunction):
        """
        Esta función divide el stream en dos:
        - Main (alto score): Va a 'validadas'
        - Side Output (bajo score): Va de vuelta a 'preguntas'
        """

        def map(self, value):
            try:
                data = json.loads(value)
                score = data.get('score', 0)

                if score >= 0.8:
                    persistence_data = {
                        'question_id': data['question_id'],
                        'llm_answer': data['llm_answer'],
                        'score': data['score']
                    }
                    return json.dumps(persistence_data)
                else:
                    feedback_data = {'question_id': data['question_id']}
                    return (output_tag_low_score, json.dumps(feedback_data))

            except Exception as e:
                logging.error(f"Error en Splitter: {e}")
                # Ignoramos este mensaje
                return None

    high_score_stream = scored_stream \
        .filter(lambda x: json.loads(x).get('score', 0) >= 0.8) \
        .map(lambda x: json.dumps({  # Formateamos para el storage_module
            'question_id': json.loads(x)['question_id'],
            'llm_answer': json.loads(x)['llm_answer'],
            'score': json.loads(x)['score']
        }), output_type=Types.STRING()).name("Stream Score Alto (>= 0.8)")

    low_score_stream = scored_stream \
        .filter(lambda x: json.loads(x).get('score', 0) < 0.8) \
        .map(lambda x: json.dumps({  # Formateamos para el tópico 'preguntas'
            'question_id': json.loads(x)['question_id']
        }), output_type=Types.STRING()).name("Stream Score Bajo (< 0.8)")

    kafka_producer_validadas = FlinkKafkaProducer(
        topic=TOPIC_VALIDADAS,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': KAFKA_BROKER}
    )
    high_score_stream.add_sink(kafka_producer_validadas).name(
        "Kafka Sink (validadas)")

    kafka_producer_preguntas = FlinkKafkaProducer(
        topic=TOPIC_PREGUNTAS,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': KAFKA_BROKER}
    )
    low_score_stream.add_sink(kafka_producer_preguntas).name(
        "Kafka Sink (preguntas - Feedback)")

    logging.info("Iniciando Job de Flink...")
    env.execute("ScoreCalculatorJob")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    run_flink_job()
