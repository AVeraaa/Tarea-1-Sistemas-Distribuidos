FROM apache/flink:1.18.0-scala_2.12-java11

ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH

RUN apt-get update -y && \
    apt-get install -y python3-pip curl && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3 1 && \
    rm -rf /var/lib/apt/lists/*


ENV FLINK_CONNECTOR_VERSION=3.1.0-1.18 
RUN curl -o $FLINK_HOME/lib/flink-connector-kafka.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/${FLINK_CONNECTOR_VERSION}/flink-connector-kafka-${FLINK_CONNECTOR_VERSION}.jar

ENV KAFKA_CLIENTS_VERSION=3.6.1
RUN curl -o $FLINK_HOME/lib/kafka-clients-3.6.1.jar \
    https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir https://github.com/explosion/spacy-models/releases/download/en_core_web_md-3.8.0/en_core_web_md-3.8.0-py3-none-any.whl

COPY main.py /app/main.py

WORKDIR /app