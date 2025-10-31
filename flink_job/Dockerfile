FROM flink:1.17.2-scala_2.12

WORKDIR /opt/flink/usrlib

RUN apt-get update && \
    apt-get install -y build-essential python3-dev wget && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python


RUN wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.17/flink-connector-kafka-3.1.0-1.17.jar -O /opt/flink/lib/flink-connector-kafka-3.1.0-1.17.jar
RUN wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.1.0/kafka-clients-3.1.0.jar -O /opt/flink/lib/kafka-clients-3.1.0.jar

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY main.py .