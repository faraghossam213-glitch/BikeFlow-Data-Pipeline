FROM apache/spark:3.5.8
USER root
WORKDIR /app
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.8.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.8/spark-token-provider-kafka-0-10_2.12-3.5.8.jar && \
    curl -fsSL -o /opt/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSL -o /opt/spark/jars/postgresql-42.7.5.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar && \
    pip install --no-cache-dir psycopg2-binary==2.9.10
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.8.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.jar && \
    curl -fsSL -o /opt/spark/jars/kafka-clients-3.4.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
COPY stream.py /app/stream.py
USER 185
