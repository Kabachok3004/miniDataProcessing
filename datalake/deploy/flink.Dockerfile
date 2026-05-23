FROM flink:1.18.1-scala_2.12-java11

# Python для PyFlink-джобов
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir apache-flink==1.18.1

# Kafka SQL connector (включает kafka client) — для Table API source/sink
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar \
    /opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar

# S3 (Hadoop) plugin — Flink требует его как plugin, не в lib/
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

RUN chmod -R 644 /opt/flink/lib/flink-sql-connector-kafka-*.jar
