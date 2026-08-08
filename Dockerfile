FROM flink:2.2.1-scala_2.12-java17

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel --break-system-packages --ignore-installed
RUN pip3 install --no-cache-dir apache-flink==2.2.1 --break-system-packages

RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar

USER flink