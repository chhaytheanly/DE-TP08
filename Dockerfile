FROM apache/spark:latest

USER root

RUN apt-get update && apt-get install -y \
    curl \
    python3-pip \
    openjdk-17-jdk \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:${PATH}"

RUN pip3 install matplotlib pandas

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN mkdir -p /opt/spark-apps /opt/spark-data

WORKDIR /opt/spark-apps      