#BUilder stage(temporary,  discarded after build)
FROM curlimages/curl AS builder

# ADD hadoop environment variables to the image
RUN mkdir -p /opt/spark/jars  \ 
    && curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar  \
    && curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar


FROM apache/spark-py:v3.3.2

USER root

# Set Environment Variables
ENV SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:${PATH}" \
    PYSPARK_PYTHON=python3 

WORKDIR /opt/spark-app

COPY requirements.txt .

RUN pip install --no-cache-dir \ 
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    -r requirements.txt

EXPOSE 4040 8888

CMD ["bash",  "-c", "jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root"]

