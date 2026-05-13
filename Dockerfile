
FROM apache/spark:3.5.2

USER root

#copy pre-downloaded jars
COPY hadoop-aws.jar   /opt/spark/jars/hadoop-aws-3.3.4.jar
COPY aws-java-sdk-bundle.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

RUN mkdir -p /opt/spark/conf && \
    printf "%s\n" \
    "spark.driver.extraClassPath /opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar" \
    "spark.executor.extraClassPath /opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar" \
    >> /opt/spark/conf/spark-defaults.conf

# Set Environment Variables
ENV SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/spark-app
    

WORKDIR /opt/spark-app

COPY requirements.txt .

RUN pip install --no-cache-dir \
   --default-timeout=700 \
   --retries=5 \
   --trusted-host pypi.org \
   --trusted-host files.pythonhosted.org \
   -r requirements.txt

EXPOSE 4040 8888

CMD ["bash"]

