FROM apache/airflow:latest
USER root

# Set Java environment variable to match the Docker Compose volume mapping
ENV JAVA_HOME /opt/java
ENV PATH $PATH:$JAVA_HOME/bin

# Hadoop environment variables should match the volume mapping in Docker Compose
ENV HADOOP_VERSION 3.2.2
ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_CONF_DIR /opt/hadoop/etc/hadoop  # Corrected typo here
ENV PATH $PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Assuming Hadoop is already set up on the host and mapped via Docker Compose

# Installation of Airflow provider packages
RUN pip install --no-cache-dir \
    'apache-airflow-providers-apache-spark' \
    'apache-airflow-providers-apache-hive'

USER airflow