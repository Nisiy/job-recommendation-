FROM apache/airflow:2.8.0
USER root
RUN apt-get update -qq && apt-get install -y default-jdk-headless -qq && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow
RUN pip install --no-cache-dir \
    "numpy==1.24.4" \
    "pandas==2.0.3" \
    "scikit-learn==1.3.2" \
    "pyspark==3.4.1" \
    kafka-python \
    psycopg2-binary \
    streamlit \
    plotly \
    python-dotenv \
    "apache-airflow-providers-fab==1.0.4" \
    "apache-airflow-providers-openlineage==1.8.0"