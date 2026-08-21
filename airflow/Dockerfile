FROM apache/airflow:2.9.3

USER root

# Spark's driver runs on the JVM (via py4j), so we need a JDK present
# even though this container never runs Spark itself - it only submits
# jobs to the remote Spark master.
# We symlink the installed JDK to a stable path so JAVA_HOME doesn't
# depend on the exact Debian package layout (which can vary by image).
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    ln -sf "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# The Spark provider gives Airflow the SparkSubmitOperator. The pyspark
# pip package conveniently bundles its own spark-submit script, so we
# don't need a separate full Spark install just to submit jobs.
RUN pip install --no-cache-dir \
      apache-airflow-providers-apache-spark==4.11.3 \
      pyspark==3.5.1

# Symlink to the pyspark package's install location so spark-submit is
# on PATH, regardless of the base image's Python version.
RUN PYSPARK_HOME="$(python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")" && \
    ln -s "$PYSPARK_HOME" /home/airflow/spark

ENV SPARK_HOME=/home/airflow/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
