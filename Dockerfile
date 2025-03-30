# Use the official Bitnami Spark image with a specific version for consistency
FROM bitnami/spark:latest

# Switch to root user to install additional packages
USER root
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*



# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --default-timeout=1000 --no-cache-dir -r requirements.txt

# Create JARs directory and download necessary connectors
RUN mkdir -p /opt/spark/jars && \
    # MongoDB Connector
    curl -L -o /opt/spark/jars/mongo-spark-connector_2.12-10.4.1.jar \
    https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.4.1/mongo-spark-connector_2.12-10.4.1.jar && \
    # Kafka Connector
    curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar

# Add and set permissions for the wait-for-it script
ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh /app/wait-for-it.sh
RUN chmod +x /app/wait-for-it.sh

# Copy application files into the container
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    SPARK_EXTRA_CLASSPATH=/opt/spark/jars/mongo-spark-connector_2.12-10.4.1.jar:/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar

# Expose port for Jupyter or Spark UI if needed
EXPOSE 8888

# Default command to keep the container running
CMD ["sleep", "infinity"]
