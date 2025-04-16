# --- Stage 1: Get Java 8 from a proper base ---
FROM eclipse-temurin:8-jdk as java-builder

# --- Stage 2: Build on top of Airflow ---
FROM apache/airflow:2.10.5

USER root

# Install unzip and any other dependencies
RUN apt-get update && apt-get install -y unzip && apt-get clean

# Copy Java 8 from the previous stage
COPY --from=java-builder /opt/java/openjdk /opt/java/openjdk

# Set JAVA_HOME and update PATH
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Confirm Java version (optional sanity check)
RUN java -version

USER $AIRFLOW_UID

COPY airflow/scripts .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]
