# use official Airflow image as base
FROM apache/airflow:2.7.3-python3.11

# switch to root to install system dependencies
USER root

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# switch back to airflow user
USER airflow

# copy and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# copy your entire project into the image
COPY --chown=airflow:root . /opt/airflow/project

# set project root in Python path
ENV PYTHONPATH="/opt/airflow/project:${PYTHONPATH}"