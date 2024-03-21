FROM python:3.8-slim

LABEL version="1.0"
LABEL author="camiloc"

COPY . usr/src/app

# work directory
#WORKDIR /app
WORKDIR /usr/src/app

# Dependecies installing
RUN pip install requests psycopg2-binary pandas
RUN pip3 install -U python-dotenv
# RUN pip install apache-airflow

# PYTHONPATH extends
ENV PYTHONPATH "${PYTHONPATH}:/usr/src/app/src:/usr/src/app/airflow_docker/dags"

# run script
CMD ["python", "-u", "main.py"]