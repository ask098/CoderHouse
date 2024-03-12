FROM python:3.8-slim

LABEL version="1.0"
LABEL author="camiloc"

#ENV ENVIRONMENT_NAME=production
#ENV HOME=/opt/airflow

# copy code to container
#COPY src/airflow_docker/dags/nba_etl.py .
#COPY src/airflow_docker/dags/.env .

COPY . usr/src/app



# work directory
#WORKDIR /app
WORKDIR /usr/src/app

# Dependecies installing
RUN pip install requests psycopg2-binary python-dotenv pandas




# run script
CMD ["python", "-u", "main.py"]
