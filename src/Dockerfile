FROM python:3.8-slim

LABEL version="1.0"
LABEL author="camiloc"

#ENV ENVIRONMENT_NAME=production
#ENV HOME=/opt/airflow

# work directory
WORKDIR /app

# Dependecies installing
RUN pip install requests psycopg2-binary python-dotenv pandas

# copy code to container
COPY main.py .
COPY .env .
#COPY scr /src


# run script
CMD ["python", "-u", "main.py"]
