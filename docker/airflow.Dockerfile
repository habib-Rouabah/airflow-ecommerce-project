# Image Airflow enrichie des dépendances du projet (pandas, pymongo).
FROM apache/airflow:2.9.3-python3.11

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
