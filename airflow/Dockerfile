# FROM apache/airflow:2.3.4
FROM apache/airflow:2.8.0
# ENV PYTHONUNBUFFERED 1
# ENV PYTHONDONTWRITEBYTECODE 1
# WORKDIR mkdir -/airflow/
# RUN apt-get update && apt-get install -qq -y \
# git gcc build-essential libpq-dev --fix-missing --no-install-recommends \
# && apt-get clean

# Make sure we are using latest pip
RUN pip install --upgrade pip
COPY /dags .
COPY requirements.txt .
RUN pip install -r requirements.txt
# FROM python:3.9-slim as base apache-airflow==2.8.0

# ENV PYTHONUNBUFFERED 1
# ENV PYTHONDONTWRITEBYTECODE 1
# WORKDIR M:/airflow/


# ENV PGSQL_HOME /opt/postgres
# RUN python3 -m venv $PGSQL_HOME
# RUN $PGSQL_HOME/bin/pip install py-postgresql
# ENV PGSQL_BIN $PGSQL_HOME/bin/postgres

# COPY  . ./dags/aftest/src
# RUN pip freeze > requirements.txt

# RUN pip install -r requirements.txt


# RUN pip install py-postgresql

# CMD ["python", "afdagtest.py"]