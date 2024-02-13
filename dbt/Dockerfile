FROM python:3.8.1-slim-buster
ENTRYPOINT []

# Set working directory
WORKDIR /usr/app/project

# Install OS dependencies
RUN apt-get update && apt-get install -qq -y \
git gcc build-essential libpq-dev --fix-missing --no-install-recommends \
&& apt-get clean

# Make sure we are using latest pip
RUN pip install --upgrade pip

COPY . .
RUN pip install dbt-core==1.7.7
RUN pip install dbt-postgres==1.7.7
# CMD dbt docs generate
# Start the dbt RPC server
# CMD dbt init
CMD dbt docs serve 
# --project-dir /usr/app/project
# FROM python:3.9-slim

# # RUN apt-get update && apt-get install -y --no-install recomends
# WORKDIR /usr/src/dbt/dbt_project
# RUN pip install --upgrade pip
# RUN pip install dbt-postgres
# # EXPOSE 15444
# # CMD  sleep infinity
# CMD dbt docs serve
# # && dbt build --profiles-dir profiles 
# # dbt deps &&
