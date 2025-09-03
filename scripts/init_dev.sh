#!/bin/sh

# The script must be run from root folder of the repo that contains DAGs
if [ ! -d "./dags" ]; then
    echo "Current directory does not contains dags folder"
    exit 1
fi

echo "Creating directory structure"
mkdir -p ./logs ./plugins ./config

echo "Creating docker env file with Airflow UID"
echo -e "AIRFLOW_UID=$(id -u)" > .env

echo "Downloading docker-compose for Airflow 3.0.6"
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.6/docker-compose.yaml'

exit 0
