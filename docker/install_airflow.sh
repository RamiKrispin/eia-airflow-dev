#!/usr/bin/env bash

VENV_NAME=$1
USERNAME=$2
FIRST=$3
LAST=$4 
ROLE=$5 
PASSWORD=$6 
EMAIL=$7
AIRFLOW_VERSION=$8

source /opt/$VENV_NAME/bin/activate

airflow db migrate 
airflow users create \
    --username $USERNAME \
    --firstname $FIRST \
    --lastname $LAST \
    --role $ROLE \
    --password $PASSWORD \
    --email $EMAIL

# airflow webserver --port 8080
# airflow scheduler