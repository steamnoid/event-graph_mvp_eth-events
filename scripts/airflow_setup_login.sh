#!/bin/bash
cd "$(dirname "$0")"/../docker || exit 1

docker compose exec airflow-webserver airflow users create --username admin --firstname Airflow --lastname Admin --role Admin --email admin@example.com --password admin

echo "Airflow admin user created with username 'admin' and password 'admin'"