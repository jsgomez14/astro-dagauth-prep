FROM quay.io/astronomer/astro-runtime:10.1.0
ENV AIRFLOW_VAR_MY_DAG_PARTNER='{"name": "partner_a", "api_secret": "mysecret", "path": "tmp/partner_a"}'
# ENV AIRFLOW_CONN_NAME_OF_CONNECTION=your_connection

# There are 6 different ways of creating variables in Airflow 😱
# Airflow UI   👌
# Airflow CLI 👌
# REST API 👌
# Environment Variables ❤️
# Secret Backend ❤️
# Programatically 😖

# By creating a variable with an environment variable you:

# avoid making a connection to your DB
# hide sensitive values (the variable can only be fetched within a DAG)