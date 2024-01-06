#!/bin/bash
set -e

prefect_username="prefect"
prefect_password="prefect"

psql -tc "select 1 from pg_user where usename = '${prefect_username}'" | grep -q 1 || \
    psql -c "create user ${prefect_username} with password '${prefect_password}';"

prefect_database="prefect"

psql -tc "select 1 from pg_catalog.pg_database where datname = '${prefect_database}';" | grep -q 1 || \
    psql -c "create database ${prefect_database} with owner ${prefect_username};"
