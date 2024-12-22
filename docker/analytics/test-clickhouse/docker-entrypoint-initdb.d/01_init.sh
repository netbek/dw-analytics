#!/bin/bash
set -e

clickhouse-client -n <<-EOSQL
create database if not exists analytics;
EOSQL
