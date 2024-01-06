FROM python:3.11.7-slim-bookworm

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    gcc git patch

COPY build /build

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir --requirement /build/src/requirements.txt

WORKDIR /usr/local/lib/python3.11/site-packages
RUN patch -p1 < /build/patches/dbt-clickhouse/columns_in_query.diff
RUN patch -p1 < /build/patches/dbt-clickhouse/format_columns.diff

RUN apt-get purge --yes \
    gcc git patch
RUN apt-get autoremove --yes && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
