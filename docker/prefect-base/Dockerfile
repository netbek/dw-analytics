FROM prefecthq/prefect:2.14.13-python3.11

ARG DOCKER_UID
ARG DOCKER_GID

ENV DOCKER_USER=analyst

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    ca-certificates curl git openssh-client

COPY build /build

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir --requirement /build/src/requirements.txt

WORKDIR /usr/local/lib/python3.11/site-packages
RUN patch -p1 < /build/patches/dbt-clickhouse/columns_in_query.diff
RUN patch -p1 < /build/patches/dbt-clickhouse/format_columns.diff

WORKDIR /usr/local/dbt
RUN curl -O https://raw.githubusercontent.com/dbt-labs/dbt-completion.bash/915cdc5e301f5bc4c89324d3bd790320476728cf/dbt-completion.bash

RUN groupadd ${DOCKER_USER} --gid ${DOCKER_GID} && \
    useradd ${DOCKER_USER} --create-home --gid ${DOCKER_GID} --uid ${DOCKER_UID} --shell /bin/false && \
    echo "\n${DOCKER_USER} ALL=(ALL:ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    chown --recursive ${DOCKER_UID}:${DOCKER_GID} /home/${DOCKER_USER}

RUN apt-get autoremove --yes && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER ${DOCKER_USER}
WORKDIR /home/${DOCKER_USER}
