FROM python:3.11.7-slim-bookworm AS python-builder

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    gcc git patch

COPY scripts/patches /build/patches
COPY requirements.txt /build/requirements.txt
COPY requirements_dev.txt /build/requirements_dev.txt

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir --requirement /build/requirements.txt
RUN pip install --no-cache-dir --requirement /build/requirements_dev.txt

WORKDIR /usr/local/lib/python3.11/site-packages
RUN patch -p1 < /build/patches/dbt-clickhouse/columns_in_query.diff
RUN patch -p1 < /build/patches/dbt-clickhouse/format_columns.diff

RUN apt-get purge --yes \
    gcc git patch
RUN apt-get autoremove --yes && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

####################################################################################################

FROM prefecthq/prefect:2.14.13-python3.11 AS prefect-common

ARG DOCKER_UID
ARG DOCKER_GID

ENV DOCKER_USER=analyst

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    ca-certificates curl git openssh-client

COPY --from=python-builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/

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

####################################################################################################

FROM jupyter/base-notebook:2023-10-20 AS jupyter

USER root

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    tzdata unzip

USER ${NB_UID}

COPY --from=python-builder --chown="${NB_UID}:${NB_GID}" /usr/local/lib/python3.11/site-packages/ /opt/conda/lib/python3.11/site-packages/

RUN fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

USER root

RUN apt-get autoremove --yes && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

####################################################################################################

FROM python:3.11.7-slim-bookworm AS api

ARG DOCKER_UID
ARG DOCKER_GID

ENV DOCKER_USER=analyst

COPY --from=python-builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY requirements_api.txt /build/requirements_api.txt

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir --requirement /build/requirements_api.txt

RUN groupadd ${DOCKER_USER} --gid ${DOCKER_GID} && \
    useradd ${DOCKER_USER} --create-home --gid ${DOCKER_GID} --uid ${DOCKER_UID} --shell /bin/false && \
    echo "\n${DOCKER_USER} ALL=(ALL:ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    chown --recursive ${DOCKER_UID}:${DOCKER_GID} /home/${DOCKER_USER}

USER ${DOCKER_USER}
WORKDIR /home/${DOCKER_USER}
