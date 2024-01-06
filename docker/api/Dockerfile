FROM python:3.11.7-slim-bookworm

ARG DOCKER_UID
ARG DOCKER_GID

ENV DOCKER_USER=analyst

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    gcc git

COPY build /build

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir --requirement /build/src/requirements.txt

RUN groupadd ${DOCKER_USER} --gid ${DOCKER_GID} && \
    useradd ${DOCKER_USER} --create-home --gid ${DOCKER_GID} --uid ${DOCKER_UID} --shell /bin/false && \
    echo "\n${DOCKER_USER} ALL=(ALL:ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    chown --recursive ${DOCKER_UID}:${DOCKER_GID} /home/${DOCKER_USER}

RUN apt-get purge --yes \
    gcc git
RUN apt-get autoremove --yes && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER ${DOCKER_USER}
WORKDIR /home/${DOCKER_USER}
