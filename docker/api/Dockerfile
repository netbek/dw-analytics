FROM dw-analytics-python AS python-builder
FROM python:3.11.7-slim-bookworm

ARG DOCKER_UID
ARG DOCKER_GID

ENV DOCKER_USER=analyst

COPY --from=python-builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY build /build

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir --requirement /build/src/requirements.txt

RUN groupadd ${DOCKER_USER} --gid ${DOCKER_GID} && \
    useradd ${DOCKER_USER} --create-home --gid ${DOCKER_GID} --uid ${DOCKER_UID} --shell /bin/false && \
    echo "\n${DOCKER_USER} ALL=(ALL:ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    chown --recursive ${DOCKER_UID}:${DOCKER_GID} /home/${DOCKER_USER}

USER ${DOCKER_USER}
WORKDIR /home/${DOCKER_USER}
