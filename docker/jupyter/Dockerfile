FROM dw-analytics-python AS python-builder
FROM jupyter/base-notebook:2023-10-20

USER root

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    tzdata unzip

USER ${NB_UID}

COPY --chown="${NB_UID}:${NB_GID}" --from=python-builder /usr/local/lib/python3.11/site-packages/ /opt/conda/lib/python3.11/site-packages/
COPY build /build

RUN fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

USER root

RUN apt-get autoremove --yes && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER ${NB_UID}
