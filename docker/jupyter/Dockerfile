FROM jupyter/base-notebook:2023-10-20

USER root

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    git tzdata unzip

USER ${NB_UID}

COPY build /build

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir --requirement /build/src/requirements.txt

RUN fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

USER root

RUN apt-get purge --yes \
    git
RUN apt-get autoremove --yes && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER ${NB_UID}
