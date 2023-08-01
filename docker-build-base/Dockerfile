FROM ubuntu:22.04

RUN set -e; \
    apt update -y && \
    apt install -y \
        build-essential \
        git unzip wget curl\
        libtool autoconf cmake make \
        python3 \
        python3-yaml \
        python3-six \
        python3-gevent \
        python3-pip

RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile default --default-toolchain 1.68.2
ENV PATH=/root/.cargo/bin:${PATH}

COPY ci-log-section /usr/bin/ci-log-section

# install Pipenv
COPY requirements-pipenv.txt /tmp/requirements-pipenv.txt
RUN PIP_NO_CACHE_DIR=true python3.10 -m pip install -q --require-hashes -r /tmp/requirements-pipenv.txt \
    && rm /tmp/requirements-pipenv.txt

# install nodejs
RUN curl -fsSL https://deb.nodesource.com/setup_19.x | bash - &&\
    apt install -y nodejs &&\
    corepack enable
