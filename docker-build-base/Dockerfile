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
        python3-pip \
        libzstd-dev \
        libyaml-dev \
        libcurl4-openssl-dev \
        libssl-dev \
        ncurses-dev \
        libreadline-dev \
        libicu-dev \
        libsasl2-dev \
        pkg-config \
        libldap2-dev \
        libssl-dev && \
        apt-get clean all

RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile default --default-toolchain 1.76.0
ENV PATH=/root/.cargo/bin:${PATH}

COPY ci-log-section /usr/bin/ci-log-section

# install Pipenv
COPY requirements-pipenv.txt /tmp/requirements-pipenv.txt
RUN PIP_NO_CACHE_DIR=true python3.10 -m pip install -q --require-hashes -r /tmp/requirements-pipenv.txt \
    && rm /tmp/requirements-pipenv.txt

# install nodejs
# https://github.com/nodesource/distributions#installation-scripts
RUN curl -SLO https://deb.nodesource.com/nsolid_setup_deb.sh && \
    chmod 755 nsolid_setup_deb.sh && \
    ./nsolid_setup_deb.sh 21 && \
    rm nsolid_setup_deb.sh && \
    apt-get install nodejs && \
    corepack enable && \
    apt-get clean all

# install glauth for ldap tests
RUN set -e; \
    cd /bin; \
    curl -L -o glauth https://github.com/glauth/glauth/releases/download/v2.3.0/glauth-linux-amd64; \
    chmod +x glauth;
