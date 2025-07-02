ARG TARANTOOL_VERSION

FROM docker-public.binary.picodata.io/tarantool:${TARANTOOL_VERSION}

ENV PATH=/usr/local/bin:/root/.cargo/bin:/root/.local/bin:${PATH}
ENV LD_LIBRARY_PATH=/usr/local/lib64:$LD_LIBRARY_PATH
ARG RUST_VERSION

RUN rm -f /etc/yum.repos.d/pgdg-redhat-all.repo \
    && dnf -y install dnf-plugins-core \
    && dnf config-manager --set-enabled powertools \
    && dnf module -y enable nodejs:20 postgresql:16 \
    && curl --silent --location https://dl.yarnpkg.com/rpm/yarn.repo -o /etc/yum.repos.d/yarn.repo \
    && rpm --import https://dl.yarnpkg.com/rpm/pubkey.gpg \
    && dnf install -y --nobest \
           bzip2-devel \
           cmake \
           cyrus-sasl-devel \
           curl-devel \
           findutils \
           git \
           gcc \
           gcc-c++ \
           golang \
           libcurl-devel \
           libffi-devel \
           libicu-devel \
           libstdc++-static \
           libtool \
           libunwind \
           libunwind-devel \
           libyaml-devel \
           libzstd-devel \
           make \
           nodejs \
           openldap-devel \
           openssl-devel \
           python3.11 \
           python3-gevent \
           python3.11-pip \
           python3.11-six \
           python3.11-yaml \
           readline-devel \
           yarn \
           zlib-devel \
           postgresql \
    && ln -fs /usr/bin/python3 /usr/bin/python \
    && dnf clean all && \
    mkdir -p $(go env GOPATH)/bin && \
    export PATH=$(go env GOPATH)/bin:$PATH && \
    git clone https://github.com/magefile/mage.git && \
    cd mage && go run bootstrap.go && cd .. && rm -rf mage && \
    git clone https://github.com/tarantool/cartridge-cli.git && \
    cd cartridge-cli && git checkout 2.12.2 && \
    mage build && mv ./cartridge /usr/local/bin && cd .. && rm -rf cartridge-cli

# install rust
RUN set -e; \
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
sh -s -- -y --profile default --default-toolchain ${RUST_VERSION} && \
rustup component add rustfmt && \
cargo install cargo-audit --locked

COPY docker-build-base/ci-log-section /usr/bin/ci-log-section

# install poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
RUN poetry --version

# install glauth for ldap tests
RUN set -e; \
    cd /bin; \
    curl -L -o glauth https://github.com/glauth/glauth/releases/download/v2.3.2/glauth-linux-amd64 && \
    chmod +x glauth

# Timezone
RUN ln -fs /usr/share/zoneinfo/Europe/Moscow /etc/localtime
