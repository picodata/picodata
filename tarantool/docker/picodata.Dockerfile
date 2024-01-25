ARG TARANTOOL_TAG
FROM docker-public.binary.picodata.io/tarantool:${TARANTOOL_TAG}
ARG RUST_VERSION

RUN set -e; \
    rm -f /etc/yum.repos.d/pg.repo && \
    yum -y install gcc git && \
    yum clean all

# Install rust + cargo
ENV PATH=/root/.cargo/bin:${PATH}
RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
    sh -s -- -y --profile default --default-toolchain ${RUST_VERSION} -c rustfmt -c clippy;

# Install glauth for LDAP testing
RUN set -e; \
    cd /bin; \
    curl -L -o glauth https://github.com/glauth/glauth/releases/download/v2.3.0/glauth-linux-amd64; \
    chmod +x glauth;

COPY docker/ci-log-section /usr/bin/ci-log-section
