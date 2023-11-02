ARG TARANTOOL_TAG
FROM docker-public.binary.picodata.io/tarantool:${TARANTOOL_TAG}
ARG RUST_VERSION

RUN set -e; \
    yum -y install gcc git; \
    yum clean all;

ENV PATH=/root/.cargo/bin:${PATH}
RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
    sh -s -- -y --profile default --default-toolchain ${RUST_VERSION} -c rustfmt -c clippy;

COPY docker/ci-log-section /usr/bin/ci-log-section
