FROM centos:7
ARG RUST_VERSION

RUN set -e; \
    curl -L https://tarantool.io/UaooCnt/release/2/installer.sh | bash; \
    yum -y install gcc git tarantool tarantool-devel; \
    yum clean all;

ENV PATH=/root/.cargo/bin:${PATH}
RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
    sh -s -- -y --profile default --default-toolchain ${RUST_VERSION} -c rustfmt -c clippy;

COPY docker/ci-log-section /usr/bin/ci-log-section
