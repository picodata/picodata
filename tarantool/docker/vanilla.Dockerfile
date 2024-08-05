FROM ubuntu:22.04
ARG RUST_VERSION

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y curl;

RUN curl -L https://tarantool.io/release/2/installer.sh | bash;

RUN apt install -y \
    gcc \
    git \
    tarantool \
    tarantool-dev \
    ;

ENV PATH=/root/.cargo/bin:${PATH}
RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
    sh -s -- -y --profile default --default-toolchain ${RUST_VERSION} -c rustfmt -c clippy;

COPY docker/ci-log-section /usr/bin/ci-log-section
