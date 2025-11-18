FROM ubuntu:22.04
ARG RUST_VERSION

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y curl;

RUN curl -L https://tarantool.io/release/2/installer.sh | bash;

RUN apt install -y \
    gcc \
    git \
    libssl-dev \
    pkg-config \
    tarantool=2.11.7.g4e04060150-1 \
    tarantool-dev=2.11.7.g4e04060150-1 \
    ;

ENV PATH=/root/.cargo/bin:${PATH}
RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
    sh -s -- -y --profile minimal --default-toolchain ${RUST_VERSION} -c rustfmt -c clippy;

COPY tarantool/docker/ci-log-section /usr/bin/ci-log-section
