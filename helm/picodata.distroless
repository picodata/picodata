FROM debian:12 AS builder

RUN set -e; \
    apt update -y && \
    apt install -y \
        autoconf \
        build-essential \
        cmake \
        curl \
        git \
        libcurl4-openssl-dev \
        libicu-dev \
        libldap2-dev \
        libreadline-dev \
        libsasl2-dev \
        libssl-dev \
        libtool \
        libunwind-dev \
        libyaml-dev \
        libzstd-dev \
        make \
        ncurses-dev \
        pkg-config

RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile default --default-toolchain 1.76.0
ENV PATH=/root/.cargo/bin:${PATH}

RUN curl -SLO https://deb.nodesource.com/nsolid_setup_deb.sh && \
    chmod 755 nsolid_setup_deb.sh && \
    ./nsolid_setup_deb.sh 21 && \
    apt install -y nodejs && \
    corepack enable

WORKDIR /build/picodata
COPY . .
RUN cargo build --locked --release --features webui


FROM docker-public.binary.picodata.io/distroless/cc-debian12

COPY --from=builder /build/picodata/target/release/picodata /usr/bin/picodata

WORKDIR /var/lib/picodata

ENTRYPOINT ["/usr/bin/picodata"]
CMD ["run"]
