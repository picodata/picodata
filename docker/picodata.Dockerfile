FROM rockylinux:8 AS builder

ARG RUST_VERSION
RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile default --default-toolchain ${RUST_VERSION}
ENV PATH=/root/.cargo/bin:${PATH}

RUN dnf -y install dnf-plugins-core epel-release \
    && dnf config-manager --set-enabled powertools \
    && dnf module -y enable nodejs:20 \
    && curl --silent --location https://dl.yarnpkg.com/rpm/yarn.repo -o /etc/yum.repos.d/yarn.repo \
    && rpm --import https://dl.yarnpkg.com/rpm/pubkey.gpg \
    && dnf install -y --nobest \
                openssl-devel libunwind libunwind-devel \
                gcc gcc-c++ make cmake git libstdc++-static libtool \
                nodejs yarn \
    && dnf clean all

WORKDIR /build/picodata
COPY . .
RUN cargo build --locked --release --features webui

FROM rockylinux:8

COPY --from=builder /build/picodata/target/release/picodata /usr/bin/picodata

RUN chmod 755 /usr/bin/picodata \
    && mkdir -p /var/lib/picodata && mkdir -p /var/run/picodata \
    && groupadd -g 1000 picodata \
    && useradd -u 1000 -g 1000 picodata -s /usr/sbin/nologin \
    && chown 1000:1000 -R /var/lib/picodata

USER 1000:1000
ENV PICODATA_PG_LISTEN 0.0.0.0:4327
WORKDIR /var/lib/picodata

ENTRYPOINT ["/usr/bin/picodata"]
CMD ["run"]
