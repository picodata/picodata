FROM rockylinux:8 AS builder

RUN dnf -y install dnf-plugins-core \
    && dnf config-manager --set-enabled powertools \
    && dnf install -y gcc gcc-c++ make cmake git libstdc++-static libtool \
    && dnf clean all

RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile default --default-toolchain 1.65.0
ENV PATH=/root/.cargo/bin:${PATH}

WORKDIR /build/picodata
COPY . .
RUN cargo build --locked --release

FROM rockylinux:8

COPY --from=builder /build/picodata/target/release/picodata /usr/bin/picodata
COPY helm/entrypoint.sh /home/picouser/entrypoint.sh

RUN chmod 755 /usr/bin/picodata \
    && chmod 755 /home/picouser/entrypoint.sh \
    && groupadd -g 1000 picouser \
    && useradd -u 1000 -g 1000 picouser \
    && chown 1000:1000 -R /home/picouser

USER 1000:1000

WORKDIR /home/picouser

ENTRYPOINT ["/home/picouser/entrypoint.sh"]
