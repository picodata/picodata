FROM docker-proxy.binary.picodata.io/rockylinux:8 AS tarantool-builder

RUN dnf install -y epel-release && \
    dnf install -y git gcc cmake3 autoconf automake libtool libarchive make gcc-c++ \
    zlib-devel readline-devel ncurses-devel openssl-devel libunwind-devel libicu-devel \
    python3-pyyaml python3-six python3-gevent && dnf clean all


COPY . local_tarantool_build
RUN cd local_tarantool_build/tarantool-sys && \
    rm -rf build && mkdir -p build && cd build && cmake .. && make -j$(nproc) && make install V=1


FROM docker-proxy.binary.picodata.io/rockylinux:8
ARG RUST_VERSION
ENV PATH=/root/.cargo/bin:${PATH}

RUN set -e; \
    rm -f /etc/yum.repos.d/pg.repo && \
    dnf -y install gcc git openssl-devel openssl libicu libgomp openssl-libs && \
    dnf clean all


COPY --from=tarantool-builder /usr/local/bin/tarantool /usr/local/bin/tarantool

# Install rust + cargo
RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
    sh -s -- -y --profile minimal --default-toolchain ${RUST_VERSION} -c rustfmt -c clippy;

# Install glauth for LDAP testing
RUN set -e; \
    cd /bin; \
    curl --silent --show-error --fail -L -o glauth https://github.com/glauth/glauth/releases/download/v2.3.0/glauth-linux-amd64; \
    chmod +x glauth;

COPY tarantool/docker/ci-log-section /usr/bin/ci-log-section
