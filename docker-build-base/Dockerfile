FROM centos:7

RUN set -e; \
    curl -L https://tarantool.io/release/2.8/installer.sh | bash; \
    yum -y install gcc git tarantool tarantool-devel; \
    yum clean all;

ENV PATH=/root/.cargo/bin:${PATH}
RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
    sh -s -- -y --profile default --default-toolchain 1.57.0;
