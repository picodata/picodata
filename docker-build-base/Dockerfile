FROM centos:7

RUN set -e; \
    curl -L https://tarantool.io/release/2.8/installer.sh | bash; \
    yum -y install gcc git tarantool tarantool-devel unzip; \
    yum clean all;

ENV PATH=/root/.cargo/bin:${PATH}
RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
    sh -s -- -y --profile default --default-toolchain 1.58.0;

ENV PATH=/root/.rocks/bin:${PATH}
RUN set -e; \
    cd /root; \
    tarantoolctl rocks install luacheck 0.26.0; \
    tarantoolctl rocks install luatest 0.5.7; \


