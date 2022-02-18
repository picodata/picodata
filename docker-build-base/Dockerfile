FROM centos:7

RUN set -e; \
    curl -L https://tarantool.io/release/2.8/installer.sh | bash; \
    yum -y install centos-release-scl; \
    yum-config-manager --enable rhel-server-rhscl-7-rpms; \
    yum -y install devtoolset-7-gcc devtoolset-7-gcc-c++ git tarantool unzip cmake3; \
    yum clean all; \
    ln -s /usr/bin/cmake3 /usr/bin/cmake;

ENV PATH=/opt/rh/devtoolset-7/root/usr/bin:${PATH}

ENV PATH=/root/.cargo/bin:${PATH}
RUN set -e; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
    sh -s -- -y --profile default --default-toolchain 1.58.0;

ENV PATH=/root/.rocks/bin:${PATH}
RUN set -e; \
    cd /root; \
    tarantoolctl rocks install luacheck 0.26.0; \
    tarantoolctl rocks install luatest 0.5.7;
