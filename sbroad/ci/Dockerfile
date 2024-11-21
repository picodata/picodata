FROM docker-public.binary.picodata.io/tarantool:2.11.2.159

ENV PATH=/usr/local/bin:/root/.cargo/bin:${PATH}
ENV LD_LIBRARY_PATH=/usr/local/lib64:$LD_LIBRARY_PATH

RUN rm -f /etc/yum.repos.d/pg.repo && \
    dnf -y update && \
    dnf install -y git gcc gcc-c++ make cmake golang findutils && \
    mkdir -p $(go env GOPATH)/bin && \
    export PATH=$(go env GOPATH)/bin:$PATH && \
    git clone https://github.com/magefile/mage.git && \
    cd mage && go run bootstrap.go && cd .. && rm -rf mage && \
    git clone https://github.com/tarantool/cartridge-cli.git && \
    cd cartridge-cli && git checkout 2.10.0 && \
    mage build && mv ./cartridge /usr/local/bin && cd .. && rm -rf cartridge-cli && \
    dnf install -y openssl-devel readline-devel libicu-devel && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain=1.76.0 -y --profile default && \
    rustup component add rustfmt && \
    cargo install cargo-audit
