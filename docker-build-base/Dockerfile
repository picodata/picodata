FROM centos:7

RUN curl -L https://tarantool.io/release/2.8/installer.sh | bash

RUN set -e; \
    yum -y install centos-release-scl; \
    yum -y groupinstall "Development Tools"; \
    yum-config-manager --enable rhel-server-rhscl-7-rpms; \
    yum -y install \
        devtoolset-7-gcc \
        devtoolset-7-gcc-c++ \
        git \
        tarantool \
        unzip \
        cmake3 \
        wget \
        zlib-devel \
        expat-devel \
        openssl11-devel \
        openssl11-libs \
        libffi-devel \
        sqlite-devel \
        ;\
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

################ Python ###############################################################
ENV GPG_KEY A035C8C19219BA821ECEA86B64E628F8D684696D
ENV PYTHON_VERSION 3.10.4

RUN set -e; \
    wget -q  -O python.tar.xz "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz"; \
    wget -q -O python.tar.xz.asc "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz.asc"; \
    GNUPGHOME="$(mktemp -d)"; export GNUPGHOME; \
    gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys "$GPG_KEY"; \
    gpg --batch --verify python.tar.xz.asc python.tar.xz; \
    command -v gpgconf > /dev/null && gpgconf --kill all || :; \
    rm -rf "$GNUPGHOME" python.tar.xz.asc; \
    mkdir -p /usr/src/python; \
    tar --extract --directory /usr/src/python --strip-components=1 --file python.tar.xz; \
    rm python.tar.xz; \
    \
    cd /usr/src/python; \
    gnuArch="$(arch)"; \
    # https://bugs.python.org/issue44319
    sed -i 's/PKG_CONFIG openssl /PKG_CONFIG openssl11 /g' configure;  \
    ./configure \
    --build="$gnuArch" \
    --enable-loadable-sqlite-extensions \
    --enable-option-checking=fatal \
    --enable-shared \
    --with-lto \
    --with-system-expat \
    --without-ensurepip \
    ; \
    nproc="$(nproc)"; \
    make -j "$nproc" \
    LDFLAGS="-Wl,--strip-all" \
    ; \
    make altinstall; \
    \
    cd /; \
    rm -rf /usr/src/python; \
    \
    find /usr/local -depth \
    \( \
    \( -type d -a \( -name test -o -name tests -o -name idle_test \) \) \
    -o \( -type f -a \( -name '*.pyc' -o -name '*.pyo' -o -name 'libpython*.a' \) \) \
    \) -exec rm -rf '{}' + \
    ; \
    \
    ldconfig; \
    yum clean all

ENV LD_LIBRARY_PATH=/usr/local/lib

# if this is called "PIP_VERSION", pip explodes with "ValueError: invalid truth value '<VERSION>'"
ENV PYTHON_PIP_VERSION 22.0.4
# https://github.com/docker-library/python/issues/365
ENV PYTHON_SETUPTOOLS_VERSION 58.1.0
# https://github.com/pypa/get-pip
ENV PYTHON_GET_PIP_URL https://github.com/pypa/get-pip/raw/38e54e5de07c66e875c11a1ebbdb938854625dd8/public/get-pip.py
ENV PYTHON_GET_PIP_SHA256 e235c437e5c7d7524fbce3880ca39b917a73dc565e0c813465b7a7a329bb279a

RUN set -eux; \
    \
    wget -q -O get-pip.py "$PYTHON_GET_PIP_URL"; \
    echo "$PYTHON_GET_PIP_SHA256 *get-pip.py" | sha256sum -c -; \
    \
    export PYTHONDONTWRITEBYTECODE=1; \
    \
    python3.10 get-pip.py \
    --disable-pip-version-check \
    --no-cache-dir \
    --no-compile \
    "pip==$PYTHON_PIP_VERSION" \
    "setuptools==$PYTHON_SETUPTOOLS_VERSION" \
    ; \
    rm -f get-pip.py; \
    \
    pip --version


# install Pipenv
COPY requirements-pipenv.txt /tmp/requirements-pipenv.txt
RUN PIP_NO_CACHE_DIR=true python3.10 -m pip install -q --require-hashes -r /tmp/requirements-pipenv.txt \
    && rm /tmp/requirements-pipenv.txt

################ END Python ###############################################################
