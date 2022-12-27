FROM rockylinux:8

RUN curl -L https://tarantool.io/release/2/installer.sh | bash \
    && dnf install -y iputils bind-utils wget tarantool

RUN groupadd -g 1000 picouser \
    && useradd -u 1000 -g 1000 picouser

USER 1000:1000

WORKDIR /home/picouser
