ARG REPO_URL=https://download.picodata.io
ARG PICODATA_VERSION

FROM debian:13 AS installer
ARG REPO_URL
ARG PICODATA_VERSION
RUN apt-get update && apt-get install -y --no-install-recommends curl gpg
RUN curl -s ${REPO_URL}/tarantool-picodata/picodata.gpg.key | \
    gpg --no-default-keyring \
        --keyring gnupg-ring:/etc/apt/trusted.gpg.d/picodata.gpg --import && \
    chmod 644 /etc/apt/trusted.gpg.d/picodata.gpg && \
    echo "deb [arch=$(dpkg --print-architecture)] ${REPO_URL}/tarantool-picodata/debian/ trixie main" \
    > /etc/apt/sources.list.d/picodata.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends picodata${PICODATA_VERSION:+=${PICODATA_VERSION}}

FROM docker-public.binary.picodata.io/distroless/cc-debian13

COPY --from=installer /usr/bin/picodata /usr/bin/picodata
COPY --from=installer /usr/share/picodata /usr/share/picodata
COPY docker/config.yaml /etc/picodata/config.yaml

WORKDIR /var/lib/picodata

ENTRYPOINT ["/usr/bin/picodata"]
CMD ["run", "--config", "/etc/picodata/config.yaml"]
