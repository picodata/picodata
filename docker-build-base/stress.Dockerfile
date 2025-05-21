ARG BASE_IMAGE_TAG

FROM ${BASE_IMAGE}:${BASE_IMAGE_TAG}

ENV STRESS_TEST=""
COPY . .
COPY docker-build-base/entrypoint-stress.sh sbroad/
WORKDIR sbroad

RUN make build_integration

ENTRYPOINT ["/opt/tarantool/sbroad/entrypoint-stress.sh"]
HEALTHCHECK --interval=30s --timeout=10s --retries=20 \
 CMD cat /etc/OK || exit 1