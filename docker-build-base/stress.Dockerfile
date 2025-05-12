ARG BASE_IMAGE_TAG

FROM ${BASE_IMAGE}:${BASE_IMAGE_TAG}

ENV STRESS_TEST=""
COPY docker-build-base/entrypoint-stress.sh sbroad/Makefile /sbroad/
COPY sbroad/ /sbroad/
WORKDIR /sbroad

RUN dnf install -y tree
RUN tree -L 3
RUN make build_integration

ENTRYPOINT ["/sbroad/entrypoint-stress.sh"]
HEALTHCHECK --interval=30s --timeout=10s --retries=20 \
 CMD cat /etc/OK || exit 1