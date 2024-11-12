ARG BASE_IMAGE_TAG

FROM docker-public.binary.picodata.io/sbroad-builder:${BASE_IMAGE_TAG}

ENV STRESS_TEST=""
COPY docker-build-base/entrypoint-stress.sh sbroad/Makefile /sbroad/
COPY sbroad/ /sbroad/
WORKDIR /sbroad

RUN make build_integration

ENTRYPOINT ["/sbroad/entrypoint-stress.sh"]
HEALTHCHECK --interval=30s --timeout=10s --retries=20 \
 CMD cat /etc/OK || exit 1