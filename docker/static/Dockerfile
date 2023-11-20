FROM python:3.10

ENV SITE_SRC '/site/src'

# Generate static content
COPY . ${SITE_SRC}

RUN pip install -r ${SITE_SRC}/requirements.txt && \
    pip install -r ${SITE_SRC}/ci/ci-requirements.txt

EXPOSE 8080
WORKDIR ${SITE_SRC}
CMD ["mkdocs", "serve"]
