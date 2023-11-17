FROM python:3.10

ENV SITE_SRC '/site/src'
ENV SITE_HTML '/site/html'

RUN pip install \
    mkdocs==1.5.3 \
    mkdocs-material==9.4.8 \
    mkdocs-plugin-inline-svg==0.1.0

# Generate static content
COPY . ${SITE_SRC}

EXPOSE 8080
WORKDIR ${SITE_SRC}
CMD ["mkdocs", "serve"]
