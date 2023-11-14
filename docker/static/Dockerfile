FROM python:3.10

ENV SITE_SRC '/site/src'
ENV SITE_HTML '/site/html'

RUN pip install mkdocs mkdocs-material==8.5.11 mkdocs-git-revision-date-localized-plugin==1.2.0 mkdocs-plugin-inline-svg mkdocs-macros-plugin

# Generate static content
COPY . ${SITE_SRC}

EXPOSE 8080
WORKDIR ${SITE_SRC}
CMD ["mkdocs", "serve"]
