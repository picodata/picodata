FROM python:3.12

RUN apt-get update && apt-get install -y chromium && rm -rf /var/cache/apt/archives /var/lib/apt/lists/*

ARG IMAGE_DIR=/builds/
WORKDIR $IMAGE_DIR

COPY docs $WORKDIR

ENV PIPENV_CUSTOM_VENV_NAME=docs
RUN pip install pipenv
RUN pipenv sync -d

CMD ["pipenv", "run", "mkdocs", "serve", "--dev-addr", "0.0.0.0:8000"]
