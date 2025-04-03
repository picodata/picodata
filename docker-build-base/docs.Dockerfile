FROM python:3.12

ARG IMAGE_DIR=/builds/
WORKDIR $IMAGE_DIR

COPY docs $WORKDIR

ENV PIPENV_CUSTOM_VENV_NAME=docs
RUN pip install pipenv
RUN pipenv sync -d

CMD ["pipenv", "run", "mkdocs", "serve", "--dev-addr", "0.0.0.0:8000"]
