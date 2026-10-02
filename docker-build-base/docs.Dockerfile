FROM python:3.12

RUN apt-get update && apt-get install -y chromium && rm -rf /var/cache/apt/archives /var/lib/apt/lists/*

ENV PIPENV_CUSTOM_VENV_NAME=docs
# The official pypi.org repository is being blocked.
ENV PIP_INDEX_URL=https://binary.picodata.io/repository/PyPi/simple/
ENV PIPENV_PYPI_MIRROR=https://binary.picodata.io/repository/PyPi/simple/
RUN pip install pipenv

ARG IMAGE_DIR=/builds/
WORKDIR $IMAGE_DIR

# Install dependencies before copying the rest of the docs,
# so that docs changes do not invalidate this layer.
COPY docs/Pipfile docs/Pipfile.lock ./
RUN pipenv sync -d

COPY docs ./

CMD ["pipenv", "run", "mkdocs", "serve", "--dev-addr", "0.0.0.0:8000"]
