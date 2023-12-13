.PHONY: build default fmt init lint run

default: ;

build:
	mkdocs build -d site

fmt:
	black ci

init:
	python3 -m venv venv && \
	. venv/bin/activate && \
	pip install -r requirements.txt && \
	pip install -r ci/ci-requirements.txt

lint:
	flake8 ci && \
	black ci --check --diff && \
	mypy ci && \
	python ci/validation.py

run:
	mkdocs serve
