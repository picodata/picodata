.PHONY: build default fmt init lint run

default: ;

define enter_pipenv
	python3 -m venv venv && \
	. venv/bin/activate && $(1)
endef

build: init
	@$(call enter_pipenv, \
		mkdocs build -d site \
	)

fmt:
	@$(call enter_pipenv, \
		black ci \
	)

init:
	@$(call enter_pipenv, \
		pip install -r requirements.txt && \
		pip install -r ci/ci-requirements.txt \
	)

lint: init
	@$(call enter_pipenv, \
		flake8 --max-line-length 99 ci && \
		black ci --check --diff && \
		mypy ci && \
		python ci/validation.py \
	)

run: init
	@$(call enter_pipenv, \
		mkdocs serve \
	)
