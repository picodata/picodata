.PHONY: default lint test

default: ;

lint:
	cargo fmt --check
	cargo check
	cargo clippy -- --deny clippy::all
	pipenv run lint

test:
	cargo test
	pipenv run pytest
