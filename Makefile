.PHONY: default fmt lint test check fat

default: ;

fmt:
	cargo fmt
	pipenv run fmt

lint:
	cargo fmt --check
	cargo check
	cargo clippy -- --deny clippy::all
	pipenv run lint

test:
	cargo test
	pipenv run pytest

check:
	@$(MAKE) lint --no-print-directory
	@$(MAKE) test --no-print-directory

fat:
	@$(MAKE) fmt --no-print-directory
	@$(MAKE) lint --no-print-directory
	@$(MAKE) test --no-print-directory
