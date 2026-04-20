YARN_VERSION = 4.13.0
YARN_FLAGS = --immutable

# XXX: we override this variable in `build.rs`.
OUT_DIR = dist


.PHONY: all
all: build

# XXX: this rule is mostly useful for repo maintenance.
.PHONY: yarn-set-version
yarn-set-version: export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
yarn-set-version:
	yarn set version $(YARN_VERSION)

.PHONY: yarn-install
yarn-install: export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
yarn-install:
	yarn install $(YARN_FLAGS)

.PHONY: build
build: yarn-install
	yarn vite build \
		--emptyOutDir \
		--outDir $(OUT_DIR)

.PHONY: clean
clean:
	rm -rf node_modules dist dist-ssr

# Useful to keep in mind while debugging the setup.
.PHONY: corepack-cache-clean
corepack-cache-clean:
	corepack cache clean
