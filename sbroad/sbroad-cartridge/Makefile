OS := $(shell uname -s)
ifeq ($(OS), Linux)
	SRC_LIB = libsbroad_cartridge.so
	DEST_LIB = libsbroad.so
else
	ifeq ($(OS), Darwin)
		SRC_LIB = libsbroad_cartridge.dylib
		DEST_LIB = libsbroad.dylib
	endif
endif

TARGET_ROOT?=../target
CARTRIDGE_MODULE?=.
CORE_MODULE=$(CARTRIDGE_MODULE)/../sbroad-core
TEST_APP=$(CARTRIDGE_MODULE)/test_app
STRESS_TEST=$(CARTRIDGE_MODULE)/stress-test
K6_PATH=$(STRESS_TEST)/k6

build_cartridge_engine:
	cargo build -p sbroad-cartridge --release

build_cartridge_engine_debug:
	cargo build -p sbroad-cartridge

build_integration:
	cartridge build $(TEST_APP)

run_integration:
	cd $(TEST_APP) && rm -rf tmp/tarantool_configuration.log && TARANTOOL_LOG_LEVEL=5 TARANTOOL_LOG=tmp/tarantool.log ./.rocks/bin/luatest --coverage -v test/ && cd ..

# Run specific test_file, which name prefix is determined by TEST_FILE_PREFIX argument
run_integration_file:
	cd $(TEST_APP) && rm -rf tmp/tarantool_configuration.log && TARANTOOL_LOG_LEVEL=5 TARANTOOL_LOG=tmp/tarantool.log ./.rocks/bin/luatest --coverage -v test/integration/$(TEST_FILE_PREFIX)_test.lua && cd ..

# Run specific test pattern from specfic group, args are GROUP and PATTERN
run_integration_group_pattern:
	cd $(TEST_APP) && rm -rf tmp/tarantool_configuration.log && TARANTOOL_LOG_LEVEL=5 TARANTOOL_LOG=tmp/tarantool.log ./.rocks/bin/luatest --coverage -v $(GROUP) -p $(PATTERN) && cd ..

# Run specific test pattern, arg is PATTERN
run_integration_pattern:
	cd $(TEST_APP) && rm -rf tmp/tarantool_configuration.log && TARANTOOL_LOG_LEVEL=5 TARANTOOL_LOG=tmp/tarantool.log ./.rocks/bin/luatest --coverage -p $(PATTERN) && cd ..

install_debug:
	mkdir -p $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(TARGET_ROOT)/debug/$(SRC_LIB) $(LIBDIR)/$(DEST_LIB)
	cp -Rf $(CARTRIDGE_MODULE)/src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(CORE_MODULE)/src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(CARTRIDGE_MODULE)/cartridge $(LUADIR)

install_release:
	mkdir -p $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(TARGET_ROOT)/release/$(SRC_LIB) $(LIBDIR)/$(DEST_LIB)
	cp -Rf $(CARTRIDGE_MODULE)/src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(CORE_MODULE)/src/*.lua $(LUADIR)/$(PROJECT_NAME)
	cp -Rf $(CARTRIDGE_MODULE)/cartridge $(LUADIR)

test_integration:
	$(MAKE) build_integration && $(MAKE) run_integration

# Template: make test_file TEST_FILE_PREFIX=
test_file:
	$(MAKE) build_integration && $(MAKE) run_integration_file

# Template: make test_group_pattern GROUP= PATTERN=
test_group_pattern:
	$(MAKE) build_integration && $(MAKE) run_integration_group_pattern

# Template: make test_pattern PATTERN=
test_pattern:
	$(MAKE) build_integration && $(MAKE) run_integration_pattern

clean_integration:
	$(MAKE) stop
	rm -rf $(TEST_APP)/.rocks
	rm -rf $(TEST_APP)/tmp

setup:
	cd $(TEST_APP) && cartridge replicasets setup --bootstrap-vshard && cd ..

start:
	$(MAKE) clean_integration
	$(MAKE) build_integration && cd $(TEST_APP) && cartridge start -d
	$(MAKE) setup

stop:
	cd $(TEST_APP) && cartridge stop &&  while  cartridge status 2>&1 | grep "RUNNING" -q; do sleep 1; done

stress_start:
	cd $(TEST_APP) && cartridge start -d
	$(MAKE) setup

stress_init:
	@if [ -z "${test}" ]; then echo "test variable must be set"; exit 1; fi
	tarantool $(CARTRIDGE_MODULE)/stress-test/$(test)/init.lua 1000

stress_local:
	@if [ -z "${test}" ]; then echo "test variable must be set"; exit 1; fi
	$(MAKE) start
	$(MAKE) stress_init test=$(test)
	$(K6_PATH) run -u 10 -d 1m $(STRESS_TEST)/$(test)/k6.js --summary-export $(STRESS_TEST)/$(test)/k6_summary_local.json
	$(MAKE) stop

stress_all_local:
	$(MAKE) stress_local test=projection
	$(MAKE) stress_local test=projection_wide
	$(MAKE) stress_local test=insert
	$(MAKE) stress_local test=groupby
