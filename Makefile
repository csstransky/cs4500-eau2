all: build run

.PHONY: FORCE

build: FORCE
	@mkdir -p build
	@cd build && cmake .. && make

run: build
	build/src/application/trival

test: build
	build/tests/serial_examples
	build/tests/test_serial
	build/tests/test_sorer
	build/tests/test-array
	build/tests/test-map
	build/tests/dataframe/basic_example
	build/tests/dataframe/parallel_map_personal_test_suite
	build/tests/test_kv_store
	build/tests/test_kd_store
	build/tests/test_application

valgrind:
	valgrind build/tests/test_serial
	valgrind build/tests/test_sorer
	valgrind build/tests/test-array
	valgrind build/tests/test-map
	valgrind build/tests/test_kv_store
	valgrind build/tests/test_kd_store
	valgrind build/tests/test_application
	valgrind build/src/application/trival

clean:
	rm -rf build