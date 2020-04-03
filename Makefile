all: build

.PHONY: FORCE

build: FORCE
	@mkdir -p build
	@cd build && cmake .. && make

trivial: build
	(build/src/rserver -ip 127.0.0.1)&
	@sleep 1
	build/src/trivial -ip 127.0.0.2 -s 127.0.0.1

demo: build
	(build/src/rserver -ip 127.0.0.1)&
	@sleep 1
	(build/src/demo -ip 127.0.0.2 -s 127.0.0.1 0)&
	(build/src/demo -ip 127.0.0.3 -s 127.0.0.1 1)&
	build/src/demo -ip 127.0.0.4 -s 127.0.0.1 2

test: build
	build/tests/test_serial
	build/tests/test_sorer
	build/tests/test-array
	build/tests/test-map
	build/tests/test_dataframe
	build/tests/test_kv_store
	build/tests/test_kd_store
	build/tests/test_application
	build/tests/test_networking

valgrind: build
	valgrind build/tests/test_serial
	valgrind build/tests/test_sorer
	valgrind build/tests/test-array
	valgrind build/tests/test-map
	valgrind build/tests/test_kv_store
	valgrind build/tests/test_kd_store
	valgrind build/tests/test_application
	valgrind build/tests/test_dataframe
	valgrind build/tests/test_networking

clean:
	rm -rf build