all: build test

build: 
	mkdir -p build
	cd build && cmake .. && make

test: build
	build/serial_examples
	build/test_serial
	build/test_sorer
	build/test-array
	build/test-map
	build/test-pair
	build/dataframe/basic_example
	build/dataframe/parallel_map_personal_test_suite
	
valgrind: build
	valgrind build/serial_examples
	valgrind build/test_serial
	valgrind build/test_sorer
	valgrind build/test-array
	valgrind build/test-map
	valgrind build/test-pair

clean:
	rm -rf build/