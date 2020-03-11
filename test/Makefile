all: test

test:
	mkdir -p build
	cd build && cmake .. && make
	build/serial_examples
	build/test_serial
	build/test_sorer
	build/test-array
	build/test-map
	build/test-pair
	build/dataframe/basic_example
	build/dataframe/parallel_map_personal_test_suite
