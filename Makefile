all: build test valgrind

build:
	cd tests && make build

test:
	cd tests && make

valgrind:
	cd tests && make valgrind