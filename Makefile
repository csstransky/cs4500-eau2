all: build test valgrind

build:
	# Don't have anything to build yet

test:
	cd tests && make

valgrind:
	cd tests && make valgrind