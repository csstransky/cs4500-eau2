all: build run

build:
	clang++ -std=c++11 -Wall -g -o test_sor.out test_sor.cpp -v

run:
	./test_sor.out

clean:
	rm -rf bench.out test_sor.out datafile.txt sorer.out