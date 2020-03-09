all: testArray testQueue testMap

testArray:
	g++ -O3 -Wall -std=c++11 -o array test-array.cpp
	./array

testMap:
	g++ -O3 -Wall -std=c++11 -o map test-map.cpp
	./map

testQueue:
	g++ -O3 -Wall -std=c++11 -o queue test-queue.cpp
	./queue

clean:
	rm -rf array queue map