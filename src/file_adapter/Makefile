build:
	g++ -O3 -pthread -std=c++11 -o bench.out bench.cpp

run:
ifeq ("$(wildcard ./datafile.txt)","")
		git clone https://github.com/kaylindevchand/datafile.git
		unzip datafile/datafile.zip
		rm -rf datafile
endif
	./bench

clean:
	rm -rf bench.out datafile.txt