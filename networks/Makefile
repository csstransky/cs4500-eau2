build:
	g++ -g rendezvous_server.cpp -o rserver
	g++ -g node.cpp -o node

run:
	# Running in background. Will take 100 secs
	# Printing server output
	@sleep 1
	(./rserver -ip 127.0.0.1)&
	@sleep 1
	(./node -ip 127.0.0.9 > node1.txt)&
	@sleep 1
	(./node -ip 127.0.0.8 > node2.txt)&
	@sleep 1
	(./node -ip 127.0.0.7 > node3.txt)&
	@sleep 100
	# Node output is in node1.txt, node2.txt, node3.txt
	
clean:
	rm -rf rserver node *.txt