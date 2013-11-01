all: histogram copy

histogram.o: histogram.cpp
	mpicc -g -O -I/root/mrmpi-17Sep13/src  -c  histogram.cpp

histogram: histogram.o
	mpic++ -g -O histogram.o /root/mrmpi-17Sep13/src/libmrmpi_mpicc.a -o histogram


histogram.bryan.o:
	mpicc -g -O -I/root/mrmpi-17Sep13/src  -c  histogram.bryan.cpp

histogramb: histogram.bryan.o
	mpic++ -g -O histogram.bryan.o /root/mrmpi-17Sep13/src/libmrmpi_mpicc.a -o histogramb

clean:
	rm *.o histogram hist.a hist.b hist.c result.out

test:
	mpirun histogram 10000.a

testip:
	mpirun -host 192.168.1.149,192.168.1.136,192.168.1.100,192.168.1.140,192.168.1.129 -n 5 -prefix /usr/local histogram ${ARGS}

testd:
	mpirun -host rpi1,rpi2,rpi3,rpi4,rpi5 -n 5 -prefix /usr/local histogram ${ARGS} 

testd10k:
	mpirun -host rpi1,rpi2,rpi3,rpi4,rpi5 -n 5 -prefix /usr/local histogram 10000.a 10000.b

testd100k:
	mpirun -host rpi1,rpi2,rpi3,rpi4,rpi5 -n 5 -prefix /usr/local histogram 100000.a 100000.b

testd1m:
	mpirun -host rpi1,rpi2,rpi3,rpi4,rpi5 -n 5 -prefix /usr/local histogram 1000000.a 1000000.b

testd10m:
	mpirun -host rpi1,rpi2,rpi3,rpi4,rpi5 -n 5 -prefix /usr/local histogram 10000000.a 10000000.b

testj:
	mpirun -host rpi1,rpi2,rpi3,rpi4,rpi5 -n 5 -prefix /usr/local jhist 10000.a 10000.b 

copy: 
	scp histogram rpi2:/root/569pihistogram/
	scp histogram rpi3:/root/569pihistogram/
	scp histogram rpi4:/root/569pihistogram/
	scp histogram rpi5:/root/569pihistogram/

copyj:
	scp histogram rpi2:/root/569pihistogram/
	scp histogram rpi3:/root/569pihistogram/
	scp histogram rpi4:/root/569pihistogram/
	scp histogram rpi5:/root/569pihistogram/

copyb:
	scp histogramb rpi2:/root/569pihistogram/
	scp histogramb rpi3:/root/569pihistogram/
	scp histogramb rpi4:/root/569pihistogram/
	scp histogramb rpi5:/root/569pihistogram/

partb:
	./partb.sh 10000.a 10000.b
