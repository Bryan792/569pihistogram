all: histogram

histogram.o: histogram.cpp
	mpicc -g -O -I/root/mrmpi-17Sep13/src  -c  histogram.cpp

histogram: histogram.o
	mpic++ -g -O histogram.o /root/mrmpi-17Sep13/src/libmrmpi_mpicc.a -o histogram

clean:
	rm *.o histogram

test:
	mpirun histogram 10000.a

testd:
	mpirun -host rpi1,rpi2,rpi3,rpi4,rpi5 -n 5 -prefix /usr/local histogram 10000.a 10000.b

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
