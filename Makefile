all: histogram

histogram.o: histogram.c
	mpicc  -g -O -I/root/mrmpi-17Sep13/src  -c  histogram.c

histogram: histogram.o
	mpic++ -g -O histogram.o /root/mrmpi-17Sep13/src/libmrmpi_mpicc.a -o histogram

clean:
	rm *.o histogram

test:
	mpirun histogram input
