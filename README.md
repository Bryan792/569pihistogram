569pihistogram
==============
This application takes in two files (both space separated float values) 
and calculates a histogram for each file. A vector sum is calculated 
and a histogram is produced for the vector sum. 

watchdog is an redundant implementation of this program. It works by 
constantly pinging all worker nodes. If a node is found to be unresponsive 
the master mpi process would be killed and restarted with the remaining 
online nodes. 

Required Libraries:
MR-MPI
libmpi

Compiling:
make


