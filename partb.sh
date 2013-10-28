#! /bin/sh
mpirun -host rpi1,rpi2,rpi3,rpi4,rpi5 -n 5 -prefix /usr/local histogram $1 $1

#if [ ! -s hist.c ]; then
if [[ $? -ne 0 ]]; then

up=('rpi1')

[ "$(ping -q -c1 rpi2)" ] && up=("${up[@]}" "rpi2")
[ "$(ping -q -c1 rpi3)" ] && up=("${up[@]}" "rpi3")
[ "$(ping -q -c1 rpi4)" ] && up=("${up[@]}" "rpi4")
[ "$(ping -q -c1 rpi5)" ] && up=("${up[@]}" "rpi5")

string=${up[@]}
	mpirun -host ${string// /,} -prefix /usr/local histogram $1 $2 
fi
