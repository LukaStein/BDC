#!/bin/bash:

FILE_PATH=$1
CORES=$2


awk 'NR % 4 == 0' $FILE_PATH | parallel --sshloginfile parallel_ssh -j $CORES --pipe python3 Kwartaal_12/BDC/Assignment3/assignment3.py
