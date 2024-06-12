#!/bin/bash

# Locate the location of this bash script, and assumely the python script
LOCATE_DIR=$(dirname -- "$( readlink -f -- "$0"; )")

FILE_PATH=$1
CORES=$2


awk 'NR % 4 == 0' $FILE_PATH | parallel --pipe --sshloginfile parallel_ssh --block 5M --memfree 1G -j $CORES python3 ${LOCATE_DIR}/assignment3.py -calc {} | python3 ${LOCATE_DIR}/assignment3.py -mean {}
