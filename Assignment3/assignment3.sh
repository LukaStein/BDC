#!/bin/bash

# Locate the location of this bash script, and assumely the python script
LOCATE_DIR=$(dirname -- "$( readlink -f -- "$0"; )")

CORES=$1
# Moves the next argument, which should be a fastq file to $1 of the parameter array.
shift

process_fastq_to_meanscores () {
  local fastq_file=$1
  local full_filename=$(basename "$fastq_file")
  awk 'NR % 4 == 0' "$fastq_file" | parallel --pipe --sshloginfile parallel_ssh --block 5M --memfree 1G -j "$CORES" python3 ${LOCATE_DIR}/assignment3.py -calc -filename "$full_filename" {} | python3 ${LOCATE_DIR}/assignment3.py -mean -filename "$full_filename" {}
}

# Make function accessible to be used in parallelization and variables accessible from within the function
export -f process_fastq_to_meanscores
export LOCATE_DIR
export CORES

# Invoke function parallel for all files in the arguments
parallel process_fastq_to_meanscores ::: "$@" 1> output.csv
