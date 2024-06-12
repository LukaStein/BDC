import sys
import csv
from collections import defaultdict
import numpy as np
import argparse as ap

def process_prompt_params():
    """
    Using argparse enables this script to receive parameters
    such as number of CPUs, fastqfile.fastq (or more in a list)
    and optionally the name of the output file; otherwise presented
    with STDOUT to terminal.

    param: -n <number_cpus>
    param: -o <output csv file>
    param: fastqfile1.fastq (no keyword required i.e. positional argument)
    -> can also be a list with several files
    [fastqfile1.fastq fastqfile2,fastq]

    :return: args object containing all params as objects
    """
    argparser = ap.ArgumentParser(
        description="Script for assignment 1 \
                                  of Big Data Computing"
    )
    argparser.add_argument(
        "-o",
        action="store",
        dest="csvfile",
        required=False,
        help="CSV file to save the output to. \
                          Defaulted output to terminal STDOUT",
    )
    argparser.add_argument(
        "-calc",
        action='store_true',
        required=False,
        help="Option for making phredscore calculations \
                          Defaulted output to terminal STDOUT",
    )
    argparser.add_argument(
        "-mean",
        action='store_true',
        required=False,
        help="Option to calculate means across base positions \
                          Defaulted output to terminal STDOUT",
    )
    argparser.add_argument(
        "fastq_files",
        action="store",
        nargs="+",
        help="At least 1 processable \
                           Illumina Fastq Format file",
    )
    return argparser.parse_args()

def process_to_numeric(line: bytes):
    return [byte - 33 for byte in line]

def convert_binary_to_phredscores():
    for line in sys.stdin.buffer:
        if line.strip():
            numeric_phredscores = process_to_numeric(line.strip())
            print(*numeric_phredscores)

def calculate_averages():
    counts_per_basepair = []
    total_sums_per_basepair = []
    line_count = 0

    for line in sys.stdin:
        if line.strip():
            scores = [int(score) for score in line.split()]
            if not any(counts_per_basepair):
                counts_per_basepair = np.zeros(len(scores), dtype=int)
                total_sums_per_basepair = np.zeros(len(scores), dtype=int)
            counts_per_basepair += 1
            total_sums_per_basepair += scores
            line_count += 1
    means = total_sums_per_basepair / counts_per_basepair
    for i, mean in enumerate(means, start=1):
        print(f"{i},{mean}")

if __name__ == "__main__":
    args = process_prompt_params()
    if args.calc:
        convert_binary_to_phredscores()
        #process_chunk()
    if args.mean:
        calculate_averages()
