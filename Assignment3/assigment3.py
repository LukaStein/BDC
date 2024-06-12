import sys
import csv
from collections import defaultdict
import numpy as np
import argparse as ap

def process_prompt_params():
    """
    Using argparse enables this script to receive parameters
    such as number of CPUs, fastqfile.fastq

    param: -calc # used to invoke the calculation step
    param: -mean # used to invoke the mean calc step
    param: -filename fastqfile1.fastq # will be printed above the output

    :return: args object containing all params as objects
    """
    argparser = ap.ArgumentParser(
        description="Script for assignment 1 \
                                  of Big Data Computing"
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
        "-filename",
        action="store",
        required=True,
        help="1 Illumina .fastq Format filename",
    )
    argparser.add_argument(
            "fastq_files",
            action="store",
            nargs="*",
            help="At least 1 processable Illumina Fastq Format file. \
            Acts as placeholder for the incoming data",
    )
    return argparser.parse_args()

def process_to_numeric(line: bytes):
    return [byte - 33 for byte in line]

def convert_binary_to_phredscores():
    for line in sys.stdin.buffer:
        if line.strip():
            numeric_phredscores = process_to_numeric(line.strip())
            print(*numeric_phredscores)

def calculate_averages(filename):
    counts_per_basepair = []
    total_sums_per_basepair = []

    for line in sys.stdin:
        if line.strip():
            scores = [int(score) for score in line.split()]
            if not any(counts_per_basepair):
                counts_per_basepair = np.zeros(len(scores), dtype=int)
                total_sums_per_basepair = np.zeros(len(scores), dtype=int)
            counts_per_basepair += 1
            total_sums_per_basepair += scores

    means = total_sums_per_basepair / counts_per_basepair
    print(f"{filename}")
    for i, mean in enumerate(means, start=1):
        print(f"{i},{mean}")
    print('')

if __name__ == "__main__":
    args = process_prompt_params()
    if args.filename:
        if args.calc:
            convert_binary_to_phredscores()
        if args.mean:
            calculate_averages(args.filename)
