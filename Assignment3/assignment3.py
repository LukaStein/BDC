"""
Script that calculates mean phredscores per base position from a chunk of binary phredscore lines.
Output is send to a CSV file
"""

# IMPORTS
import sys
import argparse as ap
import numpy as np


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
        action="store_true",
        required=False,
        help="Option for making phredscore calculations \
                          Defaulted output to terminal STDOUT",
    )
    argparser.add_argument(
        "-mean",
        action="store_true",
        required=False,
        help="Option to calculate means across base positions \
                          Defaulted output to terminal STDOUT",
    )
    argparser.add_argument(
        "-o",
        action="store",
        dest="nfiles",
        required=False,
        type=int,
        help="Number of fastq_files to decide what the output CSV filename(s) will be \
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
    """
    Determine phredscore from bytes
    :param line: line of phredscore bytes
    :type line: bytes
    :return: list of phredscores
    """
    return [byte - 33 for byte in line]


def convert_binary_to_phredscores():
    """
    Process binary phredscore line to numerical phredscores
    and send scores to STDOUT
    """
    for line in sys.stdin.buffer:
        if line.strip():
            numeric_phredscores = process_to_numeric(line.strip())
            print(*numeric_phredscores)


def calculate_averages(filename, counts):
    """
    Calculate mean phredscores for each base position from all phredscore lines.
    :param line: filename, name of fastq file
    :type line: str
    :param line: counts, number of fastq_files
    :type line: int
    """
    counts_per_basepair = []
    total_sums_per_basepair = []
    output_filename = "output.csv" if counts == 1 else f"{filename}.output.csv"

    for line in sys.stdin:
        if line.strip():
            scores = [int(score) for score in line.split()]
            if not any(counts_per_basepair):
                counts_per_basepair = np.zeros(len(scores), dtype=int)
                total_sums_per_basepair = np.zeros(len(scores), dtype=int)
            counts_per_basepair += 1
            total_sums_per_basepair += scores

    means = total_sums_per_basepair / counts_per_basepair
    with open(output_filename, mode="w", encoding="UTF-8") as csvfile:
        for i, mean in enumerate(means, start=1):
            csvfile.write(f"{i},{mean}\n")


def main():
    """
    Call all functionality
    """
    args = process_prompt_params()
    number_files = args.nfiles if hasattr(args, "nfiles") else None

    if args.filename:
        if args.calc:
            convert_binary_to_phredscores()
        if args.mean:
            calculate_averages(args.filename, number_files)


if __name__ == "__main__":
    main()
