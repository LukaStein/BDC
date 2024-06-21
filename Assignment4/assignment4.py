#!/usr/bin/env python3

"""
Calculate mean phredscores per base position.
"""

# IMPORTS
import argparse as ap
import csv
import numpy as np
from collections import defaultdict
import mmap
import os
import time
from mpi4py import MPI
from pathlib import Path


def parser():
    """
    Command line interface to present and receive arguments
    :return: argparser.parse_args(); contains arguments from user
    """
    argparser = ap.ArgumentParser(
        description="Script voor Opdracht 4 van Big Data Computing;  Calculate PHRED scores over the network."
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
        "fastq_files",
        action="store",
        type=Path,
        nargs="*",
        help="At least 1 processable \
                           Illumina Fastq Format file",
    )

    argparser.add_argument(
        "-c",
        action="store",
        dest="chunks",
        required=True,
        type=int,
        help="Number of chunks to distribute",
    )

    return argparser.parse_args()


class PhredscoreCalculator:
    """
    Calculates Phredscores in chunks from a FastQ file
    """
    def __init__(self, n_chunks):
        self.n_chunks = n_chunks

    def read_binary_chunk(self, filename, start, end):
        """
        Reads binary file and slice of a chunk (by substraction)
        param: filename, str
        param: start, int; start of chunk
        param: end, int, end of chunk
        :return: a binary chunk of the file
        """
        with open(filename, mode="rb") as binary_file:
            # Memory-map the file, size 0 means whole file, prevent loading entire chunks into memory
            mmapped_file = mmap.mmap(binary_file.fileno(), 0, access=mmap.ACCESS_READ)
            # "The Python File seek() method sets the file's cursor at a specified position in the current file"
            mmapped_file.seek(start)
            binary_chunk = mmapped_file.read(end - start)
            # therefore, slicing a piece at end - start
            mmapped_file.close()
        return binary_chunk

    def process_to_numeric(self, line: bytes):
        """
        Each byte is an 8-bit number (0-255), therefore extract each byte from the byte string (line)
        Substract 33 from the bytes, as they represent + 33 as quality score
        param: bytes, byte representation of each quality line
        :return: numerical representation of each quality line
        """
        return [byte - 33 for byte in line]  # numeric values

    def convert_binary_to_phredscores(self, chunk):
        """
        Convert quality score bytes to integers.
        param: chunk, a slice of a FastQ file
        :return: average_phredscores, a list of dictionaries with column (or line) numbers and average scores.
        """
        lines = chunk.split(b"\n")
        numeric_values = defaultdict(list)  # every new key gets a list!
        read_quality_line = False
        for line in lines:
            if read_quality_line:
                numeric_phredscores = self.process_to_numeric(line.strip())
                for base_position, score in enumerate(numeric_phredscores, start=0):
                    numeric_values[base_position].append(score)
                read_quality_line = False
            if line.startswith(b"+"):
                read_quality_line = True
        return numeric_values

    def calculate_average_phredscores(self, all_phredscores: dict):
        """
        Calculate the average phredscore per column.
        param: all_phredscores, a list with defaultdictionaries with for each base position a phredscore.
        The number of chunks equals to the number of defaultdictionaries.
        :return: A dictionary with for each base position (key) the average phredscore as value.
        """
        return {
            base_position: sum(scores) / len(scores)
            for base_position, scores in all_phredscores.items()
        }

    def process_chunk(self, file_chunk_info):
        """
        Call functions to read a chunk of file in binary mode and
        convert the bytes to numerical phredscores
        return positional_average_scores
        param: file_chunk_info, list of file path, start and end of chunk positions
        :return: numerical phredscores
        """
        file_path, start, end = file_chunk_info
        chunk = self.read_binary_chunk(file_path, start, end)
        numeric_values = self.convert_binary_to_phredscores(chunk)
        return file_path, self.calculate_average_phredscores(numeric_values)

    def determine_chunks(self, file_path):
        """
        Split file into chunks and process each chunk in parallel
 over n cpus.
        It attempts to create 10 chunks and adds the remaining ch
unk at the end of chunk list.
        param: file_path, string
        param: cpus, int
        :return: average phredscores
        """
        file_size = os.path.getsize(file_path)
        chunk_size = file_size // self.n_chunks
        # tries n chunks

        for i in range(self.n_chunks):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < self.n_chunks - 1 else (i + 1) * file_size
            yield file_path, start, end
        # first determine chunk_sizes and the last chunk will include the end of the file

def choose_output_format(phredscores, output_file, nfiles):
    """
    Function to decide what to do with presenting the results to the user.
    Also, prints after deciding.
    :param line: output_file (filename), string
    """
    fieldnames = ["line_number", "average_phredscore"]
    if output_file:
        BASENAME = output_file

        for filename, base_scores in phredscores.items():
            if nfiles > 1:
                BASENAME = f"{os.path.basename(filename)}.{output_file}"
            with open(BASENAME, mode="w", encoding="UTF-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                # use csv object for writing to a csv file
                for base_position, average_phredscore in base_scores.items():
                    writer.writerow({"line_number" : base_position, "average_phredscore": average_phredscore})
                print(
                    f"--Successfully written phredscores to {BASENAME}--"
                )
    else:
        # print instead of write
        for filename, base_scores in phredscores.items():
            print(os.path.basename(filename))
            for base_position, average_phredscores in base_scores.items():  # pylint: disable=C0200
                print(f"{base_position},{average_phredscores}")
            print("")

# Get arguments
args = parser()

comm = MPI.COMM_WORLD
comm_size = comm.Get_size()
rank = comm.Get_rank()
calculator = PhredscoreCalculator(args.chunks)
if rank == 0: # controller I am
    all_chunks = []
    for fastq_file in args.fastq_files:
        all_chunks.extend(list(calculator.determine_chunks(fastq_file)))
    # Distribute chunks over ncores, if 10 chunks / 2 cores, each core has 5 chunks
    distributed_chunks = np.array_split(all_chunks, comm_size)
else:
    distributed_chunks = None

# Send chunks of an array to different processes
scattered_chunks = comm.scatter(distributed_chunks, root=0)

results = []
for chunk in scattered_chunks:
    results.append(calculator.process_chunk(chunk))

gathered_results = comm.gather(results, root=0)

if rank == 0:
    file_results = defaultdict(lambda: defaultdict(list))
    for worker_results in gathered_results:
        for res_file, res_scores in worker_results:
            for base_position, avg_chunk_score in res_scores.items():
                file_results[res_file][base_position].append(avg_chunk_score)

    output_data = {
        file: calculator.calculate_average_phredscores(scores)
        for file, scores in file_results.items()
    }
    FILE = args.csvfile if hasattr(args, "csvfile") else None
    choose_output_format(output_data, FILE, len(args.fastq_files))