"""
Script that calculates the average PHRED score per position i.e. column of a FastQ file en returns a VCF file with each position separated from its mean score.
"""

# IMPORTS
import argparse as ap
import csv
import multiprocessing as mp
import os
from collections import defaultdict


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
        "-n",
        action="store",
        dest="n",
        required=True,
        type=int,
        help="Number of cores to use",
    )
    argparser.add_argument(
        "-o",
        action="store_true",
        required=False,
        help="CSV file option to save the output to. \
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


def read_binary_chunk(filename, start, end):
    """
    Reads binary file and slice of a chunk (by substraction)
    param: filename, str
    param: start, int; start of chunk
    param: end, int, end of chunk
    :return: a binary chunk of the file
    """
    with open(filename, mode="rb") as binary_file:
        # "The Python File seek() method sets the file's cursor at a specified position in the current file"
        binary_file.seek(start)
        binary_chunk = binary_file.read(
            end - start
        )  # therefore, slicing a piece at end - start
    return binary_chunk


def process_to_numeric(line: bytes):
    """
    Each byte is an 8-bit number (0-255), therefore extract each byte from the byte string (line)
    Substract 33 from the bytes, as they represent + 33 as quality score
    param: bytes, byte representation of each quality line
    :return: numerical representation of each quality line
    """
    return [byte - 33 for byte in line]  # numeric values


def convert_binary_to_phredscores(chunk):
    """
    Convert quality score bytes to integers.
    param: chunk, a slice of a FastQ file
    :return: average_phredscores, a list of dictionaries with column (or line) numbers and average scores.
    """
    mv = memoryview(chunk)
    lines = mv.tobytes().split(b"\n")
    #lines = chunk.split(b"\n")
    # Filter empty lines at end of chunks
    lines = [line for line in lines if line]
    numeric_values = defaultdict(list)  # every new key gets a list!
    for index, line in enumerate(lines, start=0):
        if index % 4 == 3:  # select fourth line additivel
            numeric_phredscores: list = process_to_numeric(line.strip())
            for base_position, score in enumerate(numeric_phredscores, start=0):
                numeric_values[base_position].append(
                    score
                )  # each new read add scores to corresponding base_position
    return numeric_values


def calculate_average_phredscores(all_phredscores: list):
    """
    Calculate the average phredscore per column.
    param: all_phredscores, a list with defaultdictionaries with for each base position a phredscore.
    The number of chunks equals to the number of defaultdictionaries.
    :return: A dictionary with for each base position (key) the average phredscore as value.
    """
    average_phredscores_per_base_position = defaultdict(
        list
    )  # new key receives a list!

    for chunks_results in range(len(all_phredscores)): # pylint: disable=C0200
        for (
            base_position,
            phredscores,
        ) in all_phredscores[
            chunks_results
        ].items():  # type: int type: list
            average_phredscores_per_base_position[base_position].extend(
                phredscores
            )  # merge lists of same base position to new dict
    # create list of dictionaries that is compatible with CSV module that hold average phredscores per base position

    average_phredscores = [
        {
            "line_number": base_position,
            "average_phredscore": sum(phredscores) / len(phredscores),
        }
        for base_position, phredscores in sorted(
            average_phredscores_per_base_position.items()
        )
    ]
    return average_phredscores


def process_chunk(file_chunk_info):
    """
    Call functions to read a chunk of file in binary mode and
    convert the bytes to numerical phredscores
    return positional_average_scores
    param: file_chunk_info, list of file path, start and end of chunk positions
    :return: numerical phredscores
    """
    file_path, start, end = file_chunk_info
    chunk = read_binary_chunk(file_path, start, end)
    return convert_binary_to_phredscores(chunk)


def check_read_completeness(file_path, start, end):
    """
    Adjust the start and end positions to align with read boundaries.
    param: file_path, str
    param: start, str; starting position of chunk
    param: end, str: end position of chunk
    """
    with open(file_path, "rb") as binary_file:
        if start != 0:  # If start is not byte 0, jump to the start of the new chunk
            binary_file.seek(start)  # set the file's cursor at start position
            # Move to the start of the next read
            while True:
                line = binary_file.readline()
                if not line or line.startswith(
                    b"@"
                ):  # stop if end is reached or stop before a new read (preventing overlap between chunks)
                    break  # breaks the true loop
            # Jump to the beginning of the first read, to ensure the starting boundary
            start = binary_file.tell() - len(
                line
            )  # after reading a line tell() returns the current position of the file pointer

        # Adjust end position to the end of the current chunk and only start if not at end of original file
        if end != os.path.getsize(file_path):
            binary_file.seek(end)  # set the file's cursor at start position
            # Move to the end of the current read
            while True:
                line = binary_file.readline()
                if not line or line.startswith(
                    b"@"
                ):  # stop if end is reached or stop before a new read (preventing overlap between chunks)
                    end = binary_file.tell() - len(
                        line
                    )  # Jump to beginning of last line
                    break  # breaks the true loop

    return file_path, start, end


def process_fastq_file(file_path, cpus):
    """
    Split file into chunks and process each chunk in parallel over n cpus.
    It attempts to create 10 chunks and adds the remaining chunk at the end of chunk list.
    param: file_path, string
    param: cpus, int
    :return: average phredscores
    """
    file_size = os.path.getsize(file_path)
    n_chunks = 10
    chunk_size = file_size // n_chunks  # tries 10 chunks
    uneven_chunk = file_size % n_chunks # determines remaining number of chunks possible

    # tuples of (filename, start e.g. = chunk_size * 0 = 0, end = 0 + 1 * chunk_size = chunk_size or file_size).
    # file_size is only chosen as last in the iteration

    # add uneven_chunk at end of chunks list, for end the uneven_chunk is added to the largest chunk to reach the max or file_size
    # First try all iterations in n_chunks with n_chunks -1. Finally, get the last chunk with i + 1
    chunks = [
        check_read_completeness(
            file_path,
            i * chunk_size,
            (i + 1) * chunk_size
            if i < n_chunks - 1
            else (i + 1) * chunk_size + uneven_chunk,
        )
        for i in range(n_chunks)
    ]

    with mp.Pool(cpus) as pool:
        results = pool.map(process_chunk, chunks)  # chunks is an iterable
        # , process_chunk_wrapper is invoked n = length(chunk) times

        # for each chunk a dicionary with positions as keys and scores from all reads in that chunk as values
        # e.g. 0 : [19, 19, 25, 24] the length of four is four reads on base position
    return calculate_average_phredscores(results)


def choose_output_format(phredscores, output_file):
    """
    Function to decide what to do with presenting the results
    to the user. Also, prints after deciding.
    param: output_file : string
    """
    fieldnames = ["line_number", "average_phredscore"]
    if output_file:
        for filename, base_scores in phredscores.items():
            with open(f"{os.path.basename(filename)}.output.csv", mode="w", encoding="UTF-8") as csvfile:
                writer = csv.DictWriter(
                    csvfile, fieldnames=fieldnames
                )  # use csv object for writing to a csv file
                for base_average_score in range(len(base_scores)): # pylint: disable=C0200
                    writer.writerow(base_scores[base_average_score])
            print(f"--Successfully written phredscores to {os.path.basename(filename)}.output.csv--")
    else:  # print instead of write
        for filename, base_scores in phredscores.items():
            print(os.path.basename(filename))
            for base_average_score in range(len(base_scores)): # pylint: disable=C0200
                print(
                    f"{base_scores[base_average_score]['line_number']},{base_scores[base_average_score]['average_phredscore']}"
                )
            print("")


if __name__ == "__main__":
    args = process_prompt_params()

    save_results = {}
    for fastq_file in args.fastq_files:
        average_phred_scores = process_fastq_file(fastq_file, args.n)
        save_results[fastq_file] = average_phred_scores

    FILE = True if args.o else False
    choose_output_format(save_results, output_file=FILE)
    print("--End of results--")
