""" Script that calculates the average PHRED score per position i.e. column of
a FastQ file en returns a VCF file with each position separated from its mean
score. """

# IMPORTS
import argparse as ap
import csv
import multiprocessing as mp
import os
from collections import defaultdict

def process_prompt_params():
    """
    Using argparse enables this script to receive
    parameters such as number of CPUs, fastqfile.fastq (or more in a list) and
    optionally the name of the output file; otherwise presented with STDOUT to
    terminal.

    param: -n <number_cpus> param: -o <output csv file> param: fastqfile1.fastq
    (no keyword required i.e. positional argument) -> can also be a list with
    several files [fastqfile1.fastq fastqfile2,fastq]

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
        help="Number of cores to \
                      use",
    )
    argparser.add_argument(
        "-o",
        action="store",
        dest="csvfile",
        type=ap.FileType("w", encoding="UTF-8"),
        required=False,
        help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")

    argparser.add_argument(
        "fastq_files",
        action="store",
        nargs="+",
        help="At least 1 processable Illumina Fastq Format file",
    )
    return argparser.parse_args()


def slice_chunks(filename, nchunks):
    """
    Determine number of lines to be in a chunk,
    but assure that the number of lines is divisible by 4.
    (for full reads)
    :param
    """
    # Calculate the total number of lines in the file
    count = count_lines_in_file(filename)

    # Calculate the number of lines per chunk
    # Each chunk should have a length % 4 == 0
    lines_per_chunk = count // nchunks

    # Adjust lines_per_chunk to be a multiple of 4
    if lines_per_chunk % 4 != 0:
        lines_per_chunk += 4 - (lines_per_chunk % 4)

    with open(filename, 'rb') as file:
        for _ in range(nchunks):
            chunk_lines = []
            for _ in range(lines_per_chunk):
                line = file.readline()
                if not line:
                    break
                if _ % 4 == 3:
                    chunk_lines.append(line)
            yield chunk_lines

def count_lines_in_file(filename):
    """
    Count the number of lines in the file, as fast as possible
    in binary mode and with a buffer to divide the reading.
    :param line: filename
    :param type: str
    :return: line_count
    """
    with open(filename, 'rb') as file:
        line_count = 0
        buffer_size = 1024 * 1024
        read_raw_file = file.raw.read
        buffer = read_raw_file(buffer_size) # start reading
        while buffer:
            line_count += buffer.count(b'\n')
            buffer = read_raw_file(buffer_size) # keep reading
        return line_count


def process_to_numeric(line: bytes):
    """
    Each byte is an 8-bit number (0-255),
    therefore extract each byte from the byte string (line)
    Substract 33 from the bytes, as they represent + 33 as quality score
    :param line: bytes, byte representation of each quality line
    :return: numerical representation of each quality line
    """
    return [byte - 33 for byte in line]


def convert_binary_to_phredscores(chunk):
    """
    Convert quality score bytes to integers.
    :param line: chunk, a slice of a FastQ file
    :return: average_phredscores, a list of dictionaries with column
    (or line) numbers and average scores.
    """
    numeric_values = defaultdict(list)
    # every new key gets a list!
    for line in range(len(chunk)):
        numeric_phredscores: list = process_to_numeric(chunk[line].strip())
        # each new read add scores to corresponding base_position
        for base_position, score in enumerate(numeric_phredscores):
            numeric_values[base_position].append(score)
    return numeric_values


def calculate_average_phredscores(all_phredscores: list):
    """
    Calculate the average phredscore per column.
    :param line: all_phredscores, a list with
    defaultdictionaries with for each base position a phredscore. The number of
    chunks equals to the number of defaultdictionaries.
    :return: A dictionary with for each base position (key)
    the average phredscore as value.
    """
    average_phredscores_per_base_position = defaultdict(list)
    # new key receives a list!

    for chunks_results in range(len(all_phredscores)):  # pylint: disable=C0200
        for base_position, phredscores in all_phredscores[chunks_results].items():
            average_phredscores_per_base_position[base_position].extend(phredscores)
            # merge lists of same base position to new dict
            # create list of dictionaries that is compatible with CSV module that hold
            # average phredscores per base position

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


def process_fastq_file(file_path, cpus):
    """
    Split file into chunks and process each chunk in parallel over n cpus.
    It attempts to create 10 chunks and adds the remaining chunk at the end of chunk list.
    :param line: file_path, string
    :param line: cpus, int
    :return: average phredscores
    """
    chunks = slice_chunks(file_path, 3)
    with mp.Pool(cpus) as pool:
        results = pool.map(convert_binary_to_phredscores, chunks)
    return calculate_average_phredscores(results)


def choose_output_format(phredscores, output_file: str|None = None):
    """
    Function to decide what to do with presenting the results to the user.
    Also, prints after deciding.
    :param line: output_file (filename), string
    """
    fieldnames = ["line_number", "average_phredscore"]
    if output_file:
        for filename, base_scores in phredscores.items():
            # output filenaam opbouwen volgens de opdracht 1 pagina
            if len(phredscores) == 1: out_file = output_file
            else:
                output_file_dir = os.path.dirname(output_file)
                fastq_file_name = os.path.basename(filename)
                output_file_extension = ".output.csv"
                out_file = os.path.join(output_file_dir, fastq_file_name + output_file_extension)
            with open(
                out_file, mode="w", encoding="UTF-8"
            ) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                # use csv object for writing to a csv file
                for base_average_score in range(
                    len(base_scores)
                ):  # pylint: disable=C0200
                    writer.writerow(base_scores[base_average_score])
            print(
                f"--Successfully written phredscores to {os.path.basename(filename)}.output.csv--"
            )
    else:
        # print instead of write
        for filename, base_scores in phredscores.items():
            if len(phredscores) > 1: print(os.path.basename(filename))
            for base_average_score in range(len(base_scores)):  # pylint: disable=C0200
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

    if not args.csvfile: # no output file specified, write to terminal
        choose_output_format(save_results)
    else:
        # make sure output file can be re-opened in Luka's function
        outfile_name = args.csvfile.name
        args.csvfile.close()
        choose_output_format(save_results, output_file=outfile_name)


    print("--End of results--")
