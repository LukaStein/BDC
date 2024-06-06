import sys
import csv
from collections import defaultdict

def process_to_numeric(line: bytes):
    return [byte - 33 for byte in line]

def convert_binary_to_phredscores(chunk):
    lines = chunk.split(b"\n")
    lines = [line for line in lines if line]
    all_basepositions_with_scores = []
    headers = []
    for index, line in enumerate(lines, start=0):
        numeric_phredscores = process_to_numeric(line.strip())
        headers.append(len(line))
        scores_per_line = {}
        for base_position, score in enumerate(numeric_phredscores, start=0):
            scores_per_line |= {base_position:score}
        all_basepositions_with_scores.append(scores_per_line)
    return all_basepositions_with_scores, headers

def calculate_average_phredscores(all_phredscores: dict):
        """
        Calculate the average phredscore per column.
        param: all_phredscores, a list with defaultdictionaries with for each base position a phredscore.
        The number of chunks equals to the number of defaultdictionaries.
        :return: A dictionary with for each base position (key) the average phredscore as value.
        """
        return [
            {
                "line_number": base_position,
                "average_phredscore": sum(phredscores) / len(phredscores),
            }
            for base_position, phredscores in sorted(all_phredscores.items())
        ]

def write_results(filename: str, average_scores: list, outputfile: str):
        fieldnames = ["line_number", "average_phredscore"]
        if outputfile:
            with open(outputfile, mode="a", encoding="UTF-8") as csvfile:
                writer = csv.DictWriter(
                    csvfile, fieldnames=fieldnames
                )  # use csv object for writing to a csv file
                csvfile.write(filename + "\n")
                for phredscores in average_scores:
                    writer.writerow(phredscores)
                csvfile.write("\n")
            print(
                f"--Successfully written {filename} mean phredscores to {outputfile}--"
            )
        else:  # print instead of write
            print(filename + "\n")
            for phredscores in average_scores:
                print(
                    f"{phredscores['line_number']},{phredscores['average_phredscore']}"
                )
            print("\n")

def process_chunk():
    phred_scores, headers = convert_binary_to_phredscores(sys.stdin.buffer.read())
    writer = csv.DictWriter(sys.stdout, fieldnames=range(max(headers)))
    writer.writeheader()
    for base_position_score in range(len(phred_scores)):
       writer.writerow(phred_scores[base_position_score])


if __name__ == "__main__":
    process_chunk()
