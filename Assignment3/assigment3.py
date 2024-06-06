import sys
import csv
from collections import defaultdict

def process_to_numeric(line: bytes):
    return [byte - 33 for byte in line]

def convert_binary_to_phredscores(chunk):
    lines = chunk.split(b"\n")
    lines = [line for line in lines if line]
    numeric_values = defaultdict(list)
    for index, line in enumerate(lines, start=0):
        numeric_phredscores = process_to_numeric(line.strip())
        for base_position, score in enumerate(numeric_phredscores, start=0):
            numeric_values[base_position].append(score)
    return numeric_values

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
    phred_scores = convert_binary_to_phredscores(sys.stdin.buffer.read())
    fieldnames = phred_scores.keys()
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    for position, value in enumerate(phred_scores.values()):
        writer.writerow(value[position])
    #for base_position, scores in sorted(phred_scores.items()):

    #print(f"{base_position},{scores}", file=sys.stdout)


if __name__ == "__main__":
    process_chunk()
