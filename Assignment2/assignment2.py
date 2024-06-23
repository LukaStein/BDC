"""
assignment2.py
Framework for setting up a distributed work Queue over the network.
The framework is used for calculating average phredscores per base position in a fastq_file.
"""

# IMPORTS
import argparse as ap
import csv
from collections import defaultdict
import multiprocessing as mp
from multiprocessing.managers import BaseManager
import os
import time
import queue


def parser():
    """
    Command line interface to present and receive arguments
    :return: argparser.parse_args(); contains arguments from user
    """
    argparser = ap.ArgumentParser(
        description="Script voor Opdracht 2 van Big Data Computing;  Calculate PHRED scores over the network."
    )
    mode = argparser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "-s",
        action="store_true",
        help="Run the program in Server mode; see extra options needed below",
    )
    mode.add_argument(
        "-c",
        action="store_true",
        help="Run the program in Client mode; see extra options needed below",
    )

    server_args = argparser.add_argument_group(
        title="Arguments when run in server mode"
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
        nargs="*",
        help="At least 1 processable \
                           Illumina Fastq Format file",
    )
    server_args.add_argument("--chunks", action="store", type=int)

    client_args = argparser.add_argument_group(
        title="Arguments when run in client mode"
    )
    client_args.add_argument(
        "-n",
        action="store",
        dest="n",
        required=False,
        type=int,
        help="Number of cores to use per host",
    )
    client_args.add_argument(
        "--host",
        action="store",
        type=str,
        help="The hostname where the Server is listening",
    )
    client_args.add_argument(
        "--port",
        action="store",
        type=int,
        help="The port on which the Server is listening",
    )

    return argparser.parse_args()


class ParallelProcessing:
    """
    Divide and send chunks of a FastQ file in tasks to client computer(s),
    that will process these chunks into phredscores with n cores.
    Phredscores are aggregated per base position per FastQ file,
    from which averaged scores are made. Results are either
    written to a CSV file or send to STDOUT.
    """
    def __init__(self, ip, portnum=5381):
        self.ip = ip
        self.portnum = portnum
        self.AUTHKEY = b"whathasitgotinitspocketsesss?"
        self.POISONPILL = "MEMENTOMORI"
        self.ERROR = "DOH"

    def make_server_manager(self, port, authkey):
        """
        Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
        """
        job_q = queue.Queue()
        result_q = queue.Queue()

        # This is based on the examples in the official docs of multiprocessing.
        # get_{job|result}_q return synchronized proxies for the actual Queue
        # objects.
        class QueueManager(BaseManager):
            pass

        QueueManager.register("get_job_q", callable=lambda: job_q)
        QueueManager.register("get_result_q", callable=lambda: result_q)

        manager = QueueManager(address=("", port), authkey=authkey)
        manager.start()
        print(f"Server started at port {port}")
        return manager

    def runserver(self, fn, data, csvfile=None):
        # Start a shared manager server and access its queues
        manager = self.make_server_manager(
            self.portnum, b"whathasitgotinitspocketsesss?"
        )
        shared_job_q = manager.get_job_q()
        shared_result_q = manager.get_result_q()

        if not data:
            print("Gimme something to do here!")
            return
        print("Sending data!")
        for d in data:
            shared_job_q.put({"fn": fn, "arg": d})
        time.sleep(2)
        results = []
        file_results = defaultdict()
        while True:
            try:
                result = shared_result_q.get_nowait()
                res_file = result["result"][0]
                res_scores = result["result"][1]

                if res_file not in file_results:
                    file_results[res_file] = defaultdict(
                        list
                    )  # baseposition : phredscores

                results.append(res_scores)
                # for file, score_chunks in res:
                # for score_chunk in res[1]:
                for (
                    base_position,
                    phredscores,
                ) in res_scores.items():  # type: int type: lists
                    file_results[res_file][base_position].extend(phredscores)
                # print(file_results[res_file])
                # print("Got result!", result)
                if len(results) == len(data):
                    print("Got all results!")
                    break
            except queue.Empty:
                time.sleep(1)
                continue
        # Tell the client process no more data will be forthcoming
        print("Time to kill some peons!")
        shared_job_q.put(self.POISONPILL)
        # Sleep a bit before shutting down the server - to give clients time to
        # realize the job queue is empty and exit in an orderly way.
        time.sleep(5)
        print("Aaaaaand we're done for the server!")
        manager.shutdown()
        for file, aggregated_phredscores in file_results.items():
            if not csvfile:  # no output file specified, write to terminal
                out_file = None
            elif csvfile and len(file_results) == 1:  # we have one file and write to a file
                out_file = csvfile
            elif csvfile and len(file_results) > 1: # we have multiple files and write to a file
                output_file_dir = os.path.dirname(csvfile)
                fastq_file_name = os.path.basename(file)
                output_file_extension = ".output.csv"
                out_file = os.path.join(output_file_dir, fastq_file_name + output_file_extension)
            self.write_results(
                file,
                self.calculate_average_phredscores(aggregated_phredscores),
                out_file)

    def calculate_average_phredscores(self, all_phredscores: dict):
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

    def write_results(self, filename: str, average_scores: list, outputfile: str|None):
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

    def make_client_manager(self, ip, port, authkey):
        """Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
        """

        class ServerQueueManager(BaseManager):
            pass

        ServerQueueManager.register("get_job_q")
        ServerQueueManager.register("get_result_q")

        manager = ServerQueueManager(address=(ip, port), authkey=authkey)
        manager.connect()

        print(f"Client connected to {ip}:{port}")
        return manager

    def runclient(self, num_processes):
        """
        Create client manager and start all jobs
        """
        manager = self.make_client_manager(self.ip, self.portnum, self.AUTHKEY)
        job_q = manager.get_job_q()
        result_q = manager.get_result_q()
        self.run_workers(job_q, result_q, num_processes)

    def run_workers(self, job_q, result_q, num_processes):
        """
        Run all processes
        """
        processes = []
        for _ in range(num_processes):
            temp_process = mp.Process(target=self.peon, args=(job_q, result_q))
            processes.append(temp_process)
            temp_process.start()
        print(f"Started {len(processes)} workers!")
        for temp_process in processes:
            temp_process.join()

    def peon(self, job_q, result_q):
        """
        Try parallel task (job), if finished with job put kill message for the current process in the queue
        """
        my_name = mp.current_process().name
        while True:
            try:
                job = job_q.get_nowait()
                if job == self.POISONPILL:
                    job_q.put(self.POISONPILL)
                    print("Aaaaaaargh", my_name)
                    return
                try:
                    result = job["fn"](job["arg"])
                    print(f"Peon {my_name} Workwork on {job['arg']}!")
                    result_q.put({"job": job, "result": result})
                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({"job": job, "result": self.ERROR})

            except queue.Empty:
                print("sleepytime for", my_name)
                time.sleep(1)


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
            # "The Python File seek() method sets the file's cursor at a specified position in the current file"
            binary_file.seek(start)
            binary_chunk = binary_file.read(
                end - start
            )  # therefore, slicing a piece at end - start
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
        # Filter empty lines at end of chunks
        lines = [line for line in lines if line]
        numeric_values = defaultdict(list)  # every new key gets a list!
        for index, line in enumerate(lines, start=0):
            if index % 4 == 3:  # select fourth line additivel
                numeric_phredscores: list = self.process_to_numeric(line.strip())
                for base_position, score in enumerate(numeric_phredscores, start=0):
                    numeric_values[base_position].append(
                        score
                    )  # each new read add scores to corresponding base_position
        return numeric_values

    def calculate_average_phredscores(self, all_phredscores: dict):
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
        return file_path, self.convert_binary_to_phredscores(chunk)

    def check_read_completeness(self, file_path, start, end):
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

    def determine_chunks(self, file_path):
        """
        Split file into chunks and process each chunk in parallel over n cpus.
        It attempts to create 10 chunks and adds the remaining chunk at the end of chunk list.
        param: file_path, string
        param: cpus, int
        :return: average phredscores
        """
        file_size = os.path.getsize(file_path)
        chunk_size = file_size // self.n_chunks  # tries 10 chunks
        uneven_chunk = (
            file_size % self.n_chunks
        )  # determines remaining number of chunks possible

        # tuples of (filename, start e.g. = chunk_size * 0 = 0, end = 0 + 1 * chunk_size = chunk_size or file_size).
        # file_size is only chosen as last in the iteration

        # add uneven_chunk at end of chunks list, for end the uneven_chunk is added to the largest chunk to reach the max or file_size
        # First try all iterations in n_chunks with n_chunks -1. Finally, get the last chunk with i + 1
        chunks = [
            self.check_read_completeness(
                file_path,
                i * chunk_size,
                (i + 1) * chunk_size
                if i < self.n_chunks - 1
                else (i + 1) * chunk_size + uneven_chunk,
            )
            for i in range(self.n_chunks)
        ]
        return chunks


if __name__ == "__main__":
    args = parser()
    if args.s and args.fastq_files:
        print("Server mode!")
        if not args.csvfile: # no output file specified, write to terminal
            outfile_name = None
        else:
            outfile_name = args.csvfile.name
            # make sure output file can be re-opened in Luka's function
            args.csvfile.close()

        pp_m = ParallelProcessing(args.host, args.port)
        m_ps_c = PhredscoreCalculator(args.chunks)

        all_chunks = []
        for fastq_file in args.fastq_files:
            all_chunks += m_ps_c.determine_chunks(fastq_file)
        server = mp.Process(
            target=pp_m.runserver, args=(m_ps_c.process_chunk, all_chunks, outfile_name)
        )
        server.start()
        server.join()
        print("--End of results--")

    elif args.c:
        if not args.host or not args.port:
            raise ValueError("Client mode requires --host and --port arguments")
        print("Client mode!")
        pp_c = ParallelProcessing(args.host, args.port)
        client = mp.Process(target=pp_c.runclient, args=(args.n,))
        client.start()
        client.join()  # Correctly wait for the client process to finish

    else:
        raise TypeError("No program mode given")
