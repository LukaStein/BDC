# FastQC tool and multiprocessing

With the use of multiprocessing it's possible to divide a task into to several tasks or chunks.
In this exercise phredscores are calculated per chunk that is a slice of a fastq file.

Next the phredscores are merged per base position and average phredscores are calculated.

This tool has a command-line interface and can STDOUT your results or save them to a specified CSV file.

For this exercise the following files are used for testing:
  1. /commons/Themas/Thema12/HPC/rnaseq.fastq
  2. /data/datasets/rnaseq_data/Brazil_Brain/SPM26_R1.fastq
