#!/bin/bash

#SBATCH --job-name=slorm_a4
# run for max five minutes
# #            d-hh:mm:ss
#SBATCH --time 0-00:10:00
#SBATCH --nodes=1
#SBATCH --partition=assemblix
#SBATCH --nodelist=assemblix2019
#SBATCH --ntasks=5
#SBATCH --cpus-per-task=1
#SBATCH --mem=24GB
#SBATCH --mail-type=ALL

source /commons/conda/conda_load.sh

THISDIR=$(dirname -- "$( readlink -f -- "$0"; )")
mpiexec -n $SLURM_NTASKS python3 assignment4.py -c 10 /commons/Themas/Thema12/HPC/rnaseq.fastq