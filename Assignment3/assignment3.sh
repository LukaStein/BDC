#!/bin/bash
#SBATCH --job-name=process_chunks_to_phredscores
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=01:00:00

# Arguments for splitting
FILE_PATH=$1
N_CHUNKS=10


# Create temporary directory
SCRATCH_DIRECTORY=job_${SLURM_JOBID}
mkdir -p ${SCRATCH_DIRECTORY}

# Extract every fourth line (the quality score lines) and write them to an intermediate file
awk 'NR % 4 == 0' $FILE_PATH > $SCRATCH_DIRECTORY/quality_lines.txt

# Calculate total number of quality lines
TOTAL_LINES=$(wc -l < $SCRATCH_DIRECTORY/quality_lines.txt)

# Calculate lines per chunk
LINES_PER_CHUNK=$(( TOTAL_LINES / N_CHUNKS ))

# Create N chunks intermediat files
split -l $LINES_PER_CHUNK $SCRATCH_DIRECTORY/quality_lines.txt $SCRATCH_DIRECTORY/chunk_

# Clean up the intermediate file
rm $OUTPUT_DIR/quality_lines.txt

# Tell user the number of chunks that were made
NUM_CHUNKS=$(ls -1 $OUTPUT_DIR | wc -l)
echo "Reads split into $NUM_CHUNKS chunks."
# Load appropriate environment for python script
#source /commons/conda/conda_load.sh

#for chunk in $SCRATCH_DIRECTORY/chunk_*
#do
 # cat chunk