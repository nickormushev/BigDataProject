#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=06:30:00
#SBATCH --mem=60G
#SBATCH --output=logs/bd-%J.out
#SBATCH --error=logs/bd-%J.err
#SBATCH --job-name="bd-merge"

if [ "$#" -ne 3 ]; then
    echo "Tree arguments required: <dataset to merge with> <compression> <duckDB or dask>"
    exit 1
fi

if [ "${3}" != "dask" ] && [ "${3}" != "duckDB" ]; then
    echo "Invalid third argument: ${3}. Must be either 'dask' or 'duckDB'"
    exit 1
fi

if [ "${3}" != "duckDB" ]; then
    echo "Merging with Dask"
    srun singularity -d exec ./containers/bd.sif python \
        /d/hpc/home/nk93594/BigDataProject/2_data_augmentation/merge.py ${1} ${2}
else
    echo "Merging with DuckDB"
    srun singularity -d exec ./containers/bd.sif python \
        /d/hpc/home/nk93594/BigDataProject/2_data_augmentation/merge_with_duck_db.py ${1} ${2}
fi