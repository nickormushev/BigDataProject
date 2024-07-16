#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=06:30:00
#SBATCH --mem=10G
#SBATCH --output=logs/bd-%J.out
#SBATCH --error=logs/bd-%J.err
#SBATCH --job-name="bd-duck"

srun singularity -d exec ./containers/bd.sif python \
    /d/hpc/home/nk93594/BigDataProject/3_analysis/read_datasets.py ${1}

