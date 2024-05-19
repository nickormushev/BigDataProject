#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
##SBATCH --partition=gpu
##SBATCH --gpus=1
#SBATCH --time=00:30:00
#SBATCH --mem=16G
#SBATCH --output=logs/bd-%J.out
#SBATCH --error=logs/bd-%J.err
#SBATCH --job-name="bd-test"

srun singularity -d exec ./containers/bd.sif python \
    "/d/hpc/home/nk93594/BigDataProject/generate_datasets.py"

