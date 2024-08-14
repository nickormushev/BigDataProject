#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=06:30:00
#SBATCH --mem=60G
#SBATCH --output=logs/bd-%J.out
#SBATCH --error=logs/bd-%J.err
#SBATCH --job-name="bd-gen"

# bd-hdf is the image.def container but with numpy 1.26.4 so the hdf conversion works
srun singularity -d exec ./containers/bd-hdf.sif python \
    /d/hpc/home/nk93594/BigDataProject/1_convert_files/generate_datasets.py ${1}

