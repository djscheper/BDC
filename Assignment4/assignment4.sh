#!/bin/bash

#SBATCH --job-name=djscheper_slurm
#SBATCH --time=0-00:01:00
#SBATCH --nodes=1
#SBATCH --partition=assemblix
#SBATCH --nodelist=assemblix2019
#SBATCH --ntasks=5
#SBATCH --cpus-per-task=1
#SBATCH --mem=5GB

##
# Created by Dennis Scheper - d.j.scheper@st.hanze.nl
# Usage: 
#       first time only; chmod +x assignment4.sh
#       ./assignment4.sh [fastq_file1] [fastq_file2] [fastq_fileN]
##

if [ -z "$1" ]; then 
   export INPUT_FILES="/commons/Themas/Thema12/HPC/rnaseq.fastq"
else 
   export INPUT_FILES=("$@")
fi

export WORK_DIR=$(realpath "$(dirname "$0")")

source /commons/conda/conda_load.sh

mpirun -np 5 python3 assignment4.py "${INPUT_FILES}"
