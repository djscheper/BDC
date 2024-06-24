#!/bin/bash

##
# Created by Dennis Scheper - d.j.scheper@st.hanze.nl
# Usage: 
#       first time only; chmod +x assignment3.sh
#       ./assignment3.sh [fastq_file1] [fastq_file2] [fastq_fileN]
##

# user can specify their own fastq file, if not use the standard one
if [ -z "$1" ]; then 
   INPUT_FILES="/commons/Themas/Thema12/HPC/rnaseq.fastq"
else 
   INPUT_FILES=("$@")
fi

#count how many files if user decides to give more than one
NUM_FILES=${#INPUT_FILES[@]}
HOSTS="nuc100,nuc101"

#export for parallel processes
export WORK_DIR=$(realpath "$(dirname "$0")")

for INPUT_FILE in "${INPUT_FILES[@]}"; do
  export FILE_NAME=$(basename "$INPUT_FILE")
  parallel -S "${HOSTS}" --pipepart --jobs 4 --block 10M --regex --recstart "@.*$" --recend "\n" python3 "${WORK_DIR}"/assignment3.py --calculate :::: "${INPUT_FILE}" | python3 "${WORK_DIR}"/assignment3.py --merge -o "${FILE_NAME}" -n "${NUM_FILES}"
done
