#!/bin/bash

##
# Created by Dennis Scheper - d.j.scheper@st.hanze.nl
# Usage: 
#       first time only; chmod +x deliverable2.sh
#       ./deliverable2.sh [fastq_file1] [fastq_file2] [fastq_fileN]
##

if [ -z "$1" ]; then 
   export INPUT_FILES="/commons/Themas/Thema12/HPC/rnaseq.fastq"
else 
   export INPUT_FILES=("$@")
fi

export WORK_DIR=$(realpath "$(dirname "$0")")


RESULT_FILE="${WORK_DIR}/timing.csv"
echo "worker_repeat,time" > "${RESULT_FILE}"

for worker in {3..5}; do #minimal 2 since we want to have 1 controller at all times
  FILE_PATH="data/worker_${worker}"
  mkdir -p ${FILE_PATH} #make directory, -p since we don't want errors if the directory already exists
  for repeat in {1..3}; do
    echo "${worker} on ${repeat}"
    output="output_${worker}_${repeat}.csv"
    run_time=$(mpirun -np $worker python3 "${WORK_DIR}"/deliverable2.py "${INPUT_FILES}" -o ${WORK_DIR}/${FILE_PATH}/${output})
    echo "${worker}_${repeat},${run_time}" >> "${RESULT_FILE}"
  done
done