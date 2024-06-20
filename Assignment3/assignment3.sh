#!/bin/bash

#tmp - easy to work with
if [ -z "$1" ]; then 
    INPUT_FILE=~/Desktop/BDC/Assignment2/rnaseq_selection.fastq
else 
    INPUT_FILE=$1
fi

#https://www.gnu.org/software/parallel/parallel_tutorial.html

cat "$INPUT_FILE" | parallel --pipe -j 3 python assignment3.py