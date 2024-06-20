#!/usr/bin/env python3

"""
Big Data Computing (BDC) assignment 3. 

By Dennis Scheper (373689)

FIRST:
    chmod +x assignment3.sh

Usage:
    ./assignment3.sh <inputfiles>
"""

from collections import defaultdict
import sys
import numpy as np

__author__ = "Dennis Scheper (373689)"
__status__ = "Work in progress..."
__date__ = ""
__contact__ = "d.j.scheper@st.hanze.nl"

def calculation(file,chunck):
    """
    Processes the FastQ file, calculates the Phred score, and adds it to a
    defaultdict. Defaultdict allows us to add more base positions in a
    straightforward manner.

    args:
    - chunck: chunck to be processed

    returns:
    - phred_scores: defaultdict with Phred score per base position for said chunck
    """
    # unpack the chunck's start and end positions
    start, end = chunck
    # defaultdict makes adding more base positions more flexible
    phred_scores = defaultdict(list)

    with open(file, 'rb') as inputfile:
        inputfile.seek(start)
        while inputfile.tell() < end:
            line = inputfile.readline()
            if line.startswith(b"@"):
                inputfile.readline()
                inputfile.readline()
                qual = inputfile.readline().strip()
                for pos, quality in enumerate(qual):
                    phred_scores[pos].append(quality - 33)  # no need for ord() since we are already working in bytes

    return phred_scores

def calculate_average(phred_scores):
    """
    Calculates the average Phred score per base position by first merging
    every dict per chunck into one. Then simply calculate the average.

    args:
    - phred_scores: list of defaultdicts 

    returns:
    - a defaultdict with the average phred score per base position
    """
    combined = defaultdict(list)

    # combine every chunck into one dict
    for sub_dict in phred_scores:
        for key, value in sub_dict.items():
            combined[key].extend(value)
        
    # then simply return the mean per base position
    return {key: np.mean(values) for key, values in combined.items()}


def main():
    """
    """
    input_data = sys.stdin.read()
    print(input_data)

if __name__ == "__main__":
    main()
