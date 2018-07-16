#!/usr/bin/env python3
# reducer.py
# ---------------
# Author: Zhongheng Li

"""
reducer.py is consolidating the streaming input data from mapper line by line into the expected format

- The unix sorting command get the references from: https://www.geeksforgeeks.org/sort-command-linuxunix-examples/
- Skip the header / first line of the file with tail -n +2

    Run with sample itcont.txt please run the following command on command line

      tail +2 ./input/itcont.txt | python3 ./src/mapper.py | sort | python3 ./src/reducer.py | sort -t $'\t' -k 3nr -k 1,1

"""

import sys
import math

# Set up Global Variables
columns_output = ['drug_name', 'num_prescriber', 'total_cost']


# Get streaming feed from mapper line by line
def get_reducer_feeds(stdin):
    for line in stdin:
        line = line.strip()
        drug_name, prescriber, drug_cost = line.split('\t')
        yield (drug_name, prescriber, math.ceil(float(drug_cost)))


# Process streaming feed from mapper and output expected values line by line
def reducer(stdin):
    current_drug_name = None
    current_prescriber = None
    current_prescriber_count = 0
    current_total_cost = 0
    drug_name = None

    for drug_name, prescriber, drug_cost in get_reducer_feeds(stdin):

        if current_drug_name == drug_name:

            current_total_cost += drug_cost

            if prescriber != current_prescriber:
                current_prescriber_count += 1
                current_prescriber = prescriber

        else:
            if current_drug_name:
                yield ('%s\t%d\t%d' % (
                    current_drug_name, current_prescriber_count,
                    current_total_cost))

            current_drug_name = drug_name
            current_prescriber = prescriber
            current_prescriber_count = 1
            current_total_cost = drug_cost

    if current_drug_name == drug_name:
        yield ('%s\t%d\t%d' % (
            current_drug_name, current_prescriber_count,
            current_total_cost))


# A wrapper function for reducer
def run_reducer(stdin):
    for result in reducer(stdin):
        print(result)


if __name__ == '__main__':
    run_reducer(sys.stdin)
