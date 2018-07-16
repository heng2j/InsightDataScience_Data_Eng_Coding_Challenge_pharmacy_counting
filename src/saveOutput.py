#!/usr/bin/env python3
# saveOutput.py
# ---------------
# Author: Zhongheng Li

"""
saveOutput.py is an additional function to write the output form the streaming mapreduce process

- The unix sorting command get the references from: https://www.geeksforgeeks.org/sort-command-linuxunix-examples/
- Skip the header / first line of the file with tail -n +2


    Run with sample itcont.txt please run the following command on command line:
      tail +2 ./input/itcont.txt | python3 ./src/mapper.py | sort | python3 ./src/reducer.py | sort -t $'\t' -k 3nr -k 1,1  | python3 ./src/saveoutput.py -o ./output/top_cost_drug_sample_mapreduce.txt

    Run with sample itcont_sameCost.txt to run the modified itcont.txt where there are same drug costs please run the following command on command line
    Skip the header / first line of the file with tail -n +2
      tail +2 ./input/itcont_sameCost.txt | python3 ./src/mapper.py | sort | python3 ./src/reducer.py | sort -t $'\t' -k 3nr -k 1,1  | python3 ./src/saveoutput.py -o ./output/top_cost_drug_sameCost_mapreduce.txt


"""

import sys
import csv
import math
from argparse import ArgumentParser





# Set up Global Variables
columns_output = ['drug_name', 'num_prescriber', 'total_cost']


def write_header(columns_output, output_file):
    with open(output_file, 'w', newline='') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(columns_output)


def write_output_file(drug_name, num_prescriber,
                      total_cost):
    with open(outputFile, 'a', newline='') as output_file:
        writer = csv.writer(output_file)
        writer.writerow([drug_name, num_prescriber, total_cost])


def get_feeds(stdin):
    for line in stdin:
        line = line.strip()
        drug_name, num_prescriber, total_cost = line.split('\t')
        yield (drug_name, num_prescriber, math.ceil(float(total_cost)))


def export(stdin):
    for drug_name, num_prescriber, total_cost in get_feeds(stdin):
        write_output_file(drug_name, num_prescriber, total_cost)


if __name__ == '__main__':
    # Set up argument parser
    parser = ArgumentParser()
    parser.add_argument("-o", "--outputfile", help="Output file path", required=True)

    args = parser.parse_args()

    outputFile = args.outputfile

    write_header(columns_output, outputFile)
    export(sys.stdin)
