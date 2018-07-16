#!/usr/bin/env python3
# mapper.py
# ---------------
# Author: Zhongheng Li

"""
mapper.py is mapping the streaming input data line by line as dictionary labeled type

- Skip the header / first line of the file with tail -n +2

    Run with sample itcont.txt please run the following command on command line:
      tail +2 ./input/itcont.txt | python3 ./src/mapper.py | sort


    Run with sample itcont_head500.txt please run the following command on command line:
      tail -n ./input/itcont.txt | python3 ./src/mapper.py | sort
"""

import sys
import csv
import math
import numbers

# Set up Global Variables
columns_input = ['id', 'prescriber_last_name', 'prescriber_first_name', 'drug_name', 'drug_cost']


# Check if the line is in proper format and take action if necessary. Look for outliers
def check_line(line):
    # Discovered one outliner with extra ',' in between
    # Check for outliner PANCRELIPASE 5,000
    # if line['drug_name'] == "PANCRELIPASE 5,000":
    #     print(line)
    #     print("Outlier!!! - Found PANCRELIPASE 5,000")
    #     sys.exit(1)

    # Check for missing value
    if len(set(columns_input) - line.keys()) != 0:
        print("Outlier!!! - There are missing fields in this line")
        sys.exit(1)

    if ('drug_name' not in line) or (not isinstance(line['drug_name'], str)):
        print("Outlier!!! - Missing drug_name or drug_name is not a string ")
        sys.exit(1)

    if ('prescriber_last_name' not in line) or (not isinstance(line['prescriber_last_name'], str)):
        print("Outlier!!! - Missing prescriber_last_name or prescriber_last_name is not a string ")
        sys.exit(1)

    if ('prescriber_first_name' not in line) or (not isinstance(line['prescriber_first_name'], str)):
        print("Outlier!!! - Missing prescriber_first_name or prescriber_first_name is not a string ")
        sys.exit(1)

    if ('drug_cost' not in line) or (not isinstance(float(line['drug_cost']), numbers.Number)):
        print("Outlier!!! - Missing drug_cost or drug_cost is not a number ")
        sys.exit(1)


# Read in the file line by line as dictionary labeled type
def csvDictRows(stdin):
    for line in csv.DictReader(stdin, fieldnames=columns_input):
        check_line(line)
        yield ('%s\t%s\t%d' % (
            line['drug_name'], str(line['prescriber_last_name'] + " " + line['prescriber_first_name']),
            math.ceil(float(line['drug_cost']))))


def mapper(stdin):
    for line in csvDictRows(stdin):
        print(line)


if __name__ == '__main__':
    mapper(sys.stdin)
