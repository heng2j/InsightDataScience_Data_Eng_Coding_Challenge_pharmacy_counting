# InsightDataScience Data Engineering Coding Challenge: pharmacy_counting


# Table of Contents
1. [Original Problem](README.md#original-problem)
2. [Input Dataset](README.md#input-dataset)
3. [Output](README.md#output)
4. [Approaches](README.md#approaches)
5. [Instructions on running the program](README.md#instructions-on-running-the-program)
6. [Testing](README.md#testing)
7. [Questions?](README.md#questions?)

# Original Problem:

"Imagine you are a data engineer working for an online pharmacy. You are asked to generate a list of all drugs, the total number of UNIQUE individuals who prescribed the medication, and the total drug cost, which must be listed in descending order based on the total drug cost and if there is a tie, drug name." 


# Input Dataset

"The original dataset was obtained from the Centers for Medicare & Medicaid Services but has been cleaned and simplified to match the scope of the coding challenge. It provides information on prescription drugs prescribed by individual physicians and other health care providers. The dataset identifies prescribers by their ID, last name, and first name.  It also describes the specific prescriptions that were dispensed at their direction, listed by drug name and the cost of the medication." 

The following are the input datasets I am using for this challenge:
* **`itcont.txt`** : the original sample dataset came with the challenge in the `insight_testsuite/tests/test_1/input` folder 
* **`itcont_sameCost.txt`** : a modified version of the original itcont.txt where AMBIEN costed the same as CHLORPROMAZINE in their first 2 prescriptions 
* **`de_cc_data.txt`** : the 1.18GB dataset de_cc_data.txt. (*Due to the huge file size, I didn't include in this repo. You can download from <a href="https://drive.google.com/file/d/1fxtTLR_Z5fTO-Y91BnKOQd6J0VC9gPO3/view?usp=sharing">Here</a>. )
* **`de_cc_data_head500.txt`** : the top 500 records from the 1.18GB dataset de_cc_data.txt. I wrote a function in the supporting script `dataset_tool.py` to extract and save this smaller sample dataset to `input/` for tests. 


# Output 

My program needs to create the output file, `top_cost_drug.txt`, that contains comma (`,`) separated fields in each line.

Each line of this file should contain these fields:
* drug_name: the exact drug name as shown in the input dataset
* num_prescriber: the number of unique prescribers who prescribed the drug. For the purposes of this challenge, a prescriber is considered the same person if two lines share the same prescriber first and last names
* total_cost: total cost of the drug across all prescribers

For example, if the input data, **`itcont.txt`**, is

```
id,prescriber_last_name,prescriber_first_name,drug_name,drug_cost
1000000001,Smith,James,AMBIEN,100
1000000002,Garcia,Maria,AMBIEN,200
1000000003,Johnson,James,CHLORPROMAZINE,1000
1000000004,Rodriguez,Maria,CHLORPROMAZINE,2000
1000000005,Smith,David,BENZTROPINE MESYLATE,1500
```

then the output file, **`top_cost_drug.txt`**, would contain the following lines
```
drug_name,num_prescriber,total_cost
CHLORPROMAZINE,2,3000
BENZTROPINE MESYLATE,1,1500
AMBIEN,2,300
```

These specific files that mentioned above are provided in the `insight_testsuite/tests/test_1/input` and `insight_testsuite/tests/test_1/output` folders, respectively.


# Approaches
## 1. Dictionary approach on single machine

Inspired by the mentioned sample approach `pharmacy_counting.py` that generated the expected output file within a single python script with input and output files as arguments.

Original suggested command to run pharmacy_counting.py:

 `python ./src/pharmacy_counting.py ./input/itcont.txt ./output/top_cost_drug.txt`


This is an initial approach (rapid prototyped) to solve the pharmacy counting problem. It is meant to test out the logics to solve this problem. Therefore, it is designed to run on a single machine due to it is using a dictionary to store the result. It is suppose to run faster on a single node with limited volume of data compared with the Mapreduce approach mentioned below.
However, it's performance is having a positive correlation with the volume size of data. It will get slower as data size increased. And one more down side of this approach is the dictionary does consume memory and it can also cause memory issue if the dictionary size exceed the machine's memory capacity. 

The major difference compaired to the mapreduce approch is. It used the data structure `set` to store the unique prescribers that prescribed for a particular drug. And then use the `len` of the set to sum up how many uniqued prescribers. And it used the Pyhton `sorted` function with lambda to sort the the values by drug_cost(index 2) in descending order first and then drug_name(index 0) in alphabetical order if there is a tie.


## 2. Python mapreduce approach for running on cluster-computing framework like PySpark

I can see that, this coding challenge is related to the classic mapreduce problem. For scalability for BIG Data, it is better to be capable to run on cluster-computing framework like PySpark on a distributed file system. 

For the interest to practice mapreduce streaming with Python, I wrote the `mapper.py` and `reducer.py` scripts to handle the map and reduce process. And also wrote the `saveOutput.py` to wrtie the reduced outputs into the target files.

Beside the output file, input file, and sorting in between mapper, reducer and saveOutput were handled with shell commands.

For example, run with sample itcont.txt please run the following command on command line:

      ` tail +2 ./input/itcont.txt | python3 ./src/mapper.py | sort | python3 ./src/reducer.py | sort -t $'\t' -k 3nr -k 1,1  | python3 ./src/saveOutput.py -o ./output/top_cost_drug.txt `

- Skipped the header/first line of the file with the command `tail -n +2`

Executing this approach on a single machine will take longer run time than then Dictionary approach. However it can be scale out to run on multiple nodes on a cluster-computing framework like PySpark on a distributed file system.


![Sample run time for both approaches on the 1.18GB datasets](/images/Demo_Runtime.png)


# Instructions on running the program 

This solution was implemented with Pyhton 3. And it only used the following Python standard libraries and functions: **`sys`** , **`csv`** , **`math`** , **`number`**, **`unittest`** and **`ArgumentParser`** from **`argparse`** .

The `run.sh` in top-most directory of my repo will compile and run the program to generated the expected file in `output/`.

On the givien<a href="http://ec2-18-210-131-67.compute-1.amazonaws.com/test-my-repo-link">Insight testing enviorment</a>,
it will run just the dictionary approaches. And they both accept the file named `itcont.txt` in the directory `input/` . 

As a work around to also run the mapreduce process, on a bash command line, you can run the following command:
      
` tail +2 ./input/itcont.txt | python3 ./src/mapper.py | sort | python3 ./src/reducer.py | sort -t $'\t' -k 3nr -k 1,1  | python3 ./src/saveOutput.py -o ./output/top_cost_drug.txt `




*** Due to I have 2 approaches, I designed the program to save the outputs from both approaches. And they are named different than the actual expecting output file name.*** 

* **`top_cost_drug_large.txt`** : the output file that generated from the dictionary approach with `de_cc_data.txt` as the input
* **`top_cost_drug_large_mapreduce.txt`** : the output file that generated from the mapreduce approach with `de_cc_data.txt` as the input



## Testing

For unit testing on the function for mapper and reducer, I wrote the script `unit_test_cases.py` with 3 test cases to test the functions with the module `unittest` .

For running the shell script testings within `run_tests.sh` in the `insight_testsuite` folder, I have modified the `output_files` variable in the function `run_all_tests` to make it as a list of testing output files. So instead of creating multiple test folders, we can loop to test all the input and output files at one time. It is testing the results from the mapreduce approach to see if it is matching with the given output file `top_cost_drug.txt` and the `top_cost_drug_head500.txt` that generated by the dictionary approach for cross checking.

Additionally, I modifed the `compare_outputs` function to have the `compare_final_outputs` to compare the large output files that generated by the both approaches with the 1.18GB `de_cc_data.txt` dataset .

The test script called `run_tests.sh` in the `insight_testsuite` folder.

You can run the test with the following command from within the `insight_testsuite` folder:

    insight_testsuite~$ ./run_tests.sh 




# Questions?
Email me at heng2j@nyu.edu
