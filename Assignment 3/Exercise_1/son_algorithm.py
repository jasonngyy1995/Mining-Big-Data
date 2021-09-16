import argparse
import sys
import sympy
import time
import random
import re
import os, psutil

# arguments for compiling and running the program
parser = argparse.ArgumentParser()
parser.add_argument('-f', '--data_file')
parser.add_argument('-s', '--support_threshold', default=1)
parser.add_argument('-c', '--confidence', default=0.5)

args = parser.parse_args()

# store the sample from data file
original_dataset = [] 

# split data into 4 chunks
chunk_1 = []
chunk_2 = []
chunk_3 = []
chunk_4 = []

# list to hold the first mapreduce result
first_map_list = []

# 4 chunks after apriori
after_chunk_1 = []
after_chunk_2 = []
after_chunk_3 = []
after_chunk_4 = []

# 4 chunks after apriori
first_reduce_1 = []
first_reduce_2 = []
first_reduce_3 = []
first_reduce_4 = []

# function of process the data of file and return a list
def retrieve_file(data_file):
    file = open(data_file, 'r')

    # count the size of entire dataset
    data_line_num = 0

    # reading the entire file
    for line in file:
        empty_right_space_line = line.rstrip() 
        converted_data = empty_right_space_line.split()

        data_line_num += 1
    
        original_dataset.append(converted_data)

    file.close()

    return data_line_num


# read the sample data, then count and store single itemset 
def get_single_itemset(data):
    single_itemset = []
    
    # pick single items and store into another array
    for basket in data:
        for i in basket:
            # no repeat single item
            if not [i] in single_itemset:
                single_itemset.append([i])

    # store them a frozenset list
    return list(map(frozenset, single_itemset))

""" 
 1) dataset = sample data in a list of 'set'
 2) candidate = single itemset
 3) minimum_support define in argument

 - find itemset which is above minimum support threshold
"""
def if_above_support(dataset, candidate, min_support):
    # create a dictionary list
    item_dictionary = {}

    # store single itemset into a dictionary list, single itemset as key,support as value
    for item in dataset:
        for c in candidate: 
            # increment their support value
            if c.issubset(item):
                if not c in item_dictionary: 
                    item_dictionary[c]=1
                else: item_dictionary[c]+=1
    
    above_support = {}
    # store single itemset which has a support value over the minimum support threshold
    pass_list = []
    # size of sample data
    item_num = float(len(dataset))
    
    for key in item_dictionary:
        # calculate the support of each single itemset
        support = item_dictionary[key]/item_num
        if support >= min_support:
            # store itemset which is above min. support
            pass_list.insert(0,key)    
            
        # store itemset which is above min. support with its support value
        above_support[key] = support
    
    return pass_list, above_support

# create itemset which is a possible candidate
def create_candidate(fre_itemset, next_index):
    fre_pair_list = []
    fre_itemset_size = len(fre_itemset)

    for i in range(fre_itemset_size):
        for j in range(i+1, fre_itemset_size):
            list1 = list(fre_itemset[i])[:next_index-2]
            list2 = list(fre_itemset[j])[:next_index-2]
            
            # create frequent itemsets pairs
            # {4},{5} -> {4,5}, {5,4}
            if (list1 == list2):
                fre_pair_list.append(fre_itemset[i] | fre_itemset[j])

    return fre_pair_list

def apriori_algo(data, min_support):
    # all single itemsets
    single_itemset = get_single_itemset(data)
    # single itemset in a list of set
    dataset = list(map(set, data))

    # get the frequent itemset and its support value
    candidate_list, support_value = if_above_support(dataset, single_itemset, min_support)

    # candidate_list as an element in candidate itemset
    candidate_itemset = [candidate_list]

    next_index = 2

    while (len(candidate_itemset[next_index-2]) > 0):
        # create frequent itemset pairs from frequent itemsets
        candidate = create_candidate(candidate_itemset[next_index-2], next_index)
        # check if frequent itemset pairs above minimum support threshold
        candidate_list_2, support_value_2 = if_above_support(dataset, candidate, min_support)
        # update support value
        support_value.update(support_value_2)
        # get the new frequent itemset pairs result
        candidate_itemset.append(candidate_list_2)
        next_index += 1

    return candidate_itemset, support_value

# find rule (e.g. {2} - {5, 6}) which is above the minimum confidence
def check_confidence(fre_item, fre_item_frzset, support_value, assc_rule_list, min_confidence):
    itemset_above_min_conf_list = []

    for i in fre_item_frzset:
        # calculate the confidence of a frequent itemset
        confidence = support_value[fre_item]/support_value[fre_item-i]

        if confidence >= min_confidence:
            assc_rule_list.append((fre_item - i, i, confidence))
            # store into a list
            itemset_above_min_conf_list.append(i)
            
            # for print the result
            # str_fre_item = str(fre_item - i)
            # str_i = str(i)

            # eliminate the word 'frozenset' in the result
            # print(str_fre_item[9:], '-', str_i[9:])

    return itemset_above_min_conf_list

# get more associate rule from frequent items result produced by apriori_algo()
def rule_rhs(fre_item, fre_item_frzset, support_value, assc_rule_list, min_confidence):
    l = len(fre_item_frzset[0])

    if (len(fre_item) > (l+1)):
        # create new candidate
        new_candidate = create_candidate(fre_item_frzset, l+1)
        new_candidate = check_confidence(fre_item, new_candidate, support_value, assc_rule_list, min_confidence)

        # merge two or more frequent itemset pairs
        if (len(new_candidate) > 1):
            rule_rhs(fre_item, new_candidate, support_value, assc_rule_list, min_confidence)

# get the result of associate rules which is above minimum confidence
def associate_rule(fre_pair_list, support_value, min_confidence):
    assc_rule_list = []

    # find frequent itemsets with more than items
    for i in range(1, len(fre_pair_list)):
        for fre_item in fre_pair_list[i]:
            fre_item_frzset = [frozenset([j]) for j in fre_item]
            if (i > 1):
                rule_rhs(fre_item, fre_item_frzset, support_value, assc_rule_list, min_confidence)
            else:
                check_confidence(fre_item, fre_item_frzset, support_value, assc_rule_list, min_confidence)
            
    return assc_rule_list

def apriori_func(split_data):
    
    single_itemset_list = get_single_itemset(split_data)
    # map sample data into a list of 'set'
    dataset = list(map(set, split_data))
    
    # first pass of apriori algorithm
    pass_list, support_value_1 = if_above_support(dataset, single_itemset_list, float(args.support_threshold))
    
    # second pass of apriori algorithm
    candidate_list, support_value_2 = apriori_algo(split_data, float(args.support_threshold))
    assc_rule_list = associate_rule(candidate_list, support_value_2, float(args.confidence))

    return assc_rule_list;
    
# SON

# function of process the data of file and return a list
def assign_sample(original_dataset):
    # reading the entire file and split it into four chunks
    for i in range(len(original_dataset)):
        if (i % 4 == 0):
            chunk_1.append(original_dataset[i])

        if (i % 4 == 1):
            chunk_2.append(original_dataset[i])

        if (i % 4 == 2):
            chunk_3.append(original_dataset[i])

        if (i % 4 == 3):
            chunk_4.append(original_dataset[i])

def apriori_each():
    list_1 = []
    list_2 = []
    list_3 = []
    list_4 = []

    # process each chunk
    # change them into lists and remove the last element
    after_chunk_1 = apriori_func(chunk_1)
    for x in after_chunk_1: 
        listx1 = list(x)
        listx1.pop()
        list_1.append(listx1)
    
    after_chunk_2 = apriori_func(chunk_2)
    for x in after_chunk_2: 
        listx2 = list(x)
        listx2.pop()
        list_2.append(listx2)

    after_chunk_3 = apriori_func(chunk_3)
    for x in after_chunk_3: 
        listx3 = list(x)
        listx3.pop()
        list_3.append(listx3)

    after_chunk_4 = apriori_func(chunk_4)
    for x in after_chunk_4: 
        listx4 = list(x)
        listx4.pop()
        list_4.append(listx4)

    # store it in first_map_list
    first_map_list = list_1 + list_2 + list_3 + list_4

    return first_map_list

# accumulate the result
def first_reduce(first_map_list):
    tmp1 = []
    for freq_item in first_map_list:
        tmp1.append(freq_item)

    result = []
    for res in tmp1:
        if not res in result:
            result.append(res)

    return result

# function of process the first reduce result and return a list
def assign_res(first_reduce_result):
    total_first_reduce = []

    # reading the entire file
    for i in range(len(first_reduce_result)):
        if (i % 4 == 0):
            first_reduce_1.append(first_reduce_result[i])

        if (i % 4 == 1):
            first_reduce_2.append(first_reduce_result[i])

        if (i % 4 == 2):
            first_reduce_3.append(first_reduce_result[i])

        if (i % 4 == 3):
            first_reduce_4.append(first_reduce_result[i])

    # store it into total_first_reduce
    total_first_reduce.append(first_reduce_1)
    total_first_reduce.append(first_reduce_2)
    total_first_reduce.append(first_reduce_3)
    total_first_reduce.append(first_reduce_4)

    return total_first_reduce

# function to check if frequent itemsets above support threshold
def if_res_above_support(first_reduce_result, data_line_num):
    final_result = []

    result = assign_res(first_reduce_result)
    
    for res in result:
        for i in range(len(res)):
            list_frzset = res[i]
            
            # count the occurrence
            count = 0

            # remove unnecessary elements in string
            strlx = str(list_frzset[0])[10:]
            lhs_str = (strlx[1:][:-2]).replace("'","")
            lhs = lhs_str.split(', ')   
        
            strrx = str(list_frzset[1])[10:]
            rhs_str = (strrx[1:][:-2]).replace("'","")
            rhs = rhs_str.split(', ')

            # check if both lhs and rhs of rules are in each line of dataset
            for i in range(len(original_dataset)): 
                contain = True
                for eleA in rhs:
                    if eleA not in original_dataset[i]:
                        contain = False
                for eleB in lhs:
                    if eleB not in original_dataset[i]:
                        contain = False
                if contain:
                    count += 1
        
            #store to final_result if above support threshold
            if (count/data_line_num > float(args.support_threshold)):
                final_result.append(list_frzset)

    return final_result

def main():
    # Program starting time
    programStartTime = time.time()
    line_num = retrieve_file(args.data_file)

    # first mapreduce
    assign_sample(original_dataset)
    first_map_list = apriori_each()
    
    first_reduce_result = []
    first_reduce_result = first_reduce(first_map_list)

    # second mapreduce
    final_reduce_result = []
    final_reduce_result = if_res_above_support(first_reduce_result, line_num)

    print(first_reduce(final_reduce_result))

    # count the total time and RAM used by running the program
    print("Total time: ", (time.time() - programStartTime), " seconds")
    process = psutil.Process(os.getpid())
    print(process.memory_info()[0])

if __name__ == "__main__":
    main()




