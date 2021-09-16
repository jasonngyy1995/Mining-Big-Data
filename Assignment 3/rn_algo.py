# # # function to download file from site
# # # import urllib.request
# # # # testfile = urllib.request.URLopener()
# # # # testfile.
# # #
# # # urllib.request.urlretrieve('http://fimi.uantwerpen.be/data/pumsb_star.dat', 'pumsb_star.dat')

import argparse
import time
import random
from efficient_apriori import apriori

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--data_file')
parser.add_argument('-s', '--support_threshold', default=1)
parser.add_argument('-c', '--confidence', default=0.5)
parser.add_argument('-p', '--fixed_probability', default=0.5)
args = parser.parse_args()

dataset = []

def process_file():
    file = open(args.data_file, 'r')
    line_num = 0
    for line in file:
        empty_right_space_line = line.rstrip()
        converted_data = empty_right_space_line.split()

        if random.random() < float(args.fixed_probability):
            dataset.append(converted_data)
            line_num += 1

    file.close()
    print("lines in file = ", line_num)

def main():
    programStartTime = time.time()
    process_file()
    itemsets, rules = apriori(dataset, min_support=float(args.support_threshold), min_confidence=float(args.confidence))

    print(type(rules))
    print(rules)
    print("Total time: ", (time.time() - programStartTime), " seconds")

if __name__ == "__main__":
    main()


# import argparse
# import time
# import random
# import psutil

# parser = argparse.ArgumentParser()
# parser.add_argument('-f', '--data_file')
# parser.add_argument('-s', '--support_threshold', default=1)
# parser.add_argument('-c', '--confidence', default=0.5)
# parser.add_argument('-p', '--fixed_probability', default=0.5)
# args = parser.parse_args()

# # function of process the data of file and return a list
# def process_file(data_file):
#     file = open(data_file, 'r')
#     # store the sample from data file
#     original_dataset = [] 
#     # count the size of entire dataset
#     data_line_num = 0
#     # count the size of sample data
#     sample_line_num = 0

#     # reading the entire file
#     for line in file:
#         empty_right_space_line = line.rstrip() 
#         converted_data = empty_right_space_line.split()

#         data_line_num += 1
        
#         # read sample size of file at size of basket numbers multiply by the fixed probability
#         if random.random() < float(args.fixed_probability):
#             original_dataset.append(tuple(converted_data))
#             sample_line_num += 1

#     file.close()
#     print("lines in file = ", data_line_num)
#     print("Sample size = ", sample_line_num)

#     return original_dataset

# def createC1(dataSet):
#     C1 = []
#     for transaction in dataSet:
#         for item in transaction:
#             if not [item] in C1:
#                 C1.append([item])

#     C1.sort()
#     return list(map(frozenset, C1))  # use frozen set so we
#     # can use it as a key in a dict

# def scanD(D, Ck, minSupport):
#     ssCnt = {}
#     for tid in D:
#         for can in Ck:
#             if can.issubset(tid):
#                 if not can in ssCnt: ssCnt[can]=1
#                 else: ssCnt[can] += 1
#     numItems = float(len(D))
#     retList = []
#     supportData = {}
#     for key in ssCnt:
#         support = ssCnt[key]/numItems
#         if support >= minSupport:
#             retList.insert(0,key)
#         supportData[key] = support
#     return retList, supportData


# def aprioriGen(Lk, k): #creates Ck
#     retList = []
#     lenLk = len(Lk)
#     for i in range(lenLk):
#         for j in range(i+1, lenLk):
#             L1 = list(Lk[i])[:k-2]; L2 = list(Lk[j])[:k-2]
#             L1.sort(); L2.sort()
#             if L1==L2: #if first k-2 elements are equal
#                 retList.append(Lk[i] | Lk[j]) #set union
#     return retList

# def apriori(dataSet, minSupport = 0.5):
#     C1 = createC1(dataSet)
#     D = list(map(set, dataSet))
#     L1, supportData = scanD(D, C1, minSupport)
#     L = [L1]
#     k = 2
#     while (len(L[k-2]) > 0):
#         Ck = aprioriGen(L[k-2], k)
#         Lk, supK = scanD(D, Ck, minSupport)#scan DB to get Lk
#         supportData.update(supK)
#         L.append(Lk)
#         k += 1
#     return L, supportData

# def generateRules(L, supportData, minConf=0.5):  #supportData is a dict coming from scanD
#     bigRuleList = []
#     for i in range(1, len(L)):#only get the sets with two or more items
#         for freqSet in L[i]:
#             H1 = [frozenset([item]) for item in freqSet]
#             if (i > 1):
#                 rulesFromConseq(freqSet, H1, supportData, bigRuleList, minConf)
#             else:
#                 calcConf(freqSet, H1, supportData, bigRuleList, minConf)
#     return bigRuleList   

# def calcConf(freqSet, H, supportData, brl, minConf=0.5):
#     prunedH = [] #create new list to return
#     for conseq in H:
#         conf = supportData[freqSet]/supportData[freqSet-conseq] #calc confidence
#         if conf >= minConf: 
#             print (freqSet - conseq,'-->',conseq, 'conf:',conf)
#             brl.append((freqSet - conseq, conseq, conf))
#             prunedH.append(conseq)
#     return prunedH

# def rulesFromConseq(freqSet, H, supportData, brl, minConf=0.5):
#     m = len(H[0])
#     if (len(freqSet) > (m + 1)): #try further merging
#         Hmp1 = aprioriGen(H, m+1)#create Hm+1 new candidates
#         Hmp1 = calcConf(freqSet, Hmp1, supportData, brl, minConf)
#         if (len(Hmp1) > 1):    #need at least two sets to merge
#             rulesFromConseq(freqSet, Hmp1, supportData, brl, minConf)

# def main():
#     start_time = time.time()
#     dataset = process_file('chess.dat')
#     # dataset = [[1, 3, 4], [2, 3, 5], [1, 2, 3, 5], [2, 5]]
#     C1 = createC1(dataset)
#     D = list(map(set,dataset))
#     L1,suppDat0 = scanD(D,C1,1)

#     L,suppData= apriori(dataset,minSupport=0.5)
#     rules= generateRules(L,suppData, minConf=0.5)

#     print("Total time: ", (time.time() - start_time), " seconds")
#     print('RAM memory % used:', psutil.virtual_memory()[2])

# if __name__ == "__main__":
#     main()



