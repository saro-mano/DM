import sys
import pyspark
import json
import csv
import time
from pyspark import SparkContext
from itertools import combinations
from collections import defaultdict

#produces singleton candidates
def single_candidate_items(input, support, total_baskets):
    chunks = list(input)
    chunk_len = len(chunks)
    ps = (chunk_len / total_baskets) * support
    d = defaultdict(int)
    output = list()
    if chunk_len:
        for chunk in chunks:
            items = chunk[1]
            for item in items:
                  d[item] += 1
        filt = list(filter(lambda x: x[1] >= ps, d.items()))
        output = list(map(lambda x: (x[0], 1), filt)) #returning (F,1)
    return output

#produces singleton frequent items
def single_frequent_items(input, output_phase_1):
  chunks = list(input)
  d = defaultdict(int)
  output = list()
  if len(chunks):
    for chunk in chunks:
      items = chunk[1]
      C = set(items).intersection(set(output_phase_1))
      candidate_items = list(C)
      for candidate in candidate_items:
          d[candidate] += 1
    output = list(d.items())
  return output

#produces combination of 2 i.e, pairs
def candidate_pairs(input, support, total_baskets, prev_data):
  chunks = list(input)
  chunk_len = len(chunks)
  # print(chunk_count)
  ps = (chunk_len / total_baskets) * support
  d = defaultdict(int)
  output = list()
  if chunk_len:
    for chunk in chunks:
      items = chunk[1]
      C = set(items).intersection(set(prev_data))
      C = sorted(list(C))
      pairs = list(combinations(C,2))
      for pair in pairs:
          d[pair] += 1
    filt = list(filter(lambda x: x[1] >= ps, d.items()))
    # print(filt)
    output = list(map(lambda x: (x[0], 1), filt)) #returning (F,1)
  return output

#produces frequent of 2 i.e, pairs
def frequent_pairs(input, output_phase_1):
  chunks = list(input)
  d = defaultdict(int)
  output = list()
  if len(chunks):
    for chunk in chunks:
      combination = combinations(sorted(chunk[1]), 2)
      # print(combination)
      items = list(set(combination).intersection(set(output_phase_1)))
      for item in items:
          d[item] += 1
    output = list(d.items())
  return output

#produces triples, quadruples, quintuples,etc., candidates
def rem_candidate_pairs(input,support,total_baskets,prev_data,pair_size):
  chunks = list(input)
  chunk_len = len(chunks)
  # print(chunk_count)
  ps = (chunk_len / total_baskets) * support
  output = list()
  prev_data_len = len(prev_data)
  if chunk_len:
    c = pair_size - 2
    for i in range(0,prev_data_len-1):
      for j in range(i+1,prev_data_len):
        first = prev_data[i] #eg.(10,14)
        second = prev_data[j] #eg.(10,13)
        if first[0:c] == second[0:c]:
          value = tuple(sorted(set(first).union(set(second)))) #eg. (10,13,14)
          output.append((value,1)) #returning (F,1)
  return output

#produces triples, quadruples, quintuples,etc., frequent pairs
def rem_frequent_pairs(input,output_phase_1,i):
  chunks = list(input)
  d = defaultdict(int)
  if len(chunks):
    for chunk in chunks:
      temp = chunk[1]
      if len(temp) >= i:
        for item in output_phase_1:
          if set(item).issubset(set(temp)):
              d[item] += 1
    output = list(d.items())
  return output

#function to format the output and sort them
def output_formatter(d):
    output = ""
    for index, data in d.items():
        if index == 1:
            #for singleton sets 
            data = sorted(data) #sorting in lexicographical order
            output = ','.join("('{}')".format(i) for i in data)
        else:
            #everything else
            data = sorted(data) #sorting in lexicographical order
            output = output + '\n\n' + str(data).strip('[]').replace('), (', '),(')
    return output

def main():

  '''partition function'''
  def custom_partition(key):
    return hash(key) % partition_count

  start = time.time()

  '''input from the command line'''
  support = int(sys.argv[1])
  input_file = sys.argv[2]
  output_file = sys.argv[3]

  '''dictionary to store the final results'''
  candidates = dict()
  frequent_itemset = dict()


  sc = pyspark.SparkContext('local[*]') #Spark initialization
  main_rdd = sc.textFile(input_file) #Loading the incoming data
  header = main_rdd.first() #Collecting the header of the csv file

  '''Transforming main_rdd into user_id : [states] based on the case number'''
  rdd = main_rdd.filter(lambda x: x != header).map(lambda x: tuple([x.split(",")[0],[x.split(",")[1]]])).reduceByKey(lambda x,y: list(set(x+y)))

  partition_count = rdd.getNumPartitions() #getting the number of RDD partition

  basket = rdd.partitionBy(partition_count, custom_partition)
  total_baskets = basket.count() #total number of baskets created

  #SON Phase 1 - generating singleton candidates
  map_phase_1 = basket.mapPartitions(lambda x: single_candidate_items(x,support,total_baskets))
  reduce_phase_1 = map_phase_1.reduceByKey(lambda x,y: (x+y)) #(F,1) is reduced
  candidates[1] = reduce_phase_1.map(lambda x:x[0]).collect()

  #SON Phase 2 - generating singleton frequents
  map_phase_2 = basket.mapPartitions(lambda x: single_frequent_items(x,candidates[1]))
  reduce_phase_2 = map_phase_2.reduceByKey(lambda x,y: (x+y)).filter(lambda x:x[1] >= support)
  frequent_itemset[1] = reduce_phase_2.map(lambda x:x[0]).collect()


  if len(frequent_itemset[1]) > 1:
    #SON Phase 1 - generating pair candidates
    map_phase_1 = basket.mapPartitions(lambda x: candidate_pairs(x,support,total_baskets,frequent_itemset[1]))
    reduce_phase_1 = map_phase_1.reduceByKey(lambda x,y: (x+y))
    candidates[2] = reduce_phase_1.map(lambda x:x[0]).collect()
    
    #SON Phase 2 - generating pair frequent itemsets
    map_phase_2 = basket.mapPartitions(lambda x: frequent_pairs(x,candidates[2]))
    reduce_phase_2 = map_phase_2.reduceByKey(lambda x,y: (x+y)).filter(lambda x:x[1] >= support)
    frequent_itemset[2] = reduce_phase_2.map(lambda x:x[0]).collect()

    i = 2 #Loop to generate triples, quadruples, quintuples,etc.,
    while len(frequent_itemset[i]) > 1:
      i += 1
      #SON Phase 1 - triples, quadruples, quintuples,etc., candidates
      map_phase_1 = basket.mapPartitions(lambda x: rem_candidate_pairs(x,support,total_baskets,frequent_itemset[i-1], i))
      reduce_phase_1 = map_phase_1.reduceByKey(lambda x,y: (x+y))
      candidates[i] = reduce_phase_1.map(lambda x:x[0]).collect()

      #SON Phase 2 - triples, quadruples, quintuples, etc., frequent itemsets
      map_phase_2 = basket.mapPartitions(lambda x: rem_frequent_pairs(x,candidates[i], i))
      reduce_phase_2 = map_phase_2.reduceByKey(lambda x,y: (x+y)).filter(lambda x:x[1] >= support)
      frequent_itemset[i] = reduce_phase_2.map(lambda x:x[0]).collect()

    #removing the invalid itemsets and candidates generated as a part of the loop.
    if len(frequent_itemset[i]) == 0:
        del frequent_itemset[i]
    if len(candidates[i]) == 0:
        del candidates[i]

  output = 'Candidates:\n' + output_formatter(candidates) + '\n\nFrequent Itemsets:\n' + output_formatter(frequent_itemset)

  with open(output_file, 'w') as op:
      op.write(output)
  end = time.time()
 
  print("Duration: " + str(end - start))

if __name__ == "__main__":
    main()