from __future__ import division
import sys
import pyspark
import json
import random
from pyspark import SparkContext,SparkConf
from itertools import combinations
import time

start = time.time() #Initializing the timer
input_file = sys.argv[1] #Input File
output_file = sys.argv[2] #Output File


def js_calculator(b1,b2):
    set_b1 = set(b1)
    set_b2 = set(b2)
    intersection = set_b1.intersection(set_b2) 
    union = set_b1.union(set_b2)
    return len(intersection) / len(union)

def jaccard_sim(x):
  business_id = sorted(x[1]) #Sorted in Lexicographical Order
  output = list()
  pairs = combinations(business_id,2)
  for pair in pairs:
    b1 = pair[0]
    b2 = pair[1]
    j = js_calculator(business_user[b1],business_user[b2])
    #Checking the Jaccard Similarity
    if j >= 0.055:
      output.append({"b1" : b1 , "b2" : b2 , "sim" : j})
  return output

def hash_fun(x):
  m = count
  user_id_list = x[1]
  signature_list = list()
  for i in range(num_hash_fun):
    temp = [(((a[i]* u) + b[i]) % m) for u in user_id_list]
    signature_list.append(min(temp)) #taking the minimum index of the hash function
  return tuple([x[0],signature_list]) #business_id with min_index

def lsh(x):
  business_id = x[0]
  signature_list = x[1]
  output = list()
  for i in range(bands):
    temp = tuple([signature_list[i], i])
    output.append(tuple([temp,[business_id]]))
  return output

conf = SparkConf().setMaster("local").setAppName("task1").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)

# sc = pyspark.SparkContext('local[*]')
main_rdd = sc.textFile(input_file)
rdd = main_rdd.map(json.loads)

user_rdd = rdd.map(lambda x : x['user_id']).distinct() #Only distinct users
user_rdd_indexed = user_rdd.zipWithIndex() #Zipping with index for users.
user_rdd_dict = user_rdd_indexed.map(lambda x : {x[0]:x[1]})
user_dict = user_rdd_dict.flatMap(lambda x:x.items()).collectAsMap() #{user_id_1 : 0 , user_id2: 1}
count = user_rdd.count() #Counting the number of distinct users
business_user_rdd = rdd.map(lambda x : (x['business_id'],[user_dict[x['user_id']]])) #Business_ID with users_index
business_user_rdd = business_user_rdd.reduceByKey(lambda x,y : x+y)
business_user = business_user_rdd.collectAsMap() #business id's with users index

num_hash_fun = 45
bands = 45
rows = 1

a = random.sample(range(100000,200000),45) 
b = random.sample(range(3000000,5000000),45)

signature = business_user_rdd.map(hash_fun) #creating the signature matrix with min index

lsh_rdd = signature.flatMap(lsh).reduceByKey(lambda x,y : x+y) #Converting the signature list into Bands and Rows

jaccard = lsh_rdd.flatMap(jaccard_sim) #Checking for Jaccard Similarity

output = jaccard.collect()

with open(output_file, 'w') as op:
    for i in output:
        json.dump(i, op)
        op.write('\n')
print("Duration : " + str(time.time()-start))