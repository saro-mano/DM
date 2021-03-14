from __future__ import division
import sys
import pyspark
import json
from pyspark import SparkContext
import time
import math

test_file = sys.argv[1] #Test File
model_file = sys.argv[2] #Model File containing user and business profile
output_file = sys.argv[3] #Output File

start = time.time()
sc = pyspark.SparkContext('local[*]')
main_rdd = sc.textFile(test_file)
rdd = main_rdd.map(json.loads)

f = open(model_file,"r")
temp = f.readlines()
user_prof = json.loads(temp[0]) #User ID's containing the indexes of words of similar businesses
bus_prof = json.loads(temp[1]) #Business Profile containing indexes of the word


def cosine_sim(x):
    user_id = x["user_id"]
    bus_id = x["business_id"]
    if user_id not in user_prof:
        user = []
    else:
        user = user_prof[user_id]
    if bus_id not in bus_prof:
        bus = []
    else:
        bus = bus_prof[bus_id]
    inter = set(user).intersection(set(bus))
    u_sqrt = math.sqrt(len(user))
    b_sqrt = math.sqrt(len(bus))
    try:
        cosine = len(inter) / ((u_sqrt) * (b_sqrt)) 
    except:
        cosine = 0
    x["sim"] = cosine
    return x

output = rdd.map(cosine_sim).filter(lambda x : x["sim"] >= 0.01).collect()

with open(output_file, 'w') as op:
  for i in output:
      json.dump(i, op)
      op.write('\n')
print("Duration ", time.time()-start)