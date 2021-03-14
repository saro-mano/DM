from __future__ import division
import sys
import pyspark
import json
from pyspark import SparkContext,SparkConf
import time
import math
import random
from itertools import combinations

start = time.time() #Start time
input_file = sys.argv[1] #Input File
output_file = sys.argv[2] #Model File
cf_type = sys.argv[3] #item_based or user_based

conf = SparkConf().set('spark.driver.memory', '4g').set('spark.executor.memory', '4g')
sc = SparkContext(conf=conf)

main_rdd = sc.textFile(input_file)
rdd = main_rdd.map(json.loads)


if cf_type == "item_based":
    business_user = rdd.map(lambda x: (x["business_id"], [x["user_id"]])).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], list(set(x[1])))).filter(lambda x: len(x[1]) >= 3) #Filtered the business that have atleast 3 rated items
    business_user_dict = business_user.collectAsMap() #BusinessID = [List of corated users]
    business_id = business_user.map(lambda x: x[0]).distinct() #Distinct Business
    
    '''Checking if neighbour has atleast 3 corated items'''
    def filter_corated(x):
        id1 = set(business_user_dict[x[0]])
        id2 = set(business_user_dict[x[1]])
        if len(id1.intersection(id2)) >= 3:
            return True
        else:
            return False

    bus_candidate_pairs = business_id.cartesian(business_id).filter(lambda x : x[0] < x[1]).filter(filter_corated) #Cartesian Joining for creating a pairs of business_id
    user_with_ratings = rdd.map(lambda x: ((x["user_id"],x["business_id"]),x["stars"])).collectAsMap() #(userID,busID) : (stars)
    
    '''Finding Pearson Co-relation for business pairs'''
    def find_pearson_co(x):
        b1_id = x[0] 
        b1_users = business_user_dict[b1_id] #Getting the list of user_id
        b2_id = x[1]
        b2_users = business_user_dict[b2_id]
        corated_list = list(set(b1_users).intersection(set(b2_users))) #Finding the co-rated items
        ratings1, ratings2 = list(),list()
        for user in corated_list:
            ratings1.append(user_with_ratings[(user,b1_id)])
            ratings2.append(user_with_ratings[(user,b2_id)])
        avg1 = sum(ratings1) / len(ratings1)
        avg2 = sum(ratings2) / len(ratings2)
        numerator = 0
        d1, d2 = 0,0
        for i,j in zip(ratings1,ratings2):
            numerator += (i-avg1) * (j - avg2)
            d1 += (i-avg1) ** 2
            d2 += (j-avg2) ** 2
        denominator = math.sqrt(d1) * math.sqrt(d2)

        sim = -100
        if numerator > 0 and denominator > 0:
            sim = numerator / denominator
        # d = dict()
        # if sim > 0:
        #     d = {"b1" : b1_id , "b2" : b2_id , "sim" : sim}
        # return d
        return {"u1": u1_id, "u2": u2_id, "sim": sim}

    output = bus_candidate_pairs.map(find_pearson_co).filter(lambda x : x["sim"] > 0).collect()
else:
    business_rdd = rdd.map(lambda x: x["business_id"]).distinct() #Only distinct business
    business = business_rdd.collect()
    bus_count = business_rdd.count()

    bus_index = {}
    for i,b in enumerate(business):
        bus_index[b] = i
    
    user_business_rdd = rdd.map(lambda x: (x["user_id"], [bus_index[x["business_id"]]])).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], list(set(x[1])))) #User with unique Business Indexes
    user_business = user_business_rdd.collectAsMap() #{User : [List of Bus Indexes]}

    num_hash_fun = 50
    bands = 50
    rows = 1
    a = random.sample(range(10000,200000),50)
    b = random.sample(range(10000,200000),50)

    def signature(x):
        m = bus_count
        b_id = x[1]
        sign = list()
        for i in range(num_hash_fun):
            sign.append(min([(((a[i] * id) + b[i]) % m) for id in b_id]))
        return tuple([x[0], sign])

    sign = user_business_rdd.map(signature) #User with Business Index hashed

    def lsh(x):
        u_id = x[0]
        sign = x[1]
        output = list()
        for i in range(bands):
            output.append(tuple([tuple([sign[i], i]), [u_id]]))
        return output
    
    def jaccard_sim(x):
        users = sorted(x[1])
        output = list()
        combo = combinations(users,2)
        for i in combo:
            u1 = i[0]
            u2 = i[1]
            users1 = set(user_business[u1])
            users2 = set(user_business[u2])
            intersect = len(users1.intersection(users2))
            union = len(users1.union(users2))
            j = intersect / union
            if j >= 0.01 and intersect >= 3:
                output.append(i)
            else:
                output.append(None)
        return output
    
    jaccard = sign.flatMap(lsh).reduceByKey(lambda x,y : x+y).flatMap(jaccard_sim).filter(lambda x: x!=None)

    bus_user_rating = rdd.map(lambda x: ((x["business_id"], x["user_id"]), [x["stars"]])).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], sum(x[1])/len(x[1]))).collectAsMap() 
    user_business_filtered = rdd.map(lambda x: (x["user_id"], [x["business_id"]])).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], set(x[1]))).filter(lambda x: len(x[1]) > 2).collectAsMap()

    def find_pearson_co(x):
        u1_id = x[0]
        u1_users = user_business_filtered[u1_id]
        u2_id = x[1]
        u2_users = user_business_filtered[u2_id]
        corated_list = list(set(u1_users).intersection(set(u2_users)))
        ratings1,ratings2 = list(),list()
        for bus in corated_list:
            ratings1.append(bus_user_rating[(bus, u1_id)])
            ratings2.append(bus_user_rating[(bus, u2_id)])
        avg1 = sum(ratings1) / len(ratings1)
        avg2 = sum(ratings2) / len(ratings2)
        numerator = 0
        d1, d2 = 0,0
        for i,j in zip(ratings1,ratings2):
            numerator += (i-avg1) * (j - avg2)
            d1 += (i-avg1) ** 2
            d2 += (j-avg2) ** 2
        denominator = math.sqrt(d1) * math.sqrt(d2)

        sim = -100
        if numerator > 0 and denominator > 0:
            sim = numerator / denominator
        # d = dict()
        # if sim > 0:
        #     d = {"u1": u1_id, "u2": u2_id, "sim": sim}
        return {"u1": u1_id, "u2": u2_id, "sim": sim}

    output = jaccard.map(find_pearson_co).filter(lambda x : x["sim"] > 0).collect()

with open(output_file, 'w') as op:
  for i in output:
      json.dump(i, op)
      op.write('\n')
print("Duration ", time.time()-start)