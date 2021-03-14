from __future__ import division
import sys
import pyspark
import json
from pyspark import SparkContext
import time

sc = pyspark.SparkContext('local[*]')

start = time.time() #Starting the time
train_file = sys.argv[1] #Train File
test_file = sys.argv[2] #Test File
model_file = sys.argv[3] #Computed Model File {b1,b2,sim} or {u1,u2,sim}
output_file = sys.argv[4] #Output File
cf_type = sys.argv[5] #Collobarative Filtering type


if cf_type == "item_based":  
    def average(ratings):
        avg = dict()
        for rating in ratings:
            if rating[0] not in avg:
                avg[rating[0]] = [rating[1]]
            else:
                avg[rating[0]].append(rating[1])
        for i in avg:
            num = sum(avg[i])
            den = len(avg[i])
            avg[i] = num / den
        return avg
    
    f = open(model_file,"r")

    bus_dict = dict()
    models = f.readlines()

    for model in models:
        data = json.loads(model)
        b1 = data["b1"]
        b2 = data["b2"]
        bus_dict[(b1,b2)] = data["sim"]
    train_main = sc.textFile(train_file)
    test_main = sc.textFile(test_file)

    test = test_main.map(json.loads).map(lambda x:(x["user_id"],x["business_id"])) #(User_id) : (Bus_id)
    train = train_main.map(json.loads).map(lambda x:(x["user_id"],(x["business_id"],x["stars"]))) #(User_id) : (Bus_id, stars)
    train = train.groupByKey().mapValues(list).map(lambda x:(x[0],average(x[1]))) #Grouping by same key then calculating the average for business

    def predict(x):
        bus = x[1][0] #Bus-ID
        bus_with_rate = x[1][1] #Ratings
        output = list()
        for b in bus_with_rate:
            if (b, bus) in bus_dict: #Checking in the model
                output.append((bus_dict[(b,bus)], bus_with_rate[b]))
            if (bus, b) in bus_dict:
                output.append((bus_dict[(bus,b)], bus_with_rate[b]))

        if len(output) == 0:
            return 0
    
        output = sorted(output, key = lambda x:x[0], reverse=True) #Sorting for getting the top business
        if len(output) >= 3:  #Taking only top 3 i.e, N = 3
            output = output[:3]

        #Output = (rating in model, rating in)
        numerator = 0
        denominator = 0
        stars = 0
        for o in output:
            numerator += (o[0] * o[1])
            denominator += o[0]
        if numerator > 0 and denominator > 0:
            stars = numerator/denominator
        return stars

    output = test.join(train).map(lambda x:(x[0],x[1][0],predict(x))).filter(lambda x:x[2] != 0).collect()

else:
    def average(ratings):
        avg = dict()
        for rating in ratings:
            if rating[0] not in avg:
                avg[rating[0]] = [rating[1]]
            else:
                avg[rating[0]].append(rating[1])
        for i in avg:
            num = sum(avg[i])
            den = len(avg[i])
            avg[i] = num / den
        return avg

    f = open(model_file,"r")

    user_dict = dict()
    models = f.readlines()

    for model in models:
        data = json.loads(model)
        u1 = data["u1"]
        u2 = data["u2"]
        user_dict[(u1,u2)] = data["sim"]

    train_main = sc.textFile(train_file)
    test_main = sc.textFile(test_file)
    test = test_main.map(json.loads).map(lambda x:(x["business_id"],x["user_id"]))
    train = train_main.map(json.loads).map(lambda x:(x["business_id"],(x["user_id"],x["stars"])))
    train = train.groupByKey().mapValues(list).map(lambda x:(x[0],average(x[1])))
    
    def predict(x):
        user = x[1][0]
        user_with_rate = x[1][1]
        output = list()
        for u in user_with_rate:
            if (u, user) in user_dict:
                output.append((user_dict[(u,user)], user_with_rate[u]))
            if (user, u) in user_dict:
                output.append((user_dict[(user,u)], user_with_rate[u]))

        if len(output) == 0:
            return 0
    
        output = sorted(output, key = lambda x:x[0], reverse=True) #Sorting for getting the top user
        if len(output) >= 3:  #Taking only top 3 i.e, N = 3
            output = output[:3]

        numerator = 0
        denominator = 0
        stars = 0
        for o in output:
            numerator += (o[0] * o[1])
            denominator += o[0]
        if numerator > 0 and denominator > 0:
            stars = numerator/denominator
        return stars

    output = test.join(train).map(lambda x:(x[0],x[1][0],predict(x))).filter(lambda x:x[2] != 0).collect()
    
with open(output_file,"w") as f:
    for op in output:
        json.dump({"user_id": op[0], "business_id": op[1], "stars": op[2]}, f)
        f.write("\n")
print("Duration ", time.time()-start)