import sys
from itertools import combinations
import time
from graphframes import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark


start = time.time() #Start time
threshold = int(sys.argv[1]) #Threshold frequency
input_file = sys.argv[2] #Input file path
output_file = sys.argv[3] #Output file path

conf = pyspark.SparkConf().setAppName('task1').setMaster('local[3]') #pyspark Configuration

sc = pyspark.SparkContext(conf = conf)
sc.setLogLevel("OFF") #Turning the log off!
sql_context = SQLContext(sc)

main_rdd = sc.textFile(input_file)

header = main_rdd.first() #(title)
rdd = main_rdd.filter(lambda x:x != header).map(lambda x: tuple([x.split(",")[0],[x.split(",")[1]]])).reduceByKey(lambda x,y: x+y)

user_business_dict = rdd.collectAsMap()

users = rdd.keys().collect() #only the userID

def get_pairs(x):
    output = list()
    pairs = combinations(x,2)
    for pair in pairs:
        l = len(set(user_business_dict[pair[0]]).intersection(set(user_business_dict[pair[1]])))
        if l >= threshold:
            output.append((pair[0],pair[1]))
            output.append((pair[1],pair[0]))
    return output
    

pairs = get_pairs(users)

vertex = sc.parallelize(pairs).flatMap(lambda x: [x[0], x[1]]).distinct().collect() #only distinct users of the pairs

#vertices = sql_context.createDataFrame(vertex, schema=["id"])
#edges = sql_context.createDataFrame(pairs, schema = ['src','dst'])

vertices = sc.parallelize(list(vertex)).map(lambda x: (x,)).toDF(['id']) #creating a Dataframe ID
edges = sc.parallelize(pairs).toDF(["src", "dst"]) #Creating Dataframe Source and Destination
graph = GraphFrame(vertices, edges)
comm = graph.labelPropagation(maxIter=5) #ow(id='gH0dJQhyKUOVCKQA6sqAnw', label=146028888064


comm_ = comm.rdd.map(lambda x: (x['label'], [x['id']])).reduceByKey(lambda x, y: x+y) #key = label, list of values = id
comm_sorted = comm_.map(lambda x: (x[0], sorted(list(set(x[1]))))).sortBy(lambda x: (len(x[1]), x[1]))#Sorting by user_id's and len of user_id list
community = comm_sorted.map(lambda x: x[1]).collect() #collecting the final answers

'''Writing into the output file'''
with open(output_file, 'w') as op:
    for c in community:
        op.write(str(c)[1:-1])
        op.write('\n')

print("Duration : ", time.time()-start)