import sys
import json
import pyspark
from pyspark import SparkContext

'''reading from the command line'''
review_file = sys.argv[1]
output_file = sys.argv[2]
partition_type = sys.argv[3]
n_partitions = sys.argv[4]
n_threshold = sys.argv[5]

n_partitions = int(n_partitions)
n_threshold = int(n_threshold)

output = {}

sc = pyspark.SparkContext('local[*]')
review = sc.textFile(review_file)
review_rdd = review.map(json.loads)
review_business_rdd = review_rdd.map(lambda x : (x["business_id"],1))

def partition_function(rdd_obj):
  return hash(rdd_obj) % n_partitions

if partition_type != "default":
  custom_rdd = review_business_rdd.partitionBy(n_partitions, partition_function) #custom partitioning of the RDD
  output["n_partitions"] = custom_rdd.getNumPartitions()
  output["n_items"] = custom_rdd.glom().map(len).collect()
  filtered_rdd = custom_rdd.reduceByKey(lambda x,y : x+y).filter(lambda x: x[1] > n_threshold)
  output["result"] = filtered_rdd.map(lambda x: list(x)).collect()
else:
  output["n_partitions"] = review_business_rdd.getNumPartitions()
  output["n_items"] = review_business_rdd.glom().map(len).collect()
  filtered_rdd = review_business_rdd.reduceByKey(lambda x,y : x+y).filter(lambda x: x[1] > n_threshold)
  output["result"] = filtered_rdd.map(lambda x: list(x)).collect()

'''writing result into JSON file'''
with open(output_file,'w') as writer:
    json.dump(output, writer)
writer.close()
