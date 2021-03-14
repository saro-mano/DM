import random
import sys
import time
import math
import json
import binascii
import pyspark


start = time.time()
input_file1 = sys.argv[1]
input_file2 = sys.argv[2]
output_file = sys.argv[3]


sc = pyspark.SparkContext('local[*]') #Spark init 
sc.setLogLevel("ERROR")


main1 = sc.textFile(input_file1)
main2 = sc.textFile(input_file2)

rdd1 = main1.map(json.loads)
names_temp = rdd1.map(lambda x : x["name"]).filter(lambda x: len(x) != 0).distinct()
names = names_temp.map(lambda x: int(binascii.hexlify(x.encode('utf8')), 16)).collect() #converting string into integer

hash_fun = 10
name_len = len(names)
m = int((hash_fun * name_len)/math.log(2))


a = random.sample(range(1, 1000), hash_fun) #for hash function
b = random.sample(range(1000, 2000), hash_fun)#for hash function

bit_arr = [0 for x in range(m)]
for name in names:
    indices = [((a[i] * name) + b[i]) % m for i in range(hash_fun)]
    for i in indices:
        bit_arr[i] = 1

rdd2 = main2.map(json.loads)
def predict(x):
    if x == "":
        return "F" #returns False if empty
    else:
        c = int(binascii.hexlify(x.encode('utf8')), 16)
        indices = [((a[i] * c) + b[i]) % m for i in range(hash_fun)]
        for i in indices:
            if bit_arr[i] == 1:
                continue
            else:
                return "F" #if not in bit array
        return "T" #if found in bit array
prediction = rdd2.map(lambda x: predict(x["name"])).collect()

result_str = ' '.join(prediction)
with open(output_file, 'w') as op:
    op.write(result_str)

print("Duration : ", time.time() - start)