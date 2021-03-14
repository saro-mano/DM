import json
import binascii
import datetime
import sys
import random
import time
import pyspark
from pyspark.streaming import StreamingContext


port = int(sys.argv[1]) #receives the streaming data
output_file = sys.argv[2] #output file path

sc = pyspark.SparkContext('local[*]') #spark init
sc.setLogLevel("ERROR")

hash_count = 30
hash_fun = 30
m = 600343 
a = random.sample(range(1, 1000), hash_fun)
b = random.sample(range(1000, 2000), hash_fun)


output = open(output_file, "w")
output.write("Time,Ground Truth,Estimation\n")
output.close()

def get_trailing_zero(binary):
    count = 0
    for i in reversed(binary):
        if i == '0':
            count += 1
        else:
            break
    return count

def get_median(bit_array):
    avg = list()
    for i in range(0, hash_fun, 2):
        v = int(((2**bit_array[i]) + (2**bit_array[i+1])) / 2)
        avg.append(v)
    sorted_avg = sorted(avg)
    length_avg = len(avg)
    return sorted_avg[int(length_avg/2)]
  

def run_flajolet_algo(rdd):
    data = rdd.collect()
    states = set()
    max_trailing_zeros = [0 for _ in range(hash_fun)]
    
    for record in data:
        h_values = list()
        h_binary = list()
        h_trailing_len = list()
        state = json.loads(record)["state"]
        c = int(binascii.hexlify(state.encode('utf8')), 16) #Converting string into int
        
        #Hashing the string with hashfunctions
        for i in range(hash_fun):
            h_values.append(((a[i] * c) + b[i]) % m)

        #Converting the integers to binary (1's and 0's)
        for i in h_values:
            h_binary.append(format(i, '016b'))

        #Calculating the trailing zeros
        for bit in h_binary:
            x = get_trailing_zero(bit)
            h_trailing_len.append(x)

        #Getting the max trailing zeros
        for i in range(hash_fun):
            if max_trailing_zeros[i] < h_trailing_len[i]:
                max_trailing_zeros[i] = h_trailing_len[i]
        states.add(state) #For ground truth value

    median = get_median(max_trailing_zeros) #Get the median value

    file = open(output_file, "a+")
    file.write('{},{},{}\n'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), len(states), median))
    file.close()
         

scc = StreamingContext(sc, 5)
stream = scc.socketTextStream("localhost", port)
stream_window = stream.window(30, 10)
stream_window.foreachRDD(run_flajolet_algo)
scc.start()
scc.awaitTermination()