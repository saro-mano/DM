import sys
import json
import time
import os
import copy
import random
import math
import pyspark

start = time.time()
sc = pyspark.SparkContext('local[*]')
sc.setLogLevel("ERROR")

input_path = sys.argv[1]
k_clusters = int(sys.argv[2])
output_path = sys.argv[3]
dimension = 0

'''calculating the eucledian distance'''
def euclidean_dist(point1, point2):
    return math.sqrt(sum([(a - b) ** 2 for a, b in zip(point1,point2)]))


'''Computing new centroid'''
def compute_new_centroid(point_coords_list):
    centroid = list()
    temp = 0
    for dim in range(dimension):
        s = 0
        for point in point_coords_list:
            s += point[dim]
        centroid.append(s / len(point_coords_list))
    return centroid

'''Checking for convergence'''
def check_convergence(prev, curr):
    dist = list()
    for c in prev.keys():
        if c in curr.keys():
            dist.append(euclidean_dist(prev[c], curr[c]))
    if max(dist) > 0.5:
        return False
    return True

'''Getting the Closest Centroid'''
def find_closest_centroid(centroid, point):
    distance_list = list()
    for c in centroid.items():
        distance_list.append((c[0], euclidean_dist(c[1], point)))
    # dist = [(c[0], calc_dist(c[1], point)) for c in list(centroid_dict.items())]
    return min(distance_list, key=lambda x: x[1])[0]

'''Running k-means algorithm'''
def k_means(data, k, max_iteration):
    rdd = sc.parallelize(data)
    #Applying K-Means ++ 
    init_centroid =[(0, random.sample(data, 1)[0][1])] #[(0, [1.1755792585655582, 9.766623269244233, -6.613327927875963..])]
    for i in range(k-1):
        temp = rdd.map(lambda x: (x[0], x[1], min([euclidean_dist(c[1], x[1]) for c in init_centroid]))).max(lambda x: x[2]) #calculating eucledian distance btw init cluster and points
        # print(temp)
        init_centroid.append((i+1, temp[1]))
    curr_centroid = dict(init_centroid)
   
    clusters = rdd.map(lambda x: (find_closest_centroid(curr_centroid, x[1]), [x])).reduceByKey(lambda x, y: x+y)
    prev_centroid = copy.deepcopy(curr_centroid)
    curr_centroid = clusters.map(lambda x: (x[0], compute_new_centroid([y[1] for y in x[1]]))).collectAsMap()
    # print(curr_centroid)
    count = 0
    '''run until there is no change in the centroids'''
    while not check_convergence(prev_centroid, curr_centroid):
        if count < max_iteration:
            break
        clusters = rdd.map(lambda x: (find_closest_centroid(curr_centroid, x[1]), [x])).reduceByKey(lambda x, y: x + y)
        prev_centroid = copy.deepcopy(curr_centroid)
        curr_centroid = clusters.map(lambda x: (x[0], compute_new_centroid([y[1] for y in x[1]]))).collectAsMap()
        count += 1

    output = clusters.map(lambda x: (x[0], [y[0] for y in x[1]])).collectAsMap()
    
    return output

file_object = open(input_path, mode='r')
data = [[float(x) for x in line.strip('\n').split(',')] for line in file_object.readlines()]
data_dict = {}
for i in data:
    data_dict[int(i[0])] = i[1:]

data_list = list(data_dict.items())

output_dict = k_means(data_list, k_clusters, 30)

rdd = sc.parallelize(list(output_dict.items()))
out_dict = rdd.flatMap(lambda x: [(i, x[0]) for i in x[1]]).collectAsMap()
final_rdd = sc.parallelize(out_dict.items())
output = final_rdd.sortByKey().map(lambda x : (x[0],x[1])).collectAsMap()
# print(output)


with open(output_path, 'w') as output_file1:
    json.dump(output, output_file1)

print("Duration: ", time.time()-start)