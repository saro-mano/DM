import sys
import json
import time
import pyspark
import os
import copy
import random
import math

'''global variables'''
discard_sets = dict()
compression_set = dict()
retained_set = dict()
discard_set_stats = dict()
comp_set_stats = dict()
comp_set_count = 0
threshold = 0
dimension = 0

'''Spark Init'''
sc = pyspark.SparkContext('local[*]')
sc.setLogLevel("ERROR")


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


#STEP 2
'''Running k-means algorithm'''
def k_means(data, k, max_iteration, kmeans_plusplus):
    rdd = sc.parallelize(data)
    #Applying K-Means ++ 
    if kmeans_plusplus:
        init_centroid =[(0, random.sample(data, 1)[0][1])] #[(0, [1.1755792585655582, 9.766623269244233, -6.613327927875963..])]
        for i in range(k-1):
            temp = rdd.map(lambda x: (x[0], x[1], min([euclidean_dist(c[1], x[1]) for c in init_centroid]))).max(lambda x: x[2]) #calculating eucledian distance btw init cluster and points
            # print(temp)
            init_centroid.append((i+1, temp[1]))
        curr_centroid = dict(init_centroid)
    else:
        curr_centroid = dict([(idx, x[1]) for idx, x in enumerate(random.sample(data, k))])

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
    output_stats = clusters.map(lambda x: (x[0], [y[1] for y in x[1]])).collectAsMap()
    return output, output_stats


#STEP 3
'''To seperate the outer data and to form RS'''
def detect_outliers(data):
    clusters, _ = k_means(data, 5*k_clusters, 3, False)
    outliers = list()
    for c in clusters:
        if len(clusters[c]) == 1:
            outliers.append(clusters[c][0])
    return outliers

'''RDD calling function to summarize the statistics'''
def summarize_cluster(points):
    summary = dict()
    n = len(points)
    summary["N"] = n
    sum_list = list()
    sumsq_list = list()
    centroid_list = list()
    for d in range(dimension):
        s, sumsq = 0,0
        for p in points:
            v = p[d]
            s += v
            sumsq += (v ** 2)
        sum_list.append(s)
        sumsq_list.append(sumsq)
        centroid_list.append(s / n)
    summary["SUM"] = sum_list
    summary["SUMSQ"] = sumsq_list
    summary["CENTROID"] = centroid_list
    return summary


'''To compute N,SUM,SUMSQ,CENTROID'''
def statistics(stat):
    data = list(stat.items())
    cluster_rdd = sc.parallelize(data)
    cluster_stats = cluster_rdd.map(lambda x: (x[0], summarize_cluster(x[1])))
    fin = cluster_stats.collectAsMap()
    return fin


#STEP 1
'''Getting the sample, remanining and outlier data'''
def get_data(data):
    sample = list()
    rem = list()
    outliers_dict = dict()
    outliers = detect_outliers(data)
    len_data = len(data)
    len_sample = int(0.30 * len_data) #Taking 30% of the data as sample
    sample_indices = set(random.sample(range(len_data), len_sample))
    for index, item in enumerate(data):
        if item[0] in outliers:
            outliers_dict[item[0]] = item[1]
        elif index in sample_indices:
            sample.append(item)
        else:
            rem.append(item)
    return sample,rem,outliers_dict


#STEP 1
def intialize(data_dict):
    global discard_sets
    global discard_set_stats
    global compression_set
    global comp_set_count
    global comp_set_stats
    global retained_set
    data_list = list(data_dict.items())
    sample_data,remaining_data,outliers_dict = get_data(data_list)
    
    #STEP 4
    discard_sets, ds_stats_temp = k_means(sample_data, k_clusters, 35, True) #run k-mean++ for discard set
    discard_set_stats = statistics(ds_stats_temp)

    csrs_dict, csrs_stats = k_means(remaining_data, 5 * k_clusters, 35, False) #making 5* more clusters for CS and RS
    rs = dict()
    
    cs_stats_temp = dict()
    for c in csrs_dict:
        len_cluster = len(csrs_dict[c])
        if len_cluster != 1:
            compression_set[comp_set_count] = csrs_dict[c]
            cs_stats_temp[comp_set_count] = csrs_stats[c]
            comp_set_count += 1
        else:
            rs[csrs_dict[c][0]] = csrs_stats[c][0]
    comp_set_stats = statistics(cs_stats_temp)
    
    retained_set.update(outliers_dict)
    retained_set.update(rs)


'''Calculating Mahalanobis Distance = (pi – ci) / σi'''
def mahalanobis_dist(coords, summary):
    n = summary["N"]
    dist = 0
    temp = 0
    for d in range(dimension):
        sum_ = summary["SUM"][d]
        sumsq = summary["SUMSQ"][d]
        variance = (sumsq/n)-((sum_/n)**2)
        centroid = summary["CENTROID"][d]
        numerator = (coords[d] - centroid)
        if variance != 0:
            temp = (numerator**2) / variance
        else:
            temp = numerator**2
        dist += temp
    return math.sqrt(dist)


'''Updating statistics after successive rounds'''
def update_statistics(prev, curr):
    summary = dict()
    sum_vec = list()
    sumsq_vec = list()
    centroid_vec = list()
    n = prev["N"] + curr["N"]
    for d in range(dimension):
        sum_vec.append(prev["SUM"][d] + curr["SUM"][d])
        sumsq_vec.append(prev["SUMSQ"][d] + curr["SUMSQ"][d])
        centroid_vec.append((prev["SUM"][d] + curr["SUM"][d])/n)
    summary["N"] = n
    summary["SUM"] = sum_vec
    summary["SUMSQ"] = sumsq_vec
    summary["CENTROID"] = centroid_vec
    return summary

'''Merging Retained Set'''
def rscs_merge(rscs_cluster, rscs_stats_temp):
    global compression_set
    global comp_set_stats
    global comp_set_count
    rscs_stats = statistics(rscs_stats_temp)
    for index in rscs_cluster:
        temp = list()
        for stats in list(comp_set_stats.items()):
            temp.append((stats[0], mahalanobis_dist(rscs_stats[index]["CENTROID"], stats[1])))
        m = min(temp,key=lambda x: x[1])
        if m[1] < threshold:
            compression_set[m[0]].extend(rscs_cluster[index])
            comp_set_stats[m[0]] = update_statistics(comp_set_stats[m[0]], rscs_stats[index])
        else:
            compression_set[comp_set_count] = copy.deepcopy(rscs_cluster[index])
            comp_set_stats[comp_set_count] = copy.deepcopy(rscs_stats[index])
            comp_set_count += 1

#STEP 6 and 7
'''Comparing DS and CS with Mahalanobis' Distance'''
def clusters(x, cluster_stats):
    index = x[0]
    coordinates = x[1]
    temp = list()
    for stats in list(cluster_stats.items()):
        temp.append((stats[0], mahalanobis_dist(coordinates, stats[1])))
    m = min(temp, key=lambda x: x[1])
    if m[1] < threshold:
        return tuple([True, m[0], {"k": index, "v": coordinates}])
    else:        
        return tuple([False, {"k": index, "v": coordinates}])

'''Run BFR after round 1'''
def bfr(data):

    global discard_sets
    global retained_set
    global compression_set
    global discard_set_stats
    global comp_set_stats

    rdd = sc.parallelize(list(data.items()))

    ds = rdd.map(lambda x: clusters(x, discard_set_stats))
    new_ds_cluster = ds.filter(lambda x: x[0]).map(lambda x: (x[1], [x[2]["k"]]))
    new_ds_cluster = new_ds_cluster.reduceByKey(lambda x, y: x+y).collectAsMap()
    new_ds = ds.filter(lambda x: x[0]).map(lambda x: (x[1], [x[2]["v"]]))
    new_ds = new_ds.reduceByKey(lambda x, y: x+y).collectAsMap()
    new_ds_stats = statistics(new_ds)

    cs = ds.filter(lambda x: not x[0]).map(lambda x: clusters(tuple([x[1]["k"], x[1]["v"]]), comp_set_stats))
    new_cs_cluster = cs.filter(lambda x: x[0]).map(lambda x: (x[1], [x[2]["k"]]))
    new_cs_cluster = new_cs_cluster.reduceByKey(lambda x, y: x+y).collectAsMap()
    new_cs = cs.filter(lambda x: x[0]).map(lambda x: (x[1], [x[2]["v"]]))
    new_cs = new_cs.reduceByKey(lambda x, y: x+y).collectAsMap()
    new_cs_stats = statistics(new_cs)

    new_rs = cs.filter(lambda x: not x[0]).map(lambda x : (x[1]["k"], x[1]["v"])).collectAsMap()

    
    for i in new_ds_cluster:
        discard_sets[i].extend(new_ds_cluster[i])
        discard_set_stats[i] = update_statistics(discard_set_stats[i], new_ds_stats[i])

    for i in new_cs_cluster:
        compression_set[i].extend(new_cs_cluster[i])
        comp_set_stats[i] = update_statistics(comp_set_stats[i], new_cs_stats[i])

    retained_set.update(new_rs)
    if len(list(retained_set.items())) > 5*k_clusters:
        rs_cluster, rs_stats = k_means(list(retained_set.items()), 5 * k_clusters, 5, False)
        rs = dict()
        rscs = dict()
        rscs_stats = dict()
        for rs in rs_cluster:
            cluster_len = len(rs_cluster[rs])
            if cluster_len != 1:
                rscs[rs] = rs_cluster[rs]
                rscs_stats[rs] = rs_stats[rs]
            else:
                rs[rs_cluster[rs][0]] = rs_stats[rs][0]
                
        rscs_merge(rscs, rscs_stats)
        retained_set = copy.deepcopy(rs)

'''Finalize outliers clusters'''
def final_process():
    balance_cs = list()
    balance_rs = list()
    temp = list()
    for key in comp_set_stats:
        point_coords = comp_set_stats[key]["CENTROID"]
        temp = list()
        for stats in list(discard_set_stats.items()):
            temp.append((stats[0], mahalanobis_dist(point_coords, stats[1])))
        
        m = min(temp, key=lambda x: x[1])
        if m[1] < threshold:
            index = m[0]
            discard_sets[index].extend(compression_set[key])
            discard_set_stats[index] = update_statistics(discard_set_stats[index], comp_set_stats[key])
        else:
            balance_cs.extend(compression_set[key])

    for point in retained_set:
        point_coords = retained_set[point]
        temp = list()
        for stats in list(discard_set_stats.items()):
            temp.append((stats[0], mahalanobis_dist(point_coords, stats[1])))
        
        m = min(temp, key=lambda x: x[1])
        if m[1] < threshold:
            index = m[0]
            discard_sets[index].append(point)
            temp_dict = {"N": 1, "SUM": point_coords, "SUMSQ": [x**2 for x in point_coords], "CENTROID" : point_coords}
            discard_set_stats[index] = update_statistics(discard_set_stats[index], temp_dict)
        else:
            balance_rs.append(point)

    rdd = sc.parallelize(list(discard_sets.items()))
    output = rdd.flatMap(lambda x: [(i, x[0]) for i in x[1]]).collectAsMap()

    #The cluster number of the outliers are number as -1
    for b in balance_cs:
        output[b] = -1

    for b in balance_rs:
        output[b] = -1

    return output


def form_clusters(files):
    intermediate = list()
    for rounds, file_name in enumerate(files):
        print(file_name)
        file_object = open(input_folder + "/" + file_name, mode='r')
        data = [[float(x) for x in line.strip('\n').split(',')] for line in file_object.readlines()]
        global dimension
        global threshold
        data_dict = dict()
        dimension = len(data[0]) - 1
        threshold = 2 * math.sqrt(dimension) #threshold for mahalano dist calc
        for i in data:
            data_dict[int(i[0])] = i[1:] #{0 : [data points]}
        if rounds == 0:
            intialize(data_dict)
        else:
            bfr(data_dict)
        
        ds = sum([x["N"] for x in discard_set_stats.values()])
        cs = sum([x["N"] for x in comp_set_stats.values()])
        no_ds_items = len(list(discard_sets.items()))
        no_cs_items = len(list(compression_set.items()))
        no_rs_items = len(list(retained_set.items()))
        log_info = [rounds+1, no_ds_items, ds, no_cs_items, cs, no_rs_items]

        intermediate.append(log_info)
        file_object.close()

    out_dict = final_process()
    return intermediate, out_dict


# start = time.time() #start time
# input_folder = "/content/drive/MyDrive/HW6/bfr_data/temp"#input folder path
# k_clusters = 5
# # output_file = sys.argv[3]
# # intermediate_op_file = sys.argv[4]

# input_files = list()
# for i in os.listdir(input_folder):
#     if ".txt" in i:
#         input_files.append(i)

# logs, output = form_clusters(input_files)

# logs.insert(0, ['round_id', 'nof_cluster_discard', 'nof_point_discard', 'nof_cluster_compression', 'nof_point_compression','nof_point_retained'])
# final_rdd = sc.parallelize(output.items())
# output_dict = final_rdd.sortByKey().collectAsMap()

# '''writing into the output file'''
# with open("temp1.txt", 'w') as output_file1:
#     json.dump(output_dict, output_file1)


# '''Writing into the intermediate file'''
# with open("temp2.txt", 'w') as output_file2:
#     for row in logs:
#         output_file2.write(','.join([str(x) for x in row]))
#         output_file2.write('\n')


# print("Duration : ", time.time() - start)

start = time.time() #start time
input_folder = sys.argv[1]#input folder path
k_clusters = int(sys.argv[2])
output_file = sys.argv[3]
intermediate_op_file = sys.argv[4]

input_files = list()
for i in os.listdir(input_folder):
    if ".txt" in i:
        input_files.append(i)

logs, output = form_clusters(input_files)

logs.insert(0, ['round_id', 'nof_cluster_discard', 'nof_point_discard', 'nof_cluster_compression', 'nof_point_compression','nof_point_retained'])
final_rdd = sc.parallelize(output.items())
output_dict = final_rdd.sortByKey().collectAsMap()

'''writing into the output file'''
with open(output_file, 'w') as output_file1:
    json.dump(output_dict, output_file1)


'''Writing into the intermediate file'''
with open(intermediate_op_file, 'w') as output_file2:
    for row in logs:
        output_file2.write(','.join([str(x) for x in row]))
        output_file2.write('\n')


print("Duration : ", time.time() - start)