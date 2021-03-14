from __future__ import division
import sys
import pyspark
import json
from pyspark import SparkContext
import time



sc = pyspark.SparkContext('local[*]')

start = time.time()
train_file = sys.argv[1] #Train File
test_file = sys.argv[2] #Test File
model_file = sys.argv[3] #Computed Model File {b1,b2,sim} or {u1,u2,sim}
output_file = sys.argv[4] #Output File
cf_type = sys.argv[5] #Collobarative Filtering type


train_main = sc.textFile(train_file) #Loading train
test_main = sc.textFile(test_file) #Loading test

train = train_main.map(json.loads)
test = test_main.map(json.loads)

f = open(model_file,"r")
models = f.readlines()
user_bus_rating = train.map(lambda x: (tuple([x["user_id"], x["business_id"]]), [x["stars"]])).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], sum(x[1]) / len(x[1]))).collectAsMap()
#user_bus_rating = (user,bus) with average rating
if cf_type == "item_based":
	user_business = train.map(lambda x: (x["user_id"], [x["business_id"]])).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], set(x[1]))).collectAsMap()
	#user_business = {user_id : [ list of business_id]}
	item_data = dict()
	for model in models:
		data = json.loads(model)
		b1_dict = {data["b1"] : data["sim"]}
		if data["b1"] in item_data:
				item_data[data["b1"]].update(b1_dict)
		else:
				item_data[data["b1"]] = b1_dict
		b2_dict = {data["b2"] : data["sim"]}
		if data["b2"] in item_data:
				item_data[data["b2"]].update(b2_dict)
		else:
				item_data[data["b2"]] = b2_dict
	def predict(x):
		b_id = x["business_id"]
		u_id = x["user_id"]
		if (u_id,b_id) in user_business:
			x["stars"] = user_bus_rating[(u_id,b_id)]
		else:
			if b_id in item_data:
				if u_id in user_business:
					sim_bus = item_data[b_id].keys()
					user_rated = user_business[u_id]
					corated = set(sim_bus).intersection(set(user_rated))
					bus_neigh = list()
					for item in item_data[b_id].items():
						if item[0] in corated:
							bus_neigh.append(item)
					bus_neigh_sorted = sorted(bus_neigh, key = lambda y : y[1], reverse = True)[:5]
					numerator = 0
					denom = 0
					for neigh in bus_neigh_sorted:
						sim = neigh[1]
						if sim > 0.01:
							rating = user_bus_rating[tuple([u_id, neigh[0]])]
							numerator += (rating*sim)
							denom += sim
					if denom > 0:
						star = numerator / denom
						if star > 5:
							x["stars"] = 5
						else:
							x["stars"] = star
					else:
						x["stars"] = None
				else:
					x["stars"] = None
			else:
				x["stars"] = None
		return x

	output = test.map(predict).collect()

else:
	user_data = dict()
	for model in models:
		model = json.loads(model)
		b1_data = {model["u2"]: model["sim"]}
		b2_data = {model["u1"]: model["sim"]}
		if model["u1"] in user_data:
				user_data[model["u1"]].update(b1_data)
		else:
				user_data[model["u1"]] = b1_data
		if model["u2"] in user_data:
				user_data[model["u2"]].update(b2_data)
		else:
				user_data[model["u2"]] = b2_data
		
	business_user = train.map(lambda x: (x["business_id"], [x["user_id"]]))\
                                         .reduceByKey(lambda x, y : x + y)\
                                         .map(lambda x: (x[0], list(set(x[1]))))\
                                         .collectAsMap()
	def avg_rating(x):
		bus_rating = x[1]
		d = dict()
		for i in bus_rating:
			bus_id = i[0]
			rate = i[1]
			if bus_id in d:
				d[bus_id].append(rate)
			else:
				d[bus_id] = [rate]
		output = dict()
		for j in d.items():
			output[j[0]] = sum(j[1])/len(j[1])
		# output = dict([(y[0], sum(y[1])/len(y[1])) for y in d.items()])
		return (x[0],output)


	user_avg_rating = train.map(lambda x: (x["user_id"], [(x["business_id"], x["stars"])]))\
                                    .reduceByKey(lambda x, y: x + y)\
                                    .map(avg_rating)\
                                    .collectAsMap()
	
	def predict(dic):
		def average_rating(us, item):
			avg = 0.0
			if us in user_avg_rating:
				rating_list = [x[1] for x in user_avg_rating[us].items() if x[0] != item]
				avg = sum(rating_list)/len(rating_list)
			return avg

		user_id = dic["user_id"]
		business_id = dic["business_id"]
		if tuple([dic["user_id"], dic["business_id"]]) in user_bus_rating:
			dic["stars"] = user_bus_rating[tuple([user_id, business_id])]
		else:
			#items_rated_users_map = user_business
			#user_similarity = user_data
			#user_business_rating = user_bus_rating
			#=user_avg_rating
			if business_id in business_user:  # business already there
				if user_id in user_data:  # user already there
					similar_users = user_data[user_id].keys()
					business_rated_users = business_user[business_id]
					co_rated = set(similar_users).intersection(set(business_rated_users))
					top = sorted([k for k in user_data[user_id].items() if k[0] in co_rated],
												key=lambda z: z[1], reverse=True)[:5]
					n, d = 0.0, 0.0
					for tup in top:
							u = tup[0]
							w = tup[1]
							if w > 0.01:
									n = n + ((user_bus_rating[tuple([u, business_id])] - average_rating(u, business_id)) * w)
									d = d + w
					if d > 0:
						avg_user = average_rating(user_id, business_id)
						stars = avg_user + (n/d)
						if stars > 5:
							dic["stars"] = 5
						else:
							dic["stars"] = stars
					else:
						dic["stars"] = None
				else:
					dic["stars"] = None
			else:
					dic["stars"] = None
		return dic

output = test.map(predict).collect()

with open(output_file, 'w') as op:
  for i in output:
		json.dump(i, op)
		op.write('\n')
print("Duration ", time.time()-start)

