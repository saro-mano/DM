import sys
import json
import pyspark
from pyspark import SparkContext
import re

'''getting files from the command line'''
review_file = sys.argv[1]
business_file = sys.argv[2]
output_file = sys.argv[3]
if_spark = sys.argv[4]
n_category = sys.argv[5]

output = {"result" : []}

if if_spark == "spark":
    sc = pyspark.SparkContext('local[*]')
    review = sc.textFile(review_file)
    business = sc.textFile(business_file)
    review_rdd = review.map(json.loads)
    business_rdd = business.map(json.loads)
    review_star = review_rdd.map(lambda x:(x['business_id'],x['stars']))
    business_category = business_rdd.map(lambda x:(x['business_id'],str(x['categories']).split(",")))
    review_business_join =  review_star.join(business_category)
    review_business_join.take(2)
    def category_star_count(rdd_object):
      #output should be key = ca and value should be rating,count
      list_of_categories = rdd_object[1][1]
      star = rdd_object[1][0]
      key_value = []
      for each in list_of_categories:
        each = each.strip()
        if len(each) > 0:
          key_value.append((str(each),(star,1)))
      return key_value

    total_category = review_business_join.flatMap(category_star_count).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    average_rating = total_category.map(lambda x: list([x[0], x[1][0]/x[1][1]])).sortBy(lambda x : (-x[1],x[0]))
    output["result"] = average_rating.take(int(n_category))
else:
    business = open(business_file,'r')
    review = open(review_file,'r')
    review_dict = {} #to store the ratings and business_id
    business_dict = {} #get the business_id and categories
    business_read = business.readlines()
    review_read = review.readlines()
    for i in business_read:
      temp = json.loads(i)
      if temp["categories"]:
        business_dict[temp["business_id"]] = str(temp['categories']).split(",")
    category_count = {}
    for i in review_read:
      temp = json.loads(i)
      id = temp['business_id']
      star = temp['stars']
      if id in business_dict:
        list_of_categories = business_dict[id]
        for category in list_of_categories:
          category = category.strip()
          if category in category_count:
            prev_rating_sum = category_count[category][0]
            prev_count = category_count[category][1]
            category_count[category] = [(prev_rating_sum+star),(prev_count+1)]
          else:
            category_count[category] = [star,1]
    result = {}
    for category in category_count:
      sum = category_count[category][0]
      count = category_count[category][1]
      result[category] = sum/count
    result = sorted(result.items(),key = lambda x:(-x[1],x[0]))
    final_result = list()

    for i in range(int(n_category)):
      output["result"].append([result[i][0],result[i][1]])
    
with open(output_file,'w') as writer:
    json.dump(output, writer)
writer.close()
    