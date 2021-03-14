import sys
import pyspark
import csv
import json
from pyspark import SparkContext

# sc = pyspark.SparkContext('local[*]')
business = sc.textFile("/content/drive/My Drive/HW2/yelp_academic_dataset_business.json")
review = sc.textFile("/content/drive/My Drive/HW2/yelp_academic_dataset_review.json")

review_rdd = review.map(json.loads)
business_rdd = business.map(json.loads)

review_star = review_rdd.map(lambda x:(x['business_id'],x['user_id']))
business_category = business_rdd.filter(lambda x: x["stars"]>=4.0).map(lambda x:(x['business_id'],x["state"]))
review_business_join =  review_star.join(business_category)

data = review_business_join.collect()
output_csv = "/content/drive/My Drive/HW2/output.csv"
with open(output_csv,'w',newline='') as output:
  header = ['user_id','state']
  writer = csv.DictWriter(output, fieldnames=header)
  writer.writeheader()
  for x in data:
    writer.writerow({'user_id': x[1][0], 'state': x[1][1]})

   