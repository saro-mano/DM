import sys
import json
import pyspark
from pyspark import SparkContext
import re

'''getting files from the command line'''
input_file = sys.argv[1]
output_file = sys.argv[2]
stopwords_file = sys.argv[3]
year = sys.argv[4]
m_users = sys.argv[5]
n_freq_words = sys.argv[6]


output = {"A":0 , "B" : 0 , "C" : 0, "D" : [], "E" : []}

sc = pyspark.SparkContext('local[*]')
review = sc.textFile(input_file)
rdd = review.map(json.loads)

output["A"] = rdd.count() #Task A - The total number of reviews

year_rdd = rdd.filter(lambda x: int(x["date"].split()[0][:4])== int(year))
output["B"] = year_rdd.count() #Task B - The number of reviews in a given year

distinct_review_rdd = rdd.map(lambda x: x['business_id'])
output["C"] = distinct_review_rdd.distinct().count() #Task C - The number of distinct businesses which have the reviews

userCount = rdd.map(lambda x: (x["user_id"], 1)).reduceByKey(lambda x, y: x + y)

sortUser = userCount.sortBy(lambda x :(-x[1],x[0]))
result = sortUser.map(lambda x: list([x[0],x[1]])).take(int(m_users))
output["D"] = result #taskD - Top m users who have the largest number of reviews and its count

'''extracting the stopwords'''
stopwords = open(stopwords_file,'r')
stopword_list = []
stopwords = stopwords.readlines()
for each in stopwords:
  stopword_list.append(each.strip())

def count_words(rdd_object):
  word_count = []
  text = rdd_object["text"].strip().lower()
  text = re.sub(r'[\(\[\,\.\!\?\:\;\]\)]', '', text) #replacing the punctuations with empty character
  word_list = text.split(" ")
  for each in word_list:
    each = each.strip("\n")
    if len(each) > 0 and each not in stopword_list:
      word_count.append((each,1))
  return word_count

words = rdd.flatMap(count_words).reduceByKey(lambda x,y:x+y)
sortedwords = words.sortBy(lambda x: (-x[1],x[0]))
output["E"] = sortedwords.map(lambda x: x[0]).take(int(n_freq_words)) #Task E - Top n frequent words in the review text

#output written to a JSON
with open(output_file,'w') as writer:
    json.dump(output, writer)
writer.close()