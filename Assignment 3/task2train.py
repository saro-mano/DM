from __future__ import division
import sys
import pyspark
import json
from pyspark import SparkContext,SparkConf
import time
import re
from collections import defaultdict
import math


start = time.time() #Initializing the timer
input_file = sys.argv[1] #Input File Path
model_file = sys.argv[2] #Model File Path ~ Output file for this pgm
stopwords_file = sys.argv[3] #Stopwords

conf = SparkConf().setMaster("local").setAppName("task2train").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf) #Spark Init
main_rdd = sc.textFile(input_file) #Loading the Input file
rdd = main_rdd.map(json.loads) #Converting to JSON

business_doc = rdd.map(lambda x: (x["business_id"],x["text"])).reduceByKey(lambda x,y: x+y) #Extracting the (bus_id,reviewText)

#Extracting the stop words
stopword_file = open(stopwords_file,"r")
stopwords = stopword_file.readlines()
stopwords_list = list()
for words in stopwords:
  stopwords_list.append(words.strip())

'''Function to clean the document''' 
def cleaner(x):
  bus_id = x[0]
  doc = x[1].lower() #Conveting all the words to lower
  doc = re.sub(r'[^A-Za-z]', ' ', doc) #Removing all punctuations, numbers, and stopwords
  words = doc.split()
  final_words = list()
  for word in words:
    if word not in stopwords_list:
      final_words.append(word)
  return tuple([bus_id, final_words]) #[bussiness id : [review_word_list]]

clean_doc = business_doc.map(cleaner) #Clean Document is created which has business_id with it's reviews
doc_count = clean_doc.count() #Counting the document for IDF calculation (no of documents)

'''Counting the words for rare_words calculation'''
def word_counter(x):
  words = x[1]
  output = list()
  for word in words:
    output.append((word,1))
  return output

words_in_doc = clean_doc.flatMap(word_counter)
words_count = words_in_doc.count() #Counting the number of words for rare_words calculation
rare_words = words_in_doc.reduceByKey(lambda x,y : x+y).filter(lambda x : x[1] < (0.000001*words_count)).collectAsMap()

words_index = words_in_doc.reduceByKey(lambda x,y:x+y).map(lambda x: x[0]).distinct().zipWithIndex()
words_dict = words_index.map(lambda x : {x[0]:x[1]})
words_dictionary = words_dict.flatMap(lambda x:x.items()).collectAsMap() #all the words with index

def rare_words_removal(x):
  output = list()
  # print(x[0][1])
  words = x[1]
  for word in words:
    if word not in rare_words:
      output.append(words_dictionary[word])
  return tuple((x[0],output)) #Business with rare words removed and appends word index

business_words = clean_doc.map(rare_words_removal) #Business with rare words removed {B_id : [list of words index]}

'''Function to count number of documents containing the same word'''
def unique_word_cal(x):
  l = list(set(x[1]))
  output = list()
  for nos in l:
    output.append((nos,1))
  return output

unique_word_count = business_words.flatMap(unique_word_cal).reduceByKey(lambda x,y : x+y) #Unique Word Index : Count
unique_word_count = unique_word_count.collectAsMap()


def count_words(x):
  word_count = x[1]
  d = defaultdict(int)
  for w in word_count:
    d[w] += 1
  # m = max(list(d.values()))
  return (x[0],d)

total_occurence = business_words.map(count_words) #{B_id : dict of word index with count}
total_occurence_max = total_occurence.map(lambda x: (x[0],max(x[1].values()))).collectAsMap()


#TF = (Number of repetitions of word in a document) / (# of words in a document)
#IDF = Log[(# Number of documents) / (Number of documents containing the word)]

def bus_prof_gen(x):
  business_id = x[0]
  documents = x[1].items() #Word index with count
  TF_IDF = dict()
  for doc in documents:
    w = doc[0] #key i.e., word index
    tf = doc[1] / total_occurence_max[business_id] #word_count / total words in that doc
    idf = math.log2(doc_count/unique_word_count[w]) #no-of-doc / words in doc
    TF_IDF[w] = tf*idf
  top = sorted(TF_IDF.items(), key = lambda x: x[1], reverse = True)
  top = top[:200] #Extracting the top 200 words with counts
  top_words_alone = [y[0] for y in top] #Extracting the words index alone
  return tuple([business_id, top_words_alone]) #Business with top index words

business_profile = total_occurence.map(bus_prof_gen).collectAsMap()

user_business = rdd.map(lambda x:(x["user_id"],[x["business_id"]])).reduceByKey(lambda x,y : x+y)

def user_prof_gen(x):
  business_id = x[1] #Business_id
  user_profile = list()
  for id in business_id:
    user_profile += list(business_profile[id]) #Appending the list of words index to the profile
  user_profiles = list(set(user_profile)) #Removing dup
  return tuple([x[0],user_profiles]) #UserID , list of word indexes

user_profile = user_business.map(user_prof_gen).collectAsMap()
output = [user_profile,business_profile]

with open(model_file, 'w') as op:
  for i in output:
      json.dump(i, op)
      op.write('\n')
print("Duration ", time.time()-start)