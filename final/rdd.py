from __future__ import print_function
import time
import sys
import pandas as pd
import pymongo
from pyspark import SparkContext
from pyspark.sql import SQLContext,functions as func,Row
from pyspark.sql import Row, SparkSession
import json
import joblib
import re
import sh

from sklearn.feature_extraction.text import TfidfVectorizer
import Stemmer

if __name__ == "__main__":
  if len(sys.argv) != 1:
    print("product_streaming_file_path")
    exit(-1)


sc = SparkContext(appName="mobiport")
#STEMMING THE WORDS IN THE LIST 
english_stemmer = Stemmer.Stemmer('en')
class StemmedTfidfVectorizer(TfidfVectorizer):
  def build_analyzer(self):
    analyzer = super(TfidfVectorizer, self).build_analyzer()
    return lambda doc: english_stemmer.stemWords(analyzer(doc))


def getSparkSessionInstance(sparkConf):
  if ('sparkSessionSingletonInstance' not in globals()):
    globals()['sparkSessionSingletonInstance'] = SparkSession\
      .builder\
      .config(conf=sparkConf)\
      .getOrCreate()
  return globals()['sparkSessionSingletonInstance']


count=0

hdfsdir = 'hdfs://localhost:9000/apps/Food/*/'
filelist = [ line.rsplit(None,1)[-1] for line in sh.hdfs('dfs','-ls',hdfsdir).split('\n') if len(line.rsplit(None,1))][1:]
n=len(filelist)
file=filelist[n-1]
print(filelist)
for i in range(0,len(filelist)):
  d=filelist[count-1]

  if(d==file):
    print("true")
   

    count=count+1
    rdd=sc.textFile(filelist[i])
    debug = rdd.toDebugString()
    print("this is the main file path ")
    a = re.split(b'[|]',debug)
    b = re.split(b'[/]',a[1])
    print(b[5].decode('utf-8'))

        

        #GETTING THE PACKAGE NAME OF THE APP
    package = b[5].decode('utf-8')
    appcat=b[4].decode('utf-8')



        #GET THE SINGLE INSTANCE OF THE SPARK CONTEXT
  '''sqlc = getSparkSessionInstance(rdd.context.getConf())
  g = sqlc.read.json(rdd)                   
  data_d = g.toPandas()  

        #LIST THE COLUMNS IE JSON KEYS TO PREDICT
  vector_test = list(data_d.columns)      
  print(vector_test)
  predictlist = [word.replace('name','names') for word in vector_test]
         

  print(predictlist)
        
        #USIGN THE TRANSFORM MODULE TO CHANGE THE LIST INTO MATRIX 
  fg = tfidf.transform(predictlist).todense() 

        #PREDICTION FROM THE MACHINE LEARNING MODEL
  predicted = text_clf.predict(fg)
  print(predicted)
  p = list(predicted)
  data_d.columns = p
  data_d['specs'] = data_d['spec'].apply(lambda x: ','.join(str    (i) for i in x),axis=1)
  data_d = data_d.drop('spec',1)
  data_d['packagename'] = package 
  data_d['app_category'] = appcat
  print(data_d)'''


   
print(count)     