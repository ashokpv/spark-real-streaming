from __future__ import print_function
import time
import sys
import pandas as pd
import pymongo
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext,functions as func,Row
from pyspark.sql import Row, SparkSession
import json
import joblib
import re

from sklearn.feature_extraction.text import TfidfVectorizer
import Stemmer



if __name__ == "__main__":
  if len(sys.argv) != 1:
    print("product_streaming_file_path")
    exit(-1)

checkpointDirectory = 'hdfs://localhost:9000/checkpoint/' 

#STEMMING THE WORDS IN THE LIST 
english_stemmer = Stemmer.Stemmer('en')
class StemmedTfidfVectorizer(TfidfVectorizer):
  def build_analyzer(self):
    analyzer = super(TfidfVectorizer, self).build_analyzer()
    return lambda doc: english_stemmer.stemWords(analyzer(doc))
     

#CREATE AN INSATANCE IN THE SQL FOR DATA FRAMES

def getSparkSessionInstance(sparkConf):
  if ('sparkSessionSingletonInstance' not in globals()):
    globals()['sparkSessionSingletonInstance'] = SparkSession\
      .builder\
      .config(conf=sparkConf)\
      .getOrCreate()
  return globals()['sparkSessionSingletonInstance']


#SPARK STREAMING FUNCTION 
def functionToCreateContext():
  sc = SparkContext(appName="mobiport")
  ssc = StreamingContext(sc, 10)
       
  lines = ssc.textFileStream('hdfs://localhost:9000/apps/Food/*/')
  ssc.checkpoint(checkpointDirectory)

  #THE DSTREAM INTO RDD TRANSFORMATION

  def get_output(time,rdd): 
    if not (rdd.isEmpty()): 
      try:
        debug = rdd.toDebugString()
        print("this is the main file path ")
        a = re.split(b'[|]',debug)
        b = re.split(b'[/]',a[2])
        print(b[5].decode('utf-8'))
        

        #GETTING THE PACKAGE NAME OF THE APP
        package = b[5].decode('utf-8')
        appcat=b[4].decode('utf-8')


        #GET THE SINGLE INSTANCE OF THE SPARK CONTEXT
        sqlc = getSparkSessionInstance(rdd.context.getConf())
        g = sqlc.read.json(rdd)                   
        data_d = g.toPandas()  

        #LIST THE COLUMNS IE JSON KEYS TO PREDICT
        vector_test = list(data_d.columns)      
        print(vector_test)


        #LOADING THE PKL FOR THE SVM CLASSIFIER
        tfidf,text_clf = joblib.load("/home/ash/mobiport/datatransformation/final/resaws.pkl") 
        
        predictlist = [word.replace('name','names') for word in vector_test]
         

        print(predictlist)
        
        #USIGN THE TRANSFORM MODULE TO CHANGE THE LIST INTO MATRIX 
        fg = tfidf.transform(predictlist).todense() 

        #PREDICTION FROM THE MACHINE LEARNING MODEL
        predicted = text_clf.predict(fg)
        print(predicted)
        p = list(predicted)

        #REPLACING THE PREDICTED COLUMNS INTO THE DATAFRAME
        data_d.columns = p
        data_d['specs'] = data_d['spec'].apply(lambda x: ','.join(str    (i) for i in x),axis=1)
        data_d = data_d.drop('spec',1)
        data_d['packagename'] = package 
        data_d['app_category'] = appcat
        #final pandas dataframe mobiport

	    
		#CALLING THE MONGO CLIENT

        client = pymongo.MongoClient()
        db = client.shoppingnew
        collection = db.res
        collection.insert_many(data_d.to_dict('records'))

        print("\n\n=========TIME : %s =========" % str(time))
        print("\ninsertion success\n\n")
        print("\n\n==================================")
        
        
      except:
        print("\n\n=========this is error TIME :%s ===============" % str(time))
        print("\n\nthis is internal error\n\n") 
        print("\n\n===================================")
    else: 
        print("\n\n=========ashok TIME :%s ===============" % str(time))
        print("\n\nnothing to process\n\n") 
        print("\n\n===================================")

    
  lines.foreachRDD(get_output)
  return ssc  

#ASSIGNING THE STREAMING TO START FROM CHCECKPOINT OR NEW ONE 
checkpointDirectory = 'hdfs://localhost:9000/checkpoint/'
ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)

    
ssc.start() #STARTING THE CONTEXT
    
    
ssc.awaitTermination()
            
            
