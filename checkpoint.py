from __future__ import print_function

import time

import sys
import pandas 
from sklearn.externals import joblib
import pymongo
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext,functions as func,Row
import json

import re


if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("product_streaming_file_path")
        exit(-1)

checkpointDirectory = 'hdfs://localhost:9000/checkpoint/' 

def functionToCreateContext():
    sc = SparkContext(appName="mobiport")
    ssc = StreamingContext(sc, 10)
    sqlc = SQLContext(sc)    
    lines = ssc.textFileStream('hdfs://localhost:9000/ash/*/*/')
    ssc.checkpoint(checkpointDirectory)
    d=[]
    def get_output(time,rdd): 
      if not (rdd.isEmpty()): 
       try:
        debug = rdd.toDebugString()
        print("this is the main file path ")
        a=re.split(b'[|]',debug)
        b=re.split(b'[/]',a[2])
        print(b[4].decode('utf-8'))
        package=b[4].decode('utf-8')
	d=rdd.collect()
    	f=sc.parallelize(d) #this is te place where we get errors becuse of sc and sqlc 
   	g=sqlc.read.json(f) #it exists the code from here
    
	

        data_d=g.toPandas()
   
        data_d['packagename']=package 
        print(data_d[:3])

        vector_test=list(data_d.columns) 
        print(data_d.columns)
        text_clf = joblib.load('/home/ashok/mobiport/datatransformation/product.pkl') 
        predicted = text_clf.predict(vector_test)
        p=list(predicted)
        data_d.columns=p
        data_d['spec']=data_d['specs'].apply(lambda x: ','.join(str    (i) for i in x),axis=1)
        data_d=data_d.drop('specs',1)
        data_d['packagename']=package 
        data_d=data_d[["id","title","category","description","spec","packagename"]].reset_index(drop=True) #final pandas dataframe mobiport
        
        client = pymongo.MongoClient()
        db=client.newdata
        collection=db.bulk
        collection.insert_many(data_d.to_dict('records'))
        print("\n\n=========TIME : %s =========" % str(time))
        print("\ninsertion success\n\n")
        print("\n\n==================================")
        
        
       except:
        print("\n\n=========this is error TIME :%s ===============" % str(time))
        #print("\n\nnothing to process\n\n") 
        #print("\n\n===================================")
      else: 
        print("\n\n=========ashok TIME :%s ===============" % str(time))
        print("\n\nnothing to process\n\n") 
        print("\n\n===================================")

    
    lines.foreachRDD(get_output)
    return ssc  


ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)

    
ssc.start()
    
    
ssc.awaitTermination()
            
            
