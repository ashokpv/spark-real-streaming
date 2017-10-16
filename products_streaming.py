from __future__ import print_function


import sys
from sklearn.externals import joblib
import pymongo

import pandas 
from sklearn.externals import joblib
import pymongo
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext,functions as func,Row
import json
from subprocess import Popen, PIPE 

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("product_streaming_file_path")
        exit(-1)

sc = SparkContext(appName="mobiport")
sqlc = SQLContext(sc)
ssc = StreamingContext(sc, 10)
lines = ssc.textFileStream('hdfs://localhost:9000/hdfs/')
a=lines.flatMap(lambda x:json.loads(x))
s=a.map(lambda x: str(x)).map(lambda w: w.split(',')) 		#transformed dstream from the dstream

d=[]
def get_output(time,rdd):  
  try:  
    d=rdd.collect()
    f=sc.parallelize(d) 
    g=sqlc.read.json(f)
    data_d=g.toPandas()
    data_d.to_pickle("/home/ashok/dataframe")
    put = Popen(["/usr/local/lib/hadoop-2.7.0/bin/hadoop", "dfs", "-put", "/home/ashok/dataframe","/dataframe"])   
    vector_test=list(data_d.columns) 
    print(data_d.columns)
    text_clf = joblib.load('/home/ashok/mobiport/datatransformation/product.pkl') 
    predicted = text_clf.predict(vector_test)
    p=list(predicted)
    data_d.columns=p
    data_d['spec']=data_d['specs'].apply(lambda x: ','.join(str	(i) for i in x),axis=1)
    data_d=data_d.drop('specs',1)
    data_d=data_d[["id","title","category","description","spec"]].reset_index(drop=True) #final pandas dataframe mobiport
   
    client = pymongo.MongoClient()
    db=client.check
    collection=db.check
    collection.insert_many(data_d.to_dict('records'))
    print("\n\n=========TIME : %s =========" % str(time))
    print("\ninsertion success\n\n")
    print("\n\n==================================")
    cut = Popen(["/usr/local/lib/hadoop-2.7.0/bin/hadoop", "dfs", "-mv", "/dataframe/*","/moved"])  
  except:
    print("\n\n=========TIME :%s ===============" % str(time))
    print("\n\nnothing to process\n\n") 
    print("\n\n===================================")

lines.foreachRDD(get_output)

	
ssc.start()
	
	
ssc.awaitTermination()
