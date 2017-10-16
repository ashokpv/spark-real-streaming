from __future__ import print_function

import sys
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas 
from sklearn.externals import joblib
import pymongo
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext,functions as func,Row
import json

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("product_streaming_file_path")
        exit(-1)


sc = SparkContext(appName="mobiport")
sqlc = SQLContext(sc)
ssc = StreamingContext(sc, 20)
lines = ssc.textFileStream('hdfs://localhost:54310/hdfs/restaurant')
a=lines.flatMap(lambda x:json.loads(x))
s=a.map(lambda x: str(x)).map(lambda w: w.split(',')) 		#transformed dstream from the dstream
vectorizer = TfidfVectorizer()

d=[]
def get_output(time,rdd):  
  try:  
    d=rdd.collect()
    f=sc.parallelize(d) 
    g=sqlc.read.json(f)
    data_d=g.toPandas()    
    vector_test=list(data_d.columns) 
    print(data_d.columns)
    test_vectors = vectorizer.transform(vector_test)
    text_clf = joblib.load('/home/ashcool/restaurant.pkl') 
    predicted = text_clf.predict(test_vectors)
    p=list(predicted)
    data_d.columns=p
    data_d['spec']=data_d['specs'].apply(lambda x: ','.join(str	(i) for i in x),axis=1)
    data_d=data_d.drop('specs',1)
	    
    j = (data.groupby(['id','app_id','category','title','spec'], as_index=False).apply(lambda x: x                        [['address','city','locality','longitude','latitude','postalcode','state','rating']].to_dict('r')).reset_index().rename(columns={0:'geo'}).to_json(orient='records'))
    import ast
    j1=ast.literal_eval(j)
    client = pymongo.MongoClient()
    db=client.check
    collection=db.check
    collection.insert_many(data_d.to_dict('records'))
    print("\n\n=========TIME : %s =========" % str(time))
    print("\ninsertion success\n\n")
    print("\n\n==================================")
  except:
    print("\n\n=========TIME :%s ===============" % str(time))
    print("\n\nnothing to process\n\n") 
    print("\n\n===================================")

lines.foreachRDD(get_output)

	
ssc.start()
	
	
ssc.awaitTermination()
