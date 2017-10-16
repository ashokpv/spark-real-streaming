from __future__ import print_function
import time
import sys , pymongo
import pandas as pd
from DT_Start import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext,functions as func,Row
from pyspark.sql import Row, SparkSession
import json

import re
import ast

if __name__ == "__main__":
  if len(sys.argv) != 1:
    print("product_streaming_file_path")
    exit(-1)

#checkpointDirectory = 'hdfs://localhost:9000/checkpoint/' 
checkpointDirectory = '/home/ash/mobiport/check/'

#STEMMING THE WORDS IN THE LIST 
    

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
       
  lines = ssc.textFileStream('hdfs://localhost:9000/apps/Shopping/*/*/')
  #lines = ssc.textFileStream('/home/ash/mobiport/hdfs/')
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
        print("this is mongo app_metadata")
        package = b[5].decode('utf-8')
        appcat = b[4].decode('utf-8')

        #GET THE SINGLE INSTANCE OF THE SPARK CONTEXT
        sqlc = getSparkSessionInstance(rdd.context.getConf())
        '''g = sqlc.read.json(rdd)                   
        data_raw = g.toPandas()  
        new = g.toPandas()'''       
        m = rdd.collect()
        d = json.loads(m[0])
        data_raw = pd.DataFrame(d)
        new = pd.DataFrame(d)

        data_raw.columns = map(str.lower, data_raw.columns)
        new.columns = map(str.lower, new.columns)

        #LIST THE COLUMNS IE JSON KEYS TO PREDICT
     
        print("this is the data raw ++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print(data_raw)

        DT_start_obj = DT_Start()
        print(data_raw)
        data_new ,spec_columns = DT_Start().start_DT_main(data_raw,package)

          
        print("these are spec",spec_columns)    
        data_new['packagename'] = package 
        data_new['app_category'] = appcat

        #data_new['deeplink']=l

        print("this is the specdata")
        
        data_new = data_new.drop('spec',1)

        print("+++++++++++++++++++++++++++++++++++DATA NEW +++++++++++++++++++++")
        print(data_new)
        spec = new[spec_columns]
        #data_d=data_new.join(spec)
        print("this is the col")

        
        specs_list=[]
        for row in spec.iterrows():
            specs_list.append(row[1].to_json())

        data_new['specs'] = specs_list
        data_new.columns=DT_Start.manage_dupe_cols(list(data_new))    

        #final pandas dataframe mobiport

        print("this is the mongo")
    #CALLING THE MONGO CLIENT

        clients = pymongo.MongoClient()

        
  
        dbb = clients.checking
        collections = dbb.dt
        collections.insert_many(data_new.to_dict('records'))
        #collections.insert_many(final_dict)

        print("\n\n=========TIME : %s =========" % str(time))
        print("\ninsertion success\n\n")
        print("\n\n==================================")

       

        
        
      except Exception as e :
        print("\n\n=========this is error TIME :%s ===============" % str(time))
        print("\n\nthis is internal error\n\n") 
        print(str(e))
        print("\n\n===================================")
    else: 
        print("\n\n=========ashok TIME :%s ===============" % str(time))
        print("\n\nnothing to process\n\n") 
        print("\n\n===================================")

    
  lines.foreachRDD(get_output)
  return ssc  

#ASSIGNING THE STREAMING TO START FROM CHCECKPOINT OR NEW ONE 
#checkpointDirectory = 'hdfs://localhost:9000/checkpoint/'
checkpointDirectory = '/home/ash/mobiport/check/'

ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)

    
ssc.start() #STARTING THE CONTEXT
    
    
ssc.awaitTermination()
            
            
