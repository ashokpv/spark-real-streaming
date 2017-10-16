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
import ast

from sklearn.feature_extraction.text import TfidfVectorizer
import Stemmer

from dlink_pattern_matcher_v3 import DlinkPatternMatcherV3
from dlink_generator_utility_v2 import DlinkGeneratorUtilityV2

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
       
  lines = ssc.textFileStream('hdfs://localhost:9000/apps/Shopping/*/')
  ssc.checkpoint(checkpointDirectory)

  #THE DSTREAM INTO RDD TRANSFORMATION

  def get_output(time,rdd): 
    if not (rdd.isEmpty()): 
      try:
        spec_columns=[]
        debug = rdd.toDebugString()
        print("this is the main file path ")
        a = re.split(b'[|]',debug)
        b = re.split(b'[/]',a[2])
        print(b[5].decode('utf-8'))
        print("this is mongo app_metadata")
        package = b[5].decode('utf-8')
        client = pymongo.MongoClient("mongodb://mobiport:MoBip0rT@13.92.247.46:27017/mobiport")
        db = client.mobiport
        apps=db.apps
        

        #GETTING THE PACKAGE NAME OF THE APP
        
        print('this is the app_data')
        app_data=apps.find_one({"package":package})
        '''dl_prefix= app_data['dl_prefix']
        dl_pattern=app_data['dl_pattern']
        pattern_info=[dl_prefix,dl_pattern]

        print(pattern_info)
        '''



        appcat = b[4].decode('utf-8')

        #GET THE SINGLE INSTANCE OF THE SPARK CONTEXT
        sqlc = getSparkSessionInstance(rdd.context.getConf())
        g = sqlc.read.json(rdd)                   
        data_raw = g.toPandas()  
        new = g.toPandas()
        print("this is work")
        if not bool(app_data['dl_prefix']=="None"):
          dl_prefix =  app_data['dl_prefix']
          dl_pattern = app_data['dl_pattern']
          pattern_info = [dl_prefix,dl_pattern]
          print(pattern_info)
          dlgu = DlinkGeneratorUtilityV2(pattern_info,data_raw)
          l = dlgu.generated_links
          print(l)
        else:
          print("no deeplink found")  

        '''dlgu = DlinkGeneratorUtilityV2(pattern_info,data_raw)
        print("this is links generated")
        l = dlgu.generated_links
        print(l)'''
        
        #dlgu=DlinkGeneratorUtilityV2(pattern_info,data_raw)
        if(package=="com.abof.android"):
            regular=[]
            sale=[]
            for row in data_raw.price.iteritems():
                regular.append(row[1][0][4])
                sale.append(row[1][1][4])

              
            data_raw = data_raw.drop('price',axis=1)
            data_raw['actualprice']=regular
            data_raw['saleprice']=sale
            
        

        #LIST THE COLUMNS IE JSON KEYS TO PREDICT
        vector_test = list(data_raw.columns)      
        print(vector_test)
        #print("this is the data_raw",data_raw['discount', 'packQty', 'simples', 'url'])

        #LOADING THE PKL FOR THE SVM CLASSIFIER
        tfidf,text_clf = joblib.load("/home/ash/mobiport/datatrans/final/aws4.pkl") 
        
        predictlist = [word.replace('name','names') for word in vector_test]
         

        print(predictlist)
        raw_keys =vector_test
#vector_keys
        collection = db['data_transformation_mappings']
        docs = collection.find({'package':package})
        mapping_dict = {}
        keys = []
        for doc in docs:
            for key in doc.keys():
                keys.append(key)
                mapping_dict[key] = doc[key]
            #print(mapping_dict)

#accessing the value form mongo metadata
        if 'mappings' in keys:
            mapping_value = []
  #print(mapping_dict['mappings'])
            for key,value in mapping_dict['mappings'].items():
                if value in raw_keys:
                    mapping_value.append(key)

        print(mapping_value )
        if len(mapping_value) == len(raw_keys):
            print("this is the mapping values")
            p=mapping_value
            print(p)
       
        else:
            print("this is the predicted keys by pkl file ")
        #USIGN THE TRANSFORM MODULE TO CHANGE THE LIST INTO MATRIX 
            fg = tfidf.transform(predictlist).todense() 

        #PREDICTION FROM THE MACHINE LEARNING MODEL
            predicted = text_clf.predict(fg)
            print(predicted)
            p = list(predicted)
            print(p)

        
        data_new = data_raw    

        #REPLACING THE PREDICTED COLUMNS INTO THE DATAFRAME
        data_new.columns = p
        #data_d['specs'] = data_d['spec'].apply(lambda x: ','.join(str    (i) for i in x),axis=1)
        
      

        print("this is for loop")
        for i in range(0,len(p)):                                                                                                                                                
          if (p[i] == "spec"):
            spec_columns.append(vector_test[i])
          
        print("these are spec",spec_columns)    
        data_new['packagename'] = package 
        data_new['app_category'] = appcat

        #data_new['deeplink']=l

        print("this is the specdata")
        
        data_new = data_new.drop('spec',1)
        spec = new[spec_columns]
        #data_d=data_new.join(spec)
        print("this is the col")
        print(spec.columns)

        
        specs_list=[]
        for row in spec.iterrows():
            specs_list.append(row[1].to_json())

        data_new['specs'] = specs_list    

        '''for n in range(0,len(data_new)):
            data_new['specs'][n]=spec.iloc[n].to_json()'''
        
        #final_dict = (data_d.groupby(j, as_index=False).apply(lambda x: x[list(spec.columns)].to_dict('r')).reset_index().rename(columns={0:'spec'}).to_json(orient='records'))


        #final pandas dataframe mobiport

        print("this is the mongo")
    #CALLING THE MONGO CLIENT

        clients = pymongo.MongoClient()

        
  
        dbb = clients.ashok
        collections = dbb.test
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
checkpointDirectory = 'hdfs://localhost:9000/checkpoint/'
ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)

    
ssc.start() #STARTING THE CONTEXT
    
    
ssc.awaitTermination()
            
            
