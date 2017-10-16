from __future__ import print_function

import time ,sys , pymongo , json , joblib , re , os , Stemmer
import pandas as pd
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext,functions as func,Row
from pyspark.sql import Row, SparkSession
from sklearn.feature_extraction.text import TfidfVectorizer

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

pklfilename = "/home/ash/mobiport/datatrans/final/resaws2.pkl"


#SPARK STREAMING FUNCTION 
def functionToCreateContext():
  sc = SparkContext(appName = "mobiport")
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
        appcat = b[4].decode('utf-8')


        #GET THE SINGLE INSTANCE OF THE SPARK CONTEXT
        sqlc = getSparkSessionInstance(rdd.context.getConf())
        g = sqlc.read.json(rdd)                   
        data_d = g.toPandas()  
        new = g.toPandas() 

        if(package == "com.global.foodpanda.android"):
          cuis=[]
          cit=[]
          for row in data_d.cuisines.tolist():
            cuis.append(list((object['name'] for object in row)))
          for row in data_d.city.tolist():
            cit.append(row['name'])

          data_d=data_d.drop('cuisines',axis=1)  
          data_d=data_d.drop('city',axis=1)

          data_d['cuisines']=cuis
          data_d['city']=cit


        #LIST THE COLUMNS IE JSON KEYS TO PREDICT
        vector_test = list(data_d.columns)      
        print(vector_test)
        tfidf,text_clf = joblib.load(pklfilename) 
        
        predictlist = [word.replace('name','names') for word in vector_test]
         

        print(predictlist)
        
        #USIGN THE TRANSFORM MODULE TO CHANGE THE LIST INTO MATRIX 
        fg = tfidf.transform(predictlist).todense() 

        #PREDICTION FROM THE MACHINE LEARNING MODEL
        predicted = text_clf.predict(fg)
        print(predicted)
        p = list(predicted)

        spec_columns = []
        for i in range(0,len(p)):                                                                                                                                                
          if (p[i] == "spec"):
            spec_columns.append(vector_test[i])
        #REPLACING THE PREDICTED COLUMNS INTO THE DATAFRAME
        data_d.columns = p
        #data_d['specs'] = data_d['spec'].apply(lambda x: ','.join(str    (i) for i in x),axis=1)
        data_d = data_d.drop('spec',1)
        data_d['packagename'] = package 
        data_d['app_category'] = appcat
        #final pandas dataframe mobiport

        spec = new[spec_columns]

        data_d['specs'] = ""
        specs_list=[]
        for row in spec.iterrows():
            specs_list.append(row[1].to_json())

        data_d['specs'] = specs_list

        #for n in range(0,len(data_d)):
        #  data_d['specs'][n] = spec.iloc[n].to_json()

      
    #CALLING THE MONGO CLIENT

        client = pymongo.MongoClient()
        db = client.mobiport
        collection = db.apps_data
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
            
            
