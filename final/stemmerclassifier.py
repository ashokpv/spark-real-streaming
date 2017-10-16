import nltk
import pandas as pd
import numpy as np
from sklearn.cross_validation import train_test_split
from sklearn.metrics import classification_report
import sklearn.metrics
from pandas import DataFrame,Series
from sklearn.feature_extraction.text import TfidfVectorizer
from time import time
import matplotlib.pyplot as plt



#UPLOADING THE TRAINING DATA SET INTO THE MODEL 
jobs_df=pd.read_csv('/home/ash/mobiport/datatrans/final/lab.csv',index_col=False,na_values='',na_filter=True,usecols=['title_desc','category'])
sum(jobs_df.title_desc.isnull()) #again checking that whether there are still missing values in that column:              
jobs_df.describe(include='all')


#REMOVES THE DUPLICATES FROM THE DATAFRAME
jobs_df.ix[jobs_df.duplicated(keep='first'),]

jobs_df2=jobs_df.drop_duplicates()


#COUNTS THE NUMBER OF CATEGORIES TO PREDICT 
category_counter={x:0 for x in set(jobs_df2['category'])}

for each_cat in jobs_df2['category']:                                                                                     
    category_counter[each_cat]+=1 

print(category_counter) 

corpus=jobs_df2.title_desc 

#SPLITTING THE WORDS FOR HIGH ACCURACY
all_words=[w.split() for w in corpus]                                                                                     
                                                                                                                 
all_flat_words=[ewords for words in all_words for ewords in words]     
                                                                                          
from nltk.corpus import stopwords                                                                                                
                                                                                                                  
all_flat_words_ns=[w for w in all_flat_words if w not in stopwords.words("english")]
             
set_nf=set(all_flat_words_ns)

print("Number of unique vocabulary words in the text_description column of the dataframe: %d"%len(set_nf))


#MAKING THE WORDS INTO LOWER 
jobs_df2.title_desc.str.lower() 
import Stemmer

#SPECIFYING THE STEMMER WORDS TO BE REMOVED FROM THE DATAFRAME
english_stemmer = Stemmer.Stemmer('en')
class StemmedTfidfVectorizer(TfidfVectorizer):
     def build_analyzer(self):
         analyzer = super(TfidfVectorizer, self).build_analyzer()
         return lambda doc: english_stemmer.stemWords(analyzer(doc))
     
tfidf = StemmedTfidfVectorizer(min_df=1, stop_words='english', analyzer='word', ngram_range=(1,1))


#CONVERTING THE WORDS TO DENSE MATRIX
corpus=jobs_df2.title_desc                                                                                                
tfidf_matrix=tfidf.fit_transform(corpus).todense()                                                          
tfidf_names=tfidf.get_feature_names()  
print("Number of TFIDF Features: %d"%len(tfidf_names)) 
training_time_container={'linear_svm':0}                           
prediction_time_container={'linear_svm':0}                
accuracy_container={'linear_svm':0}


variables = tfidf_matrix                                                                                                  
labels = jobs_df2.category                                             

#SPLITTING THE DATA FOR TRAINING AND TESTING IN THE RATIO OF 75%:25%
variables_train, variables_test, labels_train, labels_test  =   train_test_split(variables, labels, test_size=.3)
print('Shape of Training Data: '+str(variables_train.shape))                                                              
print('Shape of Test Data: '+str(variables_test.shape))    


'''d=['pro_desc','pro_id','productname','img_url','SKInumber','number']    

predictlist = [word.replace('name','names') for word in d]
print(predictlist)  
fg=tfidf.transform(predictlist).todense() '''
from sklearn import linear_model

svm_classifier=linear_model.SGDClassifier(loss='log',alpha=0.0001)
#IMPORTING THE LINEAR MODEL 
t0=time()
svm_classifier=svm_classifier.fit(variables, labels)
training_time_container['linear_svm']=time()-t0
print("Training Time: %fs"%training_time_container['linear_svm'])

t0=time()
svm_predictions=svm_classifier.predict(variables_test)
prediction_time_container['linear_svm']=time()-t0
print("Prediction Time: %fs"%prediction_time_container['linear_svm'])


#ACCUARCY SCORE AND THE CONFUSION MATRIX
accuracy_container['linear_svm']=sklearn.metrics.accuracy_score(labels_test, svm_predictions)
print ("Accuracy Score of Linear SVM Classifier: %f"%accuracy_container['linear_svm'])
print(sklearn.metrics.confusion_matrix(labels_test,svm_predictions))
'''svm_predictions=svm_classifier.predict(fg)
svm_predictions 
'''
