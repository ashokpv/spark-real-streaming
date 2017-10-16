__author__ = 'ashok and samrat'

from difflib import SequenceMatcher
import pandas as pd
import numpy as np
import configparser as cp
import argparse
import json
from collections import OrderedDict
import logging
logging.basicConfig(filename='/home/ash/mobiport/example1.log',level=logging.DEBUG)

class DataTransformation(object):

    def __init__(self):
        self.json_file_name = None
        self.api_data = None
        self.nameKeys = None
        self.descriptionKeys = None
        self.offerpriceKeys = None
        self.priceKeys = None
        self.manufacturer = None
        self.idKeys = None
        self.imageKeys = None
        self.skuKeys = None
        self.startdateKeys = None
        self.specsKeys = False
        self.categoryKeys = None
        self.ratingKeys = None
        self.DT_dict = {}
        self.missing_keys = []
        self.nested_keys = []
        self.final_mapped_keys = {}
        self.final_mapped_data = {}

 

    @staticmethod
    def similar(a, b):
        return SequenceMatcher(None, a, b).ratio()

    def read_keys_from_config_file(self, config_file):

        config = cp.RawConfigParser()
        config.read(config_file)

        with open(config_file, 'r') as fin:
            print (fin.read())
         

      
        config_data = open(config_file, "r")


        
       
        ns1=config.get('KeySection','nameKeys')
        self.nameKeys=ns1.split(',')

        ds2=config.get('KeySection','descriptionKeys')
        self.descriptionKeys=ds2.split(',')

        os3=config.get('KeySection','offerpriceKeys')
        self.offerpriceKeys=os3.split(',')

        ps4=config.get('KeySection','priceKeys')
        self.priceKeys=ps4.split(',')

        ms5=config.get('KeySection','manufacturer')
        self.manufacturer=ms5.split(',')

        is6=config.get('KeySection','idKeys')
        self.idKeys=is6.split(',')

        ims7=config.get('KeySection','imageKeys')
        self.imageKeys=ims7.split(',')

        ss8=config.get('KeySection','skuKeys')
        self.skuKeys=ss8.split(',')

        sts9=config.get('KeySection','startdateKeys')
        self.startdateKeys=sts9.split(',')

        sps10=config.get('KeySection','specsKeys')
        self.specsKeys=sps10.split(',')

        sps11=config.get('KeySection','categoryKeys')
        self.categoryKeys=sps11.split(',')

        cs12=config.get('KeySection','ratingKeys')
        self.ratingKeys =cs12.split(",")


    def check_if_DT_complete(self, df_orig, df_new):


        #check for missing keys after initial DT processing
        for i in range(len(self.specsKeys)):
   
            if self.specsKeys[i] in list(df_new):
      
                #print "Key is properly mapped : " + str(self.specsKeys[i])
                val_index = list(df_new).index(self.specsKeys[i])
                
                self.final_mapped_data[list(df_new)[val_index]] = df_orig.iloc[0][val_index]
                self.final_mapped_keys[list(df_new)[val_index]] = [list(df_orig)[val_index]]
                if isinstance(df_orig.iloc[0][val_index], (dict, list)):
                   self.nested_keys.append(self.specsKeys[i])
            else:
                self.final_mapped_keys[self.specsKeys[i]] = None
                self.final_mapped_data[self.specsKeys[i]] = None
                self.missing_keys.append(self.specsKeys[i])


        #return missing_keys


    def fix_missing_keys(self, df_orig, df_new):
       
        for key in self.nested_keys:

            if key == 'image':
                print("image is nested!!")
                self.get_image_entity()


    def check_if_string_in_file(self, api_data, test_strings):
       
        for line in api_data:
            for test_string in test_strings:
                if test_string in line:
                    return 1

        return 0


    #This will check if image extension is present in JSON and get appr keys
    def get_image_entity(self):
        image_found = 0
        image_extensions = ['.jpg', '.jpeg', '.png']

        try :
            image_found = self.check_if_string_in_file(file_name=self.api_data, test_strings=image_extensions)
        except Exception as e :
            print(str(e))    
        '''
        for image_extension in image_extensions:
            if image_extension in df:
        '''

    def fix_nested_keys(self, df_orig, df_new):


        for key in self.nested_keys:
          try:
            if (key == "price" or key == "sale_price"):
                print ("price is nested!!")
                self.fix_price_entity(key)
            
            if key == 'image':
                print ("image is nested!!")
                try:
                    self.fix_image_entity()
                except Exception as e :
                    print(str(e))
          except Exception as  e :
            print(str(e))               
    def check_for_image_ext(value):
        image_extensions = ['.jpg', '.jpeg', '.png']
        img_found = [image_extension for image_extension in image_extensions if image_extension in value]
        return img_found

    def fix_image_entity(self):
        parent_image_index = self.final_mapped_keys['image'][0]
        images_list = []
        image_old_value = self.final_mapped_data['image']



        if isinstance(image_old_value, dict):
            images_len = 1
            first_image_list = image_old_value
        else:
            images_len = len(image_old_value)
            first_image_list = image_old_value[0]



        #first_price_list = price_old_value[0]

        image_extensions = ['.jpg', '.jpeg', '.png']
        img_found = []

        if not isinstance(first_image_list, list):
            img_found = self.check_for_image_ext(value=first_image_list)
            key = 0
        else:
            for key in first_image_list:
            
                value = first_image_list[key]
                img_found = self.check_for_image_ext(value=value)
                if img_found:
                    break

        if img_found:

            self.final_mapped_keys['image'] = [parent_image_index, 0, key]
        else:

            self.final_mapped_keys['image'] = None

    
    def fix_price_entity(self,key):
      try:
        if key == 'price':
            print ("price is nested!!")
            key_prop = self.priceKeys
            
        elif key == 'sale_price':
            print ("sale_price is nested!!")
            key_prop = self.offerpriceKeys
       
        
        parent_price_index = self.final_mapped_keys[key][0]
        prices_list = []
        price_old_value = self.final_mapped_data[key]

        if isinstance(price_old_value, dict):
            prices_len = 1
            first_price_list = price_old_value
        else:
            prices_len = len(price_old_value)
            first_price_list = price_old_value[0]



        for l in (range(len(list(first_price_list)))):
            for i in (range(len(key_prop))):

                sm = DataTransformation.similar(list(first_price_list)[l],key_prop[i])
                if list(first_price_list)[l] in key_prop:
                    sm=sm+1
            if np.mean(sm) >= 1:
         
                break
                #df=df.rename(columns = {list(df)[l]:'title'})
        price_index = list(first_price_list)[l]
        #price_index = l

        if prices_len == 1:
           
            self.final_mapped_keys[key] = [parent_price_index, price_index]
        elif prices_len == 2:
            for i in range(prices_len):

                price_col = list(first_price_list)[l]

                real_val = price_old_value[i][str(price_col)]

                prices_list.append(real_val)

            #prices_list = [200, 150, 100, 600]
            min_index = 0
            max_index = 0
            min_val = prices_list[0]
            max_val = prices_list[0]



            for i in range(len(prices_list)):
                if prices_list[i] < min_val:
                    min_val = prices_list[i]
                    min_index = i
                elif prices_list[i] > max_val:
                    max_val = prices_list[i]
                    max_index = i




            self.final_mapped_keys[key] = [parent_price_index, max_index, price_index]
            self.final_mapped_keys['sale_price'] = [parent_price_index, min_index, price_index]
            #orig_price_index = self.

      except Exception as e :
        print(str(e))    
        
    def data_tranformation_main(self, df):

        df.columns = [x.lower() for x in df.columns]

        self.read_keys_from_config_file(config_file='/home/ash/mobiport/datatrans/final/myprop.properties')

        printlistOld=  [self.nameKeys,self.priceKeys,self.imageKeys,self.descriptionKeys,self.offerpriceKeys,self.manufacturer,self.idKeys,self.skuKeys,self.startdateKeys,self.categoryKeys,self.ratingKeys]

        for j in range(len(printlistOld)):
            print(printlistOld[j],'===Old==>',len(printlistOld[j]))

        self.keysAdd(df[1:2])

        df_new = self.dynamicMap(df)

        df_final = self.specsMap(df=df_new)


        for keys,predicted in zip(list(df),list(df_final)):
            print((keys),"==",(predicted))



        self.check_if_DT_complete(df_orig=df, df_new=df_final)

        if self.nested_keys:
            keys_with_nested_values = self.fix_nested_keys(df_orig=df, df_new=df_final)

        if self.missing_keys:

            keys_with_nested_values = self.fix_missing_keys(df_orig=df, df_new=df_final)


        print ("about to print keys")
        real_mapped_keys = self.final_mapped_keys
        return real_mapped_keys

    def dynamicMap(self, df):

        for l in (range(len(list(df)))):
            for i in (range(len(self.nameKeys))):
                sm = DataTransformation.similar(list(df)[l],self.nameKeys[i])
                if list(df)[l] in self.nameKeys:
                    sm=sm+0.7
            if np.mean(sm) >= 0.770:
                df=df.rename(columns = {list(df)[l]:'title'})
            if i == len(self.nameKeys)-1:

                for j in (range(len(self.priceKeys))):

                   sm1 = DataTransformation.similar(list(df)[l],self.priceKeys[j])
                   if list(df)[l] in self.priceKeys:
    
                       sm1=sm1+1
                if np.mean(sm1) >= 1:
                  df=df.rename(columns = {list(df)[l]:'price'})
                if j == len(self.priceKeys)-1:
                
                    for k in (range(len(self.imageKeys))):

                        sm2 = DataTransformation.similar(list(df)[l],self.imageKeys[k])
                        if list(df)[l] in self.imageKeys:
        
                            sm2=sm2+0.6
                    if np.mean(sm2) >= 0.70:

                        df=df.rename(columns = {list(df)[l]:'image'})
                    if k == len(self.imageKeys)-1:
                        for m in (range(len(self.descriptionKeys))):
                            sm3 = DataTransformation.similar(list(df)[l],self.descriptionKeys[m])
                            if list(df)[l] in self.descriptionKeys:
                                sm3=sm3+0.3
                        if np.mean(sm3) >= 0.70:
                            df=df.rename(columns = {list(df)[l]:'description'})
                        if m == len(self.descriptionKeys)-1:
                            for n in (range(len(self.offerpriceKeys))):
                                sm4 = DataTransformation.similar(list(df)[l],self.offerpriceKeys[n])
                                if list(df)[l] in self.offerpriceKeys:
                                   sm4=sm4+0.9
                            if np.mean(sm4) >= 0.70:
                               df=df.rename(columns = {list(df)[l]:'sale_price'})
                            if n == len(self.offerpriceKeys)-1:
                       
                                for o in (range(len(self.manufacturer))):
                                    sm5 = DataTransformation.similar(list(df)[l],self.manufacturer[o])
                                
                                    if list(df)[l] in self.manufacturer:
                                  
                                        sm5=sm5+0.6
                         
                           
                                if np.mean(sm5) >= 0.730:
                                   df=df.rename(columns = {list(df)[l]:'manufacturer'})
                                if o == len(self.manufacturer)-1:
                                    for p in (range(len(self.idKeys))):
                                        sm6 = DataTransformation.similar(list(df)[l],self.idKeys[p])
                                        if list(df)[l] in self.idKeys:
                                            sm6=sm6+0.7
                                    if np.mean(sm6) >= 0.70:
                                       df=df.rename(columns = {list(df)[l]:'id'})
                                    if p == len(self.idKeys)-1:
                                       for q in (range(len(self.skuKeys))):
                                           sm7 = DataTransformation.similar(list(df)[l],self.skuKeys[q])
                                           if list(df)[l] in self.skuKeys:
                                               sm7=sm7+0.3
                                       if np.mean(sm7) >= 0.70:
                                           df=df.rename(columns = {list(df)[l]:'sku'})
                                       if q == len(self.skuKeys)-1:
                                           for r in (range(len(self.startdateKeys))):
                                               sm8 = DataTransformation.similar(list(df)[l],self.startdateKeys[r])
                                               if list(df)[l] in self.startdateKeys:
                                                   sm8=sm8+0.3
                                           if np.mean(sm8) >= 0.70:
                                               df=df.rename(columns = {list(df)[l]:'date'})
                                           if r == len(self.startdateKeys)-1:
                                               for s in (range(len(self.categoryKeys))):
                                                   sm9 = DataTransformation.similar(list(df)[l],self.categoryKeys[s])
                                                   if list(df)[l] in self.categoryKeys:
                                                       sm9=sm9+0.8
                                               if np.mean(sm9) >= 0.80:
                                                  df=df.rename(columns = {list(df)[l]:'category'})
                                               if s == len(self.categoryKeys)-1:
                                                   for t in (range(len(self.ratingKeys))):
                                                       sm10 = DataTransformation.similar(list(df)[l],self.ratingKeys[t])
                                                       if list(df)[l] in self.ratingKeys:
                                                           sm10=sm10+0.3
                                                   if np.mean(sm10) >= 0.70:
                                                      df=df.rename(columns = {list(df)[l]:'rating'})


        return df



    def specsMap(self, df):
        for l in (range(len(list(df)))):
            if list(df)[l] not in self.specsKeys:
                df=df.rename(columns={list(df)[l]:'spec'})
        return df



    def keysAdd(self, df):
    
        for l in (range(len(list(df)))):
            for i in (range(len(self.nameKeys))):
                sm = DataTransformation.similar(list(df)[l],self.nameKeys[i])
            if np.mean(sm) >= 0.7:
                self.nameKeys.append(list(df)[l])
            if i == len(self.nameKeys)-1:
                for j in (range(len(self.priceKeys))):
                    sm1 = DataTransformation.similar(list(df)[l],self.priceKeys[j])
                if np.mean(sm1) >= 0.7:
                    self.priceKeys.append(list(df)[l])
                if j == len(self.priceKeys)-1:
                    for k in (range(len(self.imageKeys))):
                        sm2 = DataTransformation.similar(list(df)[l],self.imageKeys[k])
                    if np.mean(sm2) >= 0.70:
                        self.imageKeys.append(list(df)[l])
                    if k == len(self.imageKeys)-1:
                        for m in (range(len(self.descriptionKeys))):
                            sm3 = DataTransformation.similar(list(df)[l],self.descriptionKeys[m])
                        if np.mean(sm3) >= 0.70:
                            self.descriptionKeys.append(list(df)[l])
                        if m == len(self.descriptionKeys)-1:
                            for n in (range(len(self.offerpriceKeys))):
                                 sm4 = DataTransformation.similar(list(df)[l],self.offerpriceKeys[n])
                            if np.mean(sm4) >= 0.770:
                                self.offerpriceKeys.append(list(df)[l])
                            if n == len(self.offerpriceKeys)-1:
                                for o in (range(len(self.manufacturer))):
                            
                                    sm5 = DataTransformation.similar(list(df)[l],self.manufacturer[o])
                
                                if np.mean(sm5) >= 0.70:

                                    self.manufacturer.append(list(df)[l])

                                if o == len(self.manufacturer)-1:
                                    for p in (range(len(self.idKeys))):
                                        sm6 = DataTransformation.similar(list(df)[l],self.idKeys[p])
                                    if np.mean(sm6) >= 0.70:
                                        self.idKeys.append(list(df)[l])
                                    if p == len(self.idKeys)-1:
                                        for q in (range(len(self.skuKeys))):
                                            sm7 = DataTransformation.similar(list(df)[l],self.skuKeys[q])
                                        if np.mean(sm7) >= 0.70:
                                            self.skuKeys.append(list(df)[l])
                                        if q == len(self.skuKeys)-1:
                                            for r in (range(len(self.startdateKeys))):
                                                sm8 = DataTransformation.similar(list(df)[l],self.startdateKeys[r])
                                            if np.mean(sm8) >= 0.70:
                                                self.startdateKeys.append(list(df)[l])
                                            if r == len(self.startdateKeys)-1:
                                                for s in (range(len(self.categoryKeys))):
                                                    sm9 = DataTransformation.similar(list(df)[l],self.categoryKeys[s])
                                                if np.mean(sm9) >= 0.70:
                                                    self.categoryKeys.append(list(df)[l])
                                                if s == len(self.categoryKeys)-1:
                                                    for t in (range(len(self.ratingKeys))):
                                                        sm10 = DataTransformation.similar(list(df)[l],self.ratingKeys[t])
                                                    if np.mean(sm10) >= 0.66:
                                                       self.ratingKeys.append(list(df)[l])



