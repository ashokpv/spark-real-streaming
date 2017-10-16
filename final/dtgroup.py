#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 14 10:50:14 2017

@author: niranjan
"""


from difflib import SequenceMatcher
import pandas as pd
import numpy as np


nameKeys = ['name','title','product_name','model_name','productname','product','campaignname']

descriptionKeys = ['description','shortdescription','p_desc','product_additional_info']


offerpriceKeys = ['discount_price','specialprice','saleprice','discounted_price','sellingprice','offerprice','third_price','effectiveprice','mrp']


pricekeys =['price','prices','mrpprice','marketprice','regularprice']

manufacturer = ['brand_name','brands_filter_facet','brand','brandname','vendordetails']

idKeys = ['productid','id','campaignid','idcatalogconfig','user_id','product_id','uniqueid']

imagekeys= ['images','default_img','p_img_url','search_image','image_url','imgs','normalimgs','imgurl','imageurl']

skuKeys= ['sku','sku_details']

startdateKeys = ['startdate','createdon']

categoryKeys = ['keyword','global_attr_article_type','productcategorytype','pcategoryname']

ratingKeys = ['avg_rating','avgrating','rating','average_rating']

specsKeys= ['title','price','image','description','sale_price','manufacturer','id','sku','date','category','rating']



def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def dynamicMap(df):
   
    for l in (range(len(list(df)))):
        for i in (range(len(nameKeys))):
            sm = similar(list(df)[l],nameKeys[i])
            if list(df)[l] in nameKeys:
                sm=sm+0.7
        if np.mean(sm) >= 0.770:
            df=df.rename(columns = {list(df)[l]:'title'})
        if i == len(nameKeys)-1:
           for j in (range(len(pricekeys))):
               sm1 = similar(list(df)[l],pricekeys[j])
               if list(df)[l] in pricekeys:
                   sm1=sm1+1
           if np.mean(sm1) >= 1:
              df=df.rename(columns = {list(df)[l]:'price'})
           if j == len(pricekeys)-1:
               for k in (range(len(imagekeys))):
                   sm2 = similar(list(df)[l],imagekeys[k])
                   if list(df)[l] in imagekeys:
                       sm2=sm2+0.4
               if np.mean(sm2) >= 0.70:
                   df=df.rename(columns = {list(df)[l]:'image'})
                   #print('TEst')
               if k == len(imagekeys)-1:
                    for m in (range(len(descriptionKeys))):
                        sm3 = similar(list(df)[l],descriptionKeys[m])
                        if list(df)[l] in descriptionKeys:
                            sm3=sm3+0.3
                        #print('descTEst')
                    if np.mean(sm3) >= 0.70:
                       df=df.rename(columns = {list(df)[l]:'description'})
                    if m == len(descriptionKeys)-1:
                        for n in (range(len(offerpriceKeys))):
                            sm4 = similar(list(df)[l],offerpriceKeys[n])
                            if list(df)[l] in offerpriceKeys:
                               sm4=sm4+0.9
                           # print('OfferTest')
                        if np.mean(sm4) >= 0.70:
                           df=df.rename(columns = {list(df)[l]:'sale_price'})
                        if n == len(offerpriceKeys)-1:
                            for o in (range(len(manufacturer))):
                                sm5 = similar(list(df)[l],manufacturer[o])
                                if list(df)[l] in manufacturer:
                                    sm5=sm5+0.6
                                #4print('TEST')
                            if np.mean(sm5) >= 0.730:
                               df=df.rename(columns = {list(df)[l]:'manufacturer'})
                            if o == len(manufacturer)-1:
                                for p in (range(len(idKeys))):
                                    sm6 = similar(list(df)[l],idKeys[p])
                                    if list(df)[l] in idKeys:
                                        sm6=sm6+0.7
                                if np.mean(sm6) >= 0.70:
                                   df=df.rename(columns = {list(df)[l]:'id'})
                                if p == len(idKeys)-1:
                                   for q in (range(len(skuKeys))):
                                       sm7 = similar(list(df)[l],skuKeys[q])
                                       if list(df)[l] in skuKeys:
                                           sm7=sm7+0.3
                                   if np.mean(sm7) >= 0.70:
                                       df=df.rename(columns = {list(df)[l]:'sku'})
                                   if q == len(skuKeys)-1:
                                       for r in (range(len(startdateKeys))):
                                           sm8 = similar(list(df)[l],startdateKeys[r])
                                           if list(df)[l] in startdateKeys:
                                               sm8=sm8+0.3
                                       if np.mean(sm8) >= 0.70:
                                           df=df.rename(columns = {list(df)[l]:'date'})
                                       if r == len(startdateKeys)-1:
                                           for s in (range(len(categoryKeys))):
                                               sm9 = similar(list(df)[l],categoryKeys[s])
                                               print(sm9)
                                               if list(df)[l] in categoryKeys:
                                                   sm9=sm9+0.8
                                           if np.mean(sm9) >= 0.80:
                                              df=df.rename(columns = {list(df)[l]:'category'})
                                           if s == len(categoryKeys)-1:
                                               for t in (range(len(ratingKeys))):
                                                   sm10 = similar(list(df)[l],ratingKeys[t])
                                                   print(sm10)
                                                   if list(df)[l] in ratingKeys:
                                                       sm10=sm10+0.3
                                               if np.mean(sm10) >= 0.70:
                                                  df=df.rename(columns = {list(df)[l]:'rating'})
                                  
                      
    return df

def specsMap(df):
    for l in (range(len(list(df)))):
        if list(df)[l] not in specsKeys:
            df=df.rename(columns={list(df)[l]:'spec'})
            print(list(df))
    return list(df)
    
def manage_dupe_cols(columns):
    counts = {}
    for i, col in enumerate(columns):
        cur_count = counts.get(col, 0)
        if cur_count > 0:
            columns[i] = '%s_%d' % (col, cur_count)
        counts[col] = cur_count + 1
    return columns
