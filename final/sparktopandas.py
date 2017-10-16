 a=sc.text
 z=a.collect()

 b=pd.DataFrame(m)

 s=""
 for row in b.ite
  s=s+","+(row[1][0])

s=s[1:] 
v='['+ s + ']'          
pd.read_json(v)     


bb="["+mm+"]"  
pd.read_json(bb)


 ddf = pd.read_csv('/home/ash/mobiport/api_train_data/etsy_featured.csv')

 train_datas = ddf.iloc[:,:-1]
 train_datas = train_datas.drop(train_datas.columns[[0]],axis=1)

 test_label = model_builder.predict_labels_decision_tree(test_data=train_datas, clf=decision_tree_model)

 target = ddf.target.tolist()
 le.fit(target)
 


 m=le.transform(target)

 accuracy = accuracy_score(m , test_label)*100
 accuracy
