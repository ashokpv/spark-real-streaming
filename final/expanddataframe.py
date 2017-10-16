def get_expanded_nested(outerkey):
          try:
              return outerkey[0]['price']
          except IndexError:
              return None
          



f= df.prices.apply(get_expanded_nested)
l = f.tolist()




      sp = ['prices', 0, 'price']
      innerkey = sp[2]
      outerkey = sp[0]
      def get_expanded_nested(outerkey):
                try:
                    return outerkey[sp[1]][innerkey]
                except IndexError:
                    return None
                
      
          def nested_two_level(self, key_list ,api_data):
      nested_data = api_data[key_list[0]].apply(lambda x: x.get(key_list[1]))
      return nested_data

      
     f= df[sp[0]].apply(get_expanded_nested)
     l = f.tolist()
     df['saleprice'] = f.to_frame()

if(package=="com.abof.android"):
          regular=[]
          sale=[]
          for row in api_data.price.iteritems():
              regular.append(row[1][0]['value'])
              sale.append(row[1][1]['value'])

          #api_data = api_data.drop('price',axis=1)
          api_data['price']=regular
          api_data['sale_price']=sale    
          logging.info("%s",api_data)
          del(final_mapped_keys['price'])
          try:
            
            del(final_mapped_keys['sale_price'])
          except Exception as e :
            logging.info(str(e))  


        else : 
          logging.info("using nested value problem")

if isinstance(c,list):                       
     ...:       c1=c[0]                                                   
     ...:       f.append(search_it(c1, key)) #SEARCH AND APPEND THE RESULT
     ...:      else:                                  
     ...:       f.append(search_it(c, key))          

     flat = [x for x in flat if x != None] 

             #first_price_list = OrderedDict(sorted(first_price_list.items(), key=lambda t: t[0]))


        if not json_file_name and not df_data:
            print ("Error in the input !!")
            exit(1)

        if json_file_name:
            self.json_file_name = json_file_name
            df = pd.read_json(json_file_name)
        else:
            self.api_data = df_data
            list1 = json.load(self.api_data)
            df = pd.DataFrame(list1)