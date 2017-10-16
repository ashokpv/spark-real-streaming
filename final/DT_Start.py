__author__ = 'ashok and samrat'


from DT_Algorithm_new import *
import pandas as pd
import logging
logging.basicConfig(filename='/home/ash/mobiport/example.log',level=logging.DEBUG)

class DT_Start(object):


    def get_args(self):
      parser = argparse.ArgumentParser()
      parser.add_argument("--json_file", help = "Input JSON file name", required = True)
      args = parser.parse_args()
      return(args)

    def generate_spec_value_dict(self,finalkeys,columns):
      finalkeys.append("price")
      finalkeys.append("sale_price")      
      logging.info("this is final keys %s", finalkeys)
      logging.info("this is final keys %s", columns)
      spec = list((set(columns) - set(finalkeys)))
      logging.info("%s",spec)
      dict_value = "spec"
      dict_with_spec = {k:dict_value for k in spec}
      return dict_with_spec,spec

    def rename_columns(self, api_data,final):
      api_data = api_data.rename(columns = final)
      return api_data


    def final_dict(self,final_mapped_keys_copy):
      finalkeys = list(final_mapped_keys_copy.values())      
      finalkeys = [item for sublist in finalkeys for item in sublist]
      finalvalues = list(final_mapped_keys_copy.keys())
      final = dict(zip(finalkeys,finalvalues))
      return final,finalkeys




    def delete_dict_none(self,final_mapped_keys_copy):
      final_mapped_keys_copy = {k:v for k,v in final_mapped_keys_copy.items() if v is not None}
      return final_mapped_keys_copy


    

    def manage_dupe_cols(columns):
      counts = {}
      for i, col in enumerate(columns):
          cur_count = counts.get(col, 0)
          if cur_count > 0:
              columns[i] = '%s_%d' % (col, cur_count)
          counts[col] = cur_count + 1
      return columns

    def key_in_final_mapped_keys_copy(self,api_data,final_mapped_keys_copy):
      nested_keys = {k:v for (k,v) in final_mapped_keys_copy.items() if len(v) > 1}
      #nested_keys = dict((k, v) for k, v in final_mapped_keys_copy.items() if v >=1)
      parameter_keys = list(nested_keys.keys())
      for key in parameter_keys:
        if key in final_mapped_keys_copy.keys():
              api_data,final_mapped_keys_copy = self.expanding_nested_keys(api_data,final_mapped_keys_copy,key)
      return api_data,final_mapped_keys_copy      

                

    def expanding_nested_keys(self,api_data,final_mapped_keys_copy,parameter_key):  
      try :
          
          price_key_list = list(final_mapped_keys_copy[parameter_key])
          logging.info(price_key_list)
          if (len(price_key_list) > 1 ):
            del(final_mapped_keys_copy[parameter_key])               
            def get_expanded_nested(outerkey):
              temp_outer_key = outerkey                     
              for key in price_key_list[1:]:              
                temp_outer_key = temp_outer_key[key]
                          
              return temp_outer_key   
              
            nested_key_data = api_data[price_key_list[0]].apply(get_expanded_nested)    
            logging.info("%s",nested_key_data)
            api_data[parameter_key] = nested_key_data.to_frame()    
            logging.info("%s",api_data)          

      except Exception as e  :
          logging.info(str(e))

      return api_data,final_mapped_keys_copy  

    def parse_json_file_to_dataframe(self,json_file_name):
      apis_data = open(json_file_name, "r")
      api_data=pd.read_json(apis_data)
      return api_data



    def start_DT_main(self,api_data):

        
        
        data_transform_obj = DataTransformation()
        real_final_mapped_keys_copy = data_transform_obj.data_tranformation_main(df=api_data)
        print("%s",real_final_mapped_keys_copy)
        final_mapped_keys_copy = real_final_mapped_keys_copy.copy()
        final_mapped_keys_copy = self.delete_dict_none(final_mapped_keys_copy)
        print("%s",final_mapped_keys_copy)
      
                    
        api_data , final_mapped_keys_copy = self.key_in_final_mapped_keys_copy(api_data,final_mapped_keys_copy)

        final,finalkeys = self.final_dict(final_mapped_keys_copy)
        logging.info("%s",final)
        col = list(api_data.columns)
        api_data = self.rename_columns(api_data,final)
        logging.info("%s",api_data)    
        
        specs_dict,spec_columns = self.generate_spec_value_dict(finalkeys,col)
        logging.info("%s",specs_dict)
        
        api_data = self.rename_colu
        mns(api_data,specs_dict)
        logging.info("%s",api_data)
        logging.info("%s",api_data.loc[0])
        final_list_keys = list(api_data.columns)
        return api_data,spec_columns



if __name__ == "__main__":



    DT_start_obj = DT_Start()
    args = DT_start_obj.get_args()
    print ("json file is :")
    print (args.json_file)
    df=pd.read_json("/home/ash/mobiport/raw_data/abof.txt")
    
    logging.info("%s",df)
    api_data = DT_start_obj.parse_json_file_to_dataframe(args.json_file)
    listing,spec = DT_start_obj.start_DT_main(api_data)
    print("this is the final data ", listing)
    print(list(listing.columns))
    logging.info("this is the spec column names %s", spec)