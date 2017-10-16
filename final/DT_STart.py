__author__ = 'samrat'


from DT_Algorithm_new import *
import pandas as pd

class DT_Start(object):

    @staticmethod
    def start_DT_main(api_data):

        data_transform_obj = DataTransformation()
        final_mapped_keys = data_transform_obj.data_tranformation_main(df=api_data)
        print ("final_mapped_keys are :")
        print (final_mapped_keys)

if __name__ == "__main__":

    DT_start_obj = DT_Start()
    DT_Start.start_DT_main(df)