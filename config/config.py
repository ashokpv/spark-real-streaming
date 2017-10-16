HDFS_ENDPOINT_IP ="ip-172-31-57-58.ec2.internal"
HDFS_PORT="8020"
MONGO_ENDPOINT_IP="ip-172-31-26-4.ec2.internal"
MONGO_PORT=27011
from config.config import *

print("hdfs://"+HDFS_ENDPOINT_IP+":"+HDFS_PORT+"/apps_data/Food/*")

print(MONGO_ENDPOINT_IP)

print(MONGO_PORT)