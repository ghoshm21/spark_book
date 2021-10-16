import gzip
import datetime
import time
import json, uuid, boto3
from os import close, read
# import settings
from settings import region_name, aws_access_key_id, aws_secret_access_key

# set the application name
application = 'kinesis'
# stream name
stream_name = "twitter_ds"
# connect to the Kinesis stream
client = boto3.client(application,
                    region_name=region_name,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                    )
file_name = '/Users/sandipan/personal_project/gitHub/spark_book/ch06_Clud_computation/twitter_capture.txt'
# open the file for append
file_data = open(file_name, 'a')
next_itr = []
next2_itr = []
decompressed_data = []

def write_data(record_response):
    data = record_response['Records'][0]['Data']
    decom_data = gzip.decompress(data)
    decode_data = decom_data.decode('utf-8')
    json_data = json.loads(decode_data)
    # json_data_dump = json.dumps(json_data)
    print('writing data for: ',  str(json_data['id']))
    file_data.write(json.dumps(json_data))
    file_data.write('\n')

            
# get the stream details
stream_details = client.describe_stream(StreamName=stream_name)

# run the process in loop
while True:
    if len(next_itr) == 0 :
        for shards in stream_details.get('StreamDescription')['Shards']:
            shard_id = shards['ShardId']
            # print(shard_id)
            iter_response = client.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')
            shard_iterator = iter_response['ShardIterator']
            record_response = client.get_records(ShardIterator=shard_iterator, Limit=1)
            next_itr.append(record_response['NextShardIterator'])
            if len(record_response['Records'])> 0:
                write_data(record_response)
            else:
                print('No records')
    else:
        next2_itr = []
        for itr in next_itr:
            # print(itr)
            record_response = client.get_records(ShardIterator=itr, Limit=1)
            next2_itr.append(record_response['NextShardIterator'])
            if len(record_response['Records'])> 0:
                write_data(record_response)
            else:
                print('No records')
        next_itr = next2_itr
    time.sleep(5)
f = close(file_name)