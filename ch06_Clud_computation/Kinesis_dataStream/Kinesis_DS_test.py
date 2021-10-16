import json, uuid, boto3
from os import read
import settings
from settings import region_name, aws_access_key_id, aws_secret_access_key
import datetime
import time
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

# Get stream status function
def get_status(stream_name):
    stream_details = client.describe_stream(StreamName=stream_name)
    description = stream_details.get('StreamDescription')
    status = description.get('StreamStatus')
    return status

# Create stream function
def create_stream(stream_name):
    try:
        # create the stream
        client.create_stream(StreamName=stream_name, ShardCount=1)
        print('stream {} created in region {}'.format(stream_name, region_name))
    except Exception as e:
        if e.__class__.__name__ == 'ResourceInUseException':
            print('stream {} already exists in region {}'.format(stream_name, region_name))
        else:
             raise e

# wait for the stream to become active
    while get_status(stream_name) != 'ACTIVE':
        time.sleep(1)
    print('stream {} is active'.format(stream_name))

# get the data
# defining a data for testing
# read the data from the test json file and put in 'data'
test_file = '/Users/sandipan/personal_project/gitHub/spark_book/ch06_Clud_computation/Data/test_twitter.json'
f = open(test_file)
data = json.load(f)
partition_key = str(uuid.uuid4())
client.put_record(StreamName='twitter_ds', Data=json.dumps(data), PartitionKey=partition_key)
f.close()

# print(create_stream(stream_name))
# print(get_status(stream_name))



# ip_addr='8.8.8.8'
# timestamp = datetime.datetime.utcnow()
# part_key = ip_addr
# data = timestamp.isoformat()

# client.put_record(StreamName='twitter_ds', Data=data, PartitionKey=part_key)

# response = self.kinesis_client.get_shard_iterator(
#                 StreamName=self.name, ShardId=self.details['Shards'][0]['ShardId'],
#                 ShardIteratorType='LATEST')