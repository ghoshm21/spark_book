#Import the necessary methods from tweepy library
# pip3 install tweepy==3.10.0
# pip3 index versions  tweepy

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import gzip
import datetime
import time
import json, uuid, boto3
from os import read
# import settings
from settings import region_name, aws_access_key_id, aws_secret_access_key
from settings import twee_access_token, twee_access_token_secret, twee_consumer_key, twee_consumer_secret

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
        client.create_stream(StreamName=stream_name, ShardCount=2)
        print('stream {} created in region {}'.format(stream_name, region_name))
    except Exception as e:
        if e.__class__.__name__ == 'ResourceInUseException':
            print('stream {} already exists in region {}'.format(stream_name, region_name))
        else:
             raise e


#This is a basic listener that just put the twitter to the data stream.
class StdOutListener(StreamListener):

    def on_data(self, data):
        json_data = json.loads(data)
        json_data_dump = json.dumps(json_data)
        # Convert to bytes
        encoded = json_data_dump.encode('utf-8')
        # Compress
        compressed = gzip.compress(encoded)
        partition_key = str(uuid.uuid4())
        client.put_record(StreamName='twitter_ds', Data=compressed, PartitionKey=partition_key)
        # print (data)
        print('Got the data and loaded into stream: ', json_data['id'])
        return True

    def on_error(self, status):
        print (status)

if __name__ == '__main__':
    # get the data stream created
    create_stream(stream_name)

    # wait for the stream to become active
    while get_status(stream_name) != 'ACTIVE':
        time.sleep(1)
    print('stream {} is active'.format(stream_name))

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(twee_consumer_key, twee_consumer_secret)
    auth.set_access_token(twee_access_token, twee_access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['no time to die'])
    # stream.filter(track=['corona', 'covid', 'lockdown'])

