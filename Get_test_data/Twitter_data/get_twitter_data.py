#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API 
access_token = "XXXXXXXXXXXXXXX"
access_token_secret = "XXXXXXXXXXXXXXX"
consumer_key = "XXXXXXXXXXXXXXX"
consumer_secret = "XXXXXXXXXXXXXXX"


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['corona', 'coronavirus', '2019 novel coronavirus', 'corona', 'virus', 'trumph', 'donald', 'US election', 'election', 'biden'])
    #stream.filter(track=['visa card', 'visa credit card', 'prepaid card'])
    #stream.filter(track=['prepaid', 'card', 'credit', 'debit'])
    #stream.filter(track=['prepaid card'])

