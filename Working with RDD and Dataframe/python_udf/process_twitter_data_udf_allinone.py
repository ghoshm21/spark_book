'''
Documentation, License etc.
list of contractions:- https://en.wikipedia.org/wiki/Wikipedia:List_of_English_contractions
@package process_twitter_data
'''
# All python functions are into a single UDF

from pyspark.sql import SparkSession
import json
from bs4 import BeautifulSoup
import unidecode
# from pycontractions import Contractions
import contractions
import re
import string
import time

start_time = time.perf_counter()
# create spark session
spark = SparkSession.builder.appName('python udf example') \
                            .getOrCreate()

from pyspark.sql.functions import udf, struct, col
from pyspark.sql.types import * 
import pyspark.sql.functions as func

from pyspark.sql.functions import *
#--------------------------------------------------------------------------#

# all data processing functions and UDF

def apply_all(text):
    # Remove any web url starting with http or www
    PUNCT_TO_REMOVE = string.punctuation
    text = 'NULL' if text is None else re.sub(r'(http|https|ftp|ssh|sftp|www)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?', '' , text)
    soup = BeautifulSoup(text, "lxml")
    stripped_text = soup.get_text(separator=" ", strip=True)
    textu = unidecode.unidecode(stripped_text)
    textu = textu.replace('\t', ' ')
    textu = textu.replace('\r', ' ')
    textu = textu.replace('\n', ' ')
    texta = textu.encode('ascii', 'replace').decode(encoding="utf-8")
    texta = texta.replace('?', '')
    texta = re.sub(r'([a-z0-9+._-]+@[a-z0-9+._-]+\.[a-z0-9+_-]+)', '', texta)
    texta = str(texta).lower()
    texta = contractions.fix(str(texta))
    texta = texta.translate(str.maketrans('', '', PUNCT_TO_REMOVE))
    texta = re.sub(' +', ' ', str(texta))
    return texta

apply_all_udf = udf(lambda row: apply_all(row), StringType())
spark.udf.register("apply_all_udf", apply_all, StringType())

# count hashtag
def get_hashtag(data):
    data = 'NULL' if data is None else str(data)
    hashtag = [tag for tag in data.split() if tag.startswith('#')]
    return hashtag

get_hashtag_udf = udf(lambda row: get_hashtag(row), StringType())
spark.udf.register("get_hashtag_udf", get_hashtag, StringType())

# count mentions
def get_mention(data):
    data = 'NULL' if data is None else str(data)
    mention = [tag for tag in data.split() if tag.startswith('@')]
    return mention

get_mention_udf = udf(lambda row: get_mention(row), StringType())
spark.udf.register("get_mention_udf", get_mention, StringType())

def get_email_address(text):
    # Remove any email address
    text = 'NULL' if text is None else str(text)
    email = [email for email in re.findall(r'([a-z0-9+._-]+@[a-z0-9+._-]+\.[a-z0-9+_-]+)', text)]
    return email

get_email_address_udf = udf(lambda row: get_email_address(row), StringType())
spark.udf.register("get_email_address_udf", get_email_address, StringType())

# load the real data
data = spark.read.option("samplingRation", 0.5).json("/mnt/e/Training_Data/Corona_twiter_data/*.json")
data = data.withColumn("is_it_a_retweet",when(substring('text',1,2) == 'RT', 'Y').otherwise('N'))
# create the view for spark sql
data.createOrReplaceTempView("data")
# get only the required columns, to reduce the data size
filter_data_pre_process = spark.sql("""select distinct id, lang, created_at, source,
user.id_str as user_id_str, user.name as user_name,
user.location as user_location, user.description as user_description,
case when is_it_a_retweet = 'Y' and retweeted_status.truncated = 'true'
            then retweeted_status.extended_tweet.full_text
    when is_it_a_retweet = 'Y' and retweeted_status.truncated <> 'true'
            then retweeted_status.text
    when is_it_a_retweet <> 'Y' and truncated = 'true'
            then extended_tweet.full_text
    when is_it_a_retweet <> 'Y' and truncated <> 'true'
            then text
end as tweet_text
from data
where lang = 'en' """)

# create the temp view
filter_data_pre_process.createOrReplaceTempView("filter_data_pre_process")
# remove_special_characters_udf
pre_process_data = spark.sql("""select id, lang, created_at, source,
user_id_str, user_name, user_location, user_description, tweet_text,
    get_hashtag_udf(tweet_text) as hashtag,
    get_mention_udf(tweet_text) as mention,
    get_email_address_udf(tweet_text) as emails,
    apply_all_udf(tweet_text) as clean_tweet_text
    from filter_data_pre_process""").where("clean_tweet_text is not null")

pre_process_data.coalesce(12) \
                .write.mode('overwrite') \
                .option("compression", "snappy") \
                .parquet("/mnt/e/Training_Data/Corona_twiter_data/corona_pre_process_out")

end_time = time.perf_counter()
print(f"Completed the code running in  {end_time - start_time:0.4f} seconds")