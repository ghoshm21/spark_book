'''
Documentation, License etc.
list of contractions:- https://en.wikipedia.org/wiki/Wikipedia:List_of_English_contractions
@package process_twitter_data
'''
# $SPARK_HOME/bin/spark-submit --master spark://LAPTOP-7DUT93OF.localdomain:7077 --executor-memory 6700mb ./process_twitter_data_udf_split.py
# All python functions are seperated into moduler UDF

# from pyspark.sql import SparkSession
from __future__ import print_function
import json
from bs4 import BeautifulSoup
import unidecode
# from pycontractions import Contractions
import contractions
import re
import string
import time

# all data processing functions and UDF
def remove_html(input):
    """remove html tags from text"""
    '''Beautiful Soup ranks lxml’s parser as being the best, then html5lib’s, 
    then Python’s built-in parser.'''
    # doing NULL check and proper datatype
    text = 'NULL' if input is None else str(input)
    # soup = BeautifulSoup(text, "html.parser")
    soup = BeautifulSoup(text, "lxml")
    stripped_text = soup.get_text(separator=" ", strip=True)
    return stripped_text

def remove_accented_chars(text):
    """remove accented characters from text, e.g. café"""
    text = 'NULL' if text is None else unidecode.unidecode(text)
    return text

def remove_tabs(input):
    text = 'NULL' if input is None else str(input)
    '''remove all the tab, new line char'''
    text = text.replace('\t', ' ')
    text = text.replace('\r', ' ')
    text = text.replace('\n', ' ')
    return text

def remove_blanks(text):
    '''remove all the more than 1 spaces'''
    text = 'NULL' if text is None else re.sub(' +', ' ', str(text))
    return text

def remove_digits(text):
    # Remove digits, decimal numbers, dates and time format
    text = 'NULL' if text is None else re.sub(r'\d[\.\/\-\:]\d|\d', '', text)
    return text

def remove_all_punctuation(text):
    '''Remove other punctuation, adding fe more
    string.punctuation = !"#$%&\'()*+,-./:;<=>?@[\\]^_{|}~`
    '''
    PUNCT_TO_REMOVE = string.punctuation
    text = 'NULL' if text is None else text.translate(str.maketrans('', '', PUNCT_TO_REMOVE))
    return text

def remove_special_characters(text, remove_digits=False):
    pattern = r'[^a-zA-z0-9\s]' if not remove_digits else r'[^a-zA-z\s]'
    text = 'NULL' if text is None else re.sub(pattern, '', text)
    return text

def ascii_to_string(text):
    # Encodes string to ASCII and decodes to string. This helps in removing any special characters in the database
    text = 'NULL' if text is None else  text.encode('ascii', 'replace').decode(encoding="utf-8")
    '''
    This replaces all special characters with a ?. Replacing this
    '''
    return text.replace('?', '')

def to_lower(text):
    '''conver all to lower'''
    text = 'NULL' if text is None else str(text).lower()
    return text

# def remove_url(text):
#     # Remove any web url starting with http or www
#     text = 'NULL' if text is None else re.sub(r'(www|http)\S+', '', text)
#     return text
#text = 'https://t.co/mtzeFmKmto'
def remove_url(text):
    # Remove any web url starting with http or www
    text = 'NULL' if text is None else re.sub(r'(http|https|ftp|ssh|sftp|www)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?', '' , text)
    return text

# def remove_email_address(text):
#     # Remove any email address
#     text = 'NULL' if text is None else re.sub(r'\S+@\S+', '', text)
#     return text
def remove_email_address(text):
    # Remove any email address
    text = 'NULL' if text is None else re.sub(r'([a-z0-9+._-]+@[a-z0-9+._-]+\.[a-z0-9+_-]+)', '', text)
    return text

def get_email_address(text):
    # Remove any email address
    text = 'NULL' if text is None else str(text)
    email = [email for email in re.findall(r'([a-z0-9+._-]+@[a-z0-9+._-]+\.[a-z0-9+_-]+)', text)]
    return email

def expand_contractions(text):
    text = 'NULL' if text is None else contractions.fix(str(text))
    return text

# count hashtag
def get_hashtag(data):
    data = 'NULL' if data is None else str(data)
    hashtag = [tag for tag in data.split() if tag.startswith('#')]
    return hashtag

# count mentions
def get_mention(data):
    data = 'NULL' if data is None else str(data)
    mention = [tag for tag in data.split() if tag.startswith('@')]
    return mention

#--------------------------------------------------------------------------#
# create spark session
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('python udf example') \
                            .getOrCreate()
    from pyspark.sql.functions import udf, struct, col
    from pyspark.sql.types import * 
    import pyspark.sql.functions as func
    from pyspark.sql.functions import *
#--------------------------------------------------------------------------#
    # start timer
    start_time = time.perf_counter()

    # register the UDFs
    remove_html_udf = udf(lambda row: remove_html(row), StringType())
    spark.udf.register("remove_html_udf", remove_html, StringType())

    remove_accented_chars_udf = udf(lambda row: remove_accented_chars(row), StringType())
    spark.udf.register("remove_accented_chars_udf", remove_accented_chars, StringType())

    remove_tabs_udf = udf(lambda row: remove_tabs(row), StringType())
    spark.udf.register("remove_tabs_udf", remove_tabs, StringType())

    remove_blanks_udf = udf(lambda row: remove_blanks(row), StringType())
    spark.udf.register("remove_blanks_udf", remove_blanks, StringType())

    remove_digits_udf = udf(lambda row: remove_digits(row), StringType())
    spark.udf.register("remove_digits_udf", remove_digits, StringType())

    remove_all_punctuation_udf = udf(lambda row: remove_all_punctuation(row), StringType())
    spark.udf.register("remove_all_punctuation_udf", remove_all_punctuation, StringType())

    remove_special_characters_udf = udf(lambda row: remove_special_characters(row), StringType())
    spark.udf.register("remove_special_characters_udf", remove_special_characters, StringType())

    ascii_to_string_udf = udf(lambda row: ascii_to_string(row), StringType())
    spark.udf.register("ascii_to_string_udf", ascii_to_string, StringType())

    to_lower_udf = udf(lambda row: to_lower(row), StringType())
    spark.udf.register("to_lower_udf", to_lower, StringType())

    remove_url_udf = udf(lambda row: remove_url(row), StringType())
    spark.udf.register("remove_url_udf", remove_url, StringType())

    remove_email_address_udf = udf(lambda row: remove_email_address(row), StringType())
    spark.udf.register("remove_email_address_udf", remove_email_address, StringType())

    get_email_address_udf = udf(lambda row: get_email_address(row), StringType())
    spark.udf.register("get_email_address_udf", get_email_address, StringType())

    expand_contractions_udf = udf(lambda row: expand_contractions(row), StringType())
    spark.udf.register("expand_contractions_udf", expand_contractions, StringType())

    get_hashtag_udf = udf(lambda row: get_hashtag(row), StringType())
    spark.udf.register("get_hashtag_udf", get_hashtag, StringType())

    get_mention_udf = udf(lambda row: get_mention(row), StringType())
    spark.udf.register("get_mention_udf", get_mention, StringType())

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
                                where lang = 'en'
                            """)

    # create the temp view
    filter_data_pre_process.createOrReplaceTempView("filter_data_pre_process")
    # remove_special_characters_udf
    pre_process_data = spark.sql("""select id, lang, created_at, source,
                            user_id_str, user_name, user_location, user_description, tweet_text,
                            get_hashtag_udf(tweet_text) as hashtag,
                            get_mention_udf(tweet_text) as mention,
                            get_email_address_udf(tweet_text) as emails,
                            remove_blanks_udf(
                            remove_tabs_udf(
                            remove_all_punctuation_udf(
                            expand_contractions_udf(
                            to_lower_udf(
                            remove_email_address_udf(
                            ascii_to_string_udf(
                            remove_tabs_udf(
                            remove_accented_chars_udf(
                            remove_html_udf(
                            remove_url_udf(tweet_text)
                            )))))))))) as clean_tweet_text
                            from filter_data_pre_process
                        """) \
                        .where("clean_tweet_text is not null")

    pre_process_data.coalesce(12) \
                    .write.mode('overwrite') \
                    .option("compression", "snappy") \
                    .parquet("/mnt/e/Training_Data/Corona_twiter_data/corona_pre_process_out")
    # end timer
    end_time = time.perf_counter()
    print(f"Completed the code running in  {end_time - start_time:0.4f} seconds")

exit()