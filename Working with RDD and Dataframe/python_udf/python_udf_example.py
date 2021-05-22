# cd '/mnt/c/Users/ghosh/Google Drive/personal_project/Writing_Personal/Spark_Book/Working with RDD and Dataframe/python_udf'
# $SPARK_HOME/bin/spark-submit --master spark://LAPTOP-7DUT93OF.localdomain:7077 --executor-memory 6700mb ./python_udf_example.py
'''
Documentation, License etc.
list of contractions:- https://en.wikipedia.org/wiki/Wikipedia:List_of_English_contractions
@package process_twitter_data
'''
from pyspark.sql import SparkSession
import re
import json
from bs4 import BeautifulSoup
import unidecode
from pyspark.sql.functions import *
from pyspark.sql.types import *
# import UDF function
from pyspark.sql.functions import udf, struct, col

# create spark session
spark = SparkSession.builder.appName('python udf example').getOrCreate()
sc = spark.sparkContext

# define UDF
# remove html tags from text
# Beautiful Soup ranks lxml’s parser as being the best, then html5lib’s, then Python’s built-in parser.
def remove_html(input):
# doing NULL check and proper datatype
    text = 'NULL' if input is None else str(input)
    # you can use html.parser like following code
    # soup = BeautifulSoup(text, "html.parser")
    soup = BeautifulSoup(text, "lxml")
    stripped_text = soup.get_text(separator=" ", strip=True)
    return stripped_text

 # conver the UDF and register in spark 
remove_html_udf = udf(lambda row: remove_html(row), StringType())
spark.udf.register("remove_html_udf", remove_html, StringType())

#remove accented characters from text, e.g. café
def remove_accented_chars(text):
    text = 'NULL' if text is None else unidecode.unidecode(text)
    return text

# register the UDF
remove_accented_chars_udf = udf(lambda row: remove_accented_chars(row), StringType())
spark.udf.register("remove_accented_chars_udf", remove_accented_chars, StringType())


# load the data
data = spark.read.option("samplingRation", 0.5).json("/mnt/e/Training_Data/Corona_twiter_data/india_corona_2_5_21.json")
data = data.withColumn("is_it_a_retweet",when(substring('text',1,2) == 'RT', 'Y').otherwise('N'))
# create the view for spark sql
data.createOrReplaceTempView("data")
# get only the required columns, to reduce the data size
filter_data = spark.sql("""select 
                            distinct id, lang, created_at, source,
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
filter_data.createOrReplaceTempView("filter_data")

# apply the UDF in dataframe
pre_process_data_df = filter_data.withColumn("clean_tweet_text", remove_accented_chars_udf(remove_html_udf("tweet_text")))

# apply the UDF in spark SQL way
pre_process_data_sql = spark.sql("""select
                            id, lang, created_at, source,
                            user_id_str, user_name, user_location, user_description, tweet_text,
                            remove_accented_chars_udf(remove_html_udf(tweet_text)) as clean_tweet_text
                            from filter_data""") \
                        .where("clean_tweet_text is not null")

pre_process_data_sql.coalesce(80) \
                .write.mode('overwrite') \
                .option("compression", "snappy") \
                .parquet("/mnt/e/Training_Data/Corona_twiter_data/corona_pre_process_out")