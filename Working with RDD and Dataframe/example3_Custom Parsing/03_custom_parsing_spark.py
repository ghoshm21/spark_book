# cd '/mnt/c/Users/ghosh/Google Drive/personal_project/Writing_Personal/Spark_Book/Working with RDD and Dataframe/example3_Custom Parsing'
# $SPARK_HOME/bin/spark-submit --master spark://LAPTOP-7DUT93OF.localdomain:7077 --executor-memory 6700mb ./03_custom_parsing_spark.py
import re
import json
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# from pyspark.sql.functions import from_json, col
# define the dataframe schema
schema = "`type` STRING, \
          `books_ol_number` STRING, \
          `version` STRING, \
          `upload_date` STRING, \
          `book_details` STRING, \
          `is_valid_json` STRING"

# check if the string is valid json
def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError as e:
    return False
  return True

# python function to split the data
def perse_data(line):
  y = 'NULL'
  try:
    x = re.split('\t+', line, maxsplit=4, flags=0)
    if len(x) > 4:
      y = is_json(x[4])
      return x[0].strip(), x[1].strip(), x[2].strip(), x[3].strip(), x[4].strip(), y
    else:
      return '','', '', '', '', y
  except TypeError:
    return '','', '', '', '', y

# create spark session
spark = SparkSession.builder.appName('03_custom_parsing_spark').getOrCreate()
sc = spark.sparkContext
# load the data
raw_data = sc.textFile("/mnt/e/Training_Data/100gb_data/ol_cdump_latest.txt")
split_data = raw_data.map(perse_data)
split_data_df = split_data.toDF(schema) \
                .withColumn("upload_datetime",to_timestamp("upload_date")) \
                .drop("upload_date") \
                .withColumn("version", col("version").cast(IntegerType()))

# create a spark view for runing spark sql
# split_data_df.createOrReplaceTempView("split_data_df")
split_data_df_filter = split_data_df.filter("is_valid_json == 'true'")
# read the json schema -- this will read 10% of the entire data
json_schema = spark.read.option("samplingRation", 0.1).json(split_data_df_filter.rdd.map(lambda row: row.book_details)).schema
schema = json_schema
# in json_schema we have a column like "     _date", which is not permitted in parquet format. This is column name is ok in ORC.
# We will replace this column name as field_date.
schema.fields = (list(map(lambda field: StructField('field_date', field.dataType) if field.name == '     _date' else StructField(field.name, field.dataType)  , schema.fields )))
# remove all spaces
schema.fields = (list(map(lambda field: StructField("".join(field.name.split()), field.dataType)  , schema.fields )))
# remove all duplicate column names

# apply the schema
book_details = split_data_df_filter.withColumn('book_details_parse', from_json(col('book_details'), schema))
# drop extra column
book_details = book_details.drop("book_details")

# write the data
spark.conf.set("spark.hadoop.mapred.output.committer.class","com.appsflyer.spark.DirectOutputCommitter")
# save the data
# I am using selectExpr to use AS clause to rename the column name
book_details.selectExpr("type", \
"books_ol_number", \
"version", \
"upload_datetime", \
"book_details_parse.birth_date", \
"book_details_parse.key", \
"book_details_parse.last_modified", \
"book_details_parse.name", \
"book_details_parse.personal_name", \
"book_details_parse.revision", \
"book_details_parse.edition_name", \
"book_details_parse.by_statement", \
"book_details_parse.latest_revision", \
"book_details_parse.lc_classifications", \
"book_details_parse.lccn", \
"book_details_parse.notes", \
"book_details_parse.pagination", \
"book_details_parse.series", \
"book_details_parse.subject_place", \
"book_details_parse.subject_time", \
"book_details_parse.subjects", \
"book_details_parse.subtitle", \
"book_details_parse.telephone", \
"book_details_parse.title_prefix", \
"book_details_parse.translated_from", \
"book_details_parse.translated_titles", \
"book_details_parse.translation_of", \
"book_details_parse.type as book_details_parse_type", \
"book_details_parse.type_key", \
"book_details_parse.work", \
"book_details_parse.author_names", \
"book_details_parse.authors", \
"book_details_parse.string_author", \
"book_details_parse.string_authors", \
"book_details_parse.title", \
"book_details_parse.full_title", \
"book_details_parse.bio", \
"book_details_parse.contact_email", \
"book_details_parse.contact_person", \
"book_details_parse.contact_title", \
"book_details_parse.date", \
"book_details_parse.death_date", \
"book_details_parse.description", \
"book_details_parse.download_url", \
"book_details_parse.edition", \
"book_details_parse.first_publish_date", \
"book_details_parse.genres", \
"book_details_parse.id_wikidata", \
"book_details_parse.intro", \
"book_details_parse.isbn", \
"book_details_parse.isbn_10", \
"book_details_parse.isbn_13", \
"book_details_parse.isbn_invalid", \
"book_details_parse.isbn_odd_length", \
"book_details_parse.lang", \
"book_details_parse.language", \
"book_details_parse.language_code", \
"book_details_parse.languages", \
"book_details_parse.number_of_editions", \
"book_details_parse.number_of_pages", \
"book_details_parse.numer_of_pages", \
"book_details_parse.openlibrary", \
"book_details_parse.original_isbn", \
"book_details_parse.original_language", \
"book_details_parse.original_languages", \
"book_details_parse.price", \
"book_details_parse.publish_country", \
"book_details_parse.publish_date", \
"book_details_parse.publish_places", \
"book_details_parse.publisher", \
"book_details_parse.publishers", \
"book_details_parse.purchase_url", \
"book_details_parse.website", \
"book_details_parse.website_name", \
"book_details_parse.weight", \
"book_details_parse.writers") \
.coalesce(80) \
.write.mode('overwrite') \
.option("compression", "snappy") \
.parquet("/mnt/e/Training_Data/100gb_data/ol_cdump_parquet")