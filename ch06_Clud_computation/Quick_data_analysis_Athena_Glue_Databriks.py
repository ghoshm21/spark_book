# Databricks notebook source

import urllib
# unmount if already mounted
dbutils.fs.unmount("/mnt/corona_s3")
# all AWS keys after creating the IAM role
access_key = "xxxxxx"
secret_key = "xxxxxx"
# replace the '/'
encoded_secret_key = urllib.parse.quote(secret_key).replace("/", "%2F")
aws_bucket_name = "sandipan-atena"
local_mount_name = "corona_s3"
dbutils.fs.mount("s3a://%s:%s@%s" %(access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" %local_mount_name)

# get the file details
display(dbutils.fs.ls("/mnt/corona_s3/corona/"))

# mount the location to local databriks
data_loc = "/mnt/corona_s3/corona/*.json"
# read the data
data = spark.read.json(data_loc)

data.createOrReplaceTempView("data")
data.printSchema()
# Query to get the required columns
extract_data = spark.sql("""select user.screen_name as user_screen_name,
                         user.location as user_location,
                         extended_tweet.full_text  extended_tweet_full_text,
                         retweeted_status.user.screen_name  retweeted_status_user_screen_name,
                         retweeted_status.text  retweeted_status_text,
                         retweeted_status.extended_tweet.full_text  retweeted_status_extended_tweet_full_text,
                         quoted_status.user.screen_name  quoted_status_user_screen_name,
                         quoted_status.text  quoted_status_text,
                         quoted_status.extended_tweet.full_text  quoted_status_extended_tweet_full_text,
                         place.country  place_country,
                         place.country_code  place_country_code,
                         timestamp_ms
              from data""")

# Display some data
extract_data.show(10,False)

# save the output
# extract_data.coalesce(2).write.mode("Overwrite").parquet("/mnt/corona_s3/corona_extract/")
extract_data.write.mode("Overwrite").parquet("/mnt/corona_s3/corona_extract/")



