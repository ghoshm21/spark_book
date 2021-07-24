from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# https://ghoshm21.medium.com/spark-write-single-file-per-hive-partitions-3d0ee1559a80
'''
Use this code to generate ORC files

'''

spark = SparkSession.builder.appName('convert_to_orc').getOrCreate()

file_dir = '/mnt/tv/spark_test_data_new'

# define all the schema for spark dataframe if you are reading the data from a file
# customer_df schema
customer_df_schema = "`row_id` INT, \
        `full_name` STRING, \
        `name` STRING, \
        `gender` STRING, \
        `address` STRING, \
        `email` STRING, \
        `telephone` STRING, \
        `city` STRING, \
        `state` STRING, \
        `birth_date` STRING, \
        `cust_id` STRING, \
        `user_create_date` STRING"

order_df_schema = "`order_id` STRING, \
        `cust_id` STRING, \
        `order_date` STRING, \
        `shipping_date` STRING, \
        `delivery_date` STRING, \
        `shipping_company` STRING"

order_product_df_schema = "`order_id` STRING, \
        `uniq_id` STRING, \
        `quantity` INT"

# read from files
# product = spark.read.format('csv').option('delimiter', '|').option('header', 'true').load(file_dir+'/product/*.csv')
orders = spark.read.format('csv').option('delimiter', '|').option('header', 'false').schema(order_df_schema).load(file_dir+'/order/*.gz')
customer = spark.read.format('csv').option('delimiter', '|').option('header', 'false').schema(customer_df_schema).load(file_dir+'/customer/*.gz')
orders_product = spark.read.format('csv').option('delimiter', '|').option('header', 'false').schema(order_product_df_schema).load(file_dir+'/order_product/*.gz')

order_skw_cust = spark.read.format('csv').option('delimiter', '|').option('header', 'false').schema(order_df_schema).load(file_dir+'/order_skew_cust/*.gz')

# set the ORC filesize and SQL shufflepartitons
spark.conf.set("spark.sql.files.maxPartitionBytes", 100000000)

orders.write.option("orc.compress", "snappy").save(path=file_dir+'/orders_orc', format='orc', mode='overwrite')
orders_product.write.option("orc.compress", "snappy").save(path=file_dir+'/orders_product_orc', format='orc', mode='overwrite')
customer.write.option("orc.compress", "snappy").save(path=file_dir+'/customer_orc', format='orc', mode='overwrite')
# skew data for orders for 4 customers only
order_skw_cust.write.option("orc.compress", "snappy").save(path=file_dir+'/orders_skw_orc', format='orc', mode='append')