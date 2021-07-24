from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# https://ghoshm21.medium.com/spark-write-single-file-per-hive-partitions-3d0ee1559a80
'''
Use this code to generate single file par hive(HDFS) partiton

'''

spark = SparkSession.builder.appName('create_single_file_perpertion').getOrCreate()

file_dir = '/mnt/tv/spark_test_data_new'

'''
# set the hive DB
spark.sql("use sales")
# read the data from hive
product = spark.read.table("product")
orders_product = spark.read.table("orders_product")
orders = spark.read.table("orders")
customer = spark.read.table("customer")
'''
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
# orders = spark.read.format('orc').schema(order_df_schema).load(file_dir+'/orders_orc/*.orc')
orders = spark.read.format('orc').schema(order_df_schema).load(file_dir+'/orders_skw_orc/*.orc')

product = spark.read.format('csv').option('delimiter', '|').option('header', 'true').load(file_dir+'/product/*.csv').cache()
customer = spark.read.format('orc').schema(customer_df_schema).load(file_dir+'/customer_orc/*.orc')
orders_product = spark.read.format('orc').schema(order_product_df_schema).load(file_dir+'/orders_product_orc/*.orc')

# count check
product.count()
orders_product.count()
orders.count()
customer.count()

# register the temp tables
product.createOrReplaceTempView("product")
orders_product.createOrReplaceTempView("orders_product")
orders.createOrReplaceTempView("orders")
customer.createOrReplaceTempView("customer")

# orders.write.option("orc.compress", "snappy").save(path=file_dir+'/orders_orc', format='orc', mode='overwrite')
# orders_product.write.option("orc.compress", "snappy").save(path=file_dir+'/orders_product_orc', format='orc', mode='overwrite')
# customer.write.option("orc.compress", "snappy").save(path=file_dir+'/customer_orc', format='orc', mode='overwrite')

# enable skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "2560MB")

# make the df after joining all tables
merge_data_df = spark.sql("""select
cust.full_name ,
cust.name ,
cust.gender ,
cust.address ,
cust.email ,
cust.telephone ,
cust.city ,
cust.state ,
cust.birth_date ,
cust.cust_id ,
cust.user_create_date ,
ord.order_id ,
ord.shipping_date ,
ord.delivery_date ,
ord.shipping_company ,
ordp.uniq_id ,
ordp.quantity ,
prod.product_name ,
prod.manufacturer ,
prod.price ,
prod.description ,
(prod.price * ordp.quantity) as total_sales ,
cast(ord.order_date as date) as order_date
FROM customer as cust
left outer join orders as ord
    on cust.cust_id = ord.cust_id
left outer join orders_product as ordp
    on ordp.order_id = ord.order_id
left outer join product as prod
    on prod.uniq_id = ordp.uniq_id
""")
# ----------------------- approach 1 ----------------------------------------------------------
# insert into merge data to hive final table with out repartitons
# this iwill create a lot of small files(=number of shuffle partitions) under every partition
'''spark.sql("""insert overwrite table CUSTOMER_ORDERS_PRODUCT partition(order_date)
select 
    row_id,
    full_name,
    name,
    gender,
    address,
    email,
    telephone,
    city,
    state,
    birth_date,
    cust_id,
    user_create_date,
    order_id,
    shipping_date,
    delivery_date,
    shipping_company,
    uniq_id,
    quantity,
    product_name,
    manufacturer,
    price,
    description,
    total_sales,
    order_date    
from merge_data_df""")'''
# -----------------------------------------------------------------------------------------
# ----------------------- approach 2 ----------------------------------------------------------
# to create single file par partition we need to use repartition
# if we have skewed data then this will be very slow and might fail
# I am uusing the file dir for writing the data, writing to hive table can be same just like TEST 1
# set dynamic partitions
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
# set shuffle partitions properties
spark.conf.set("spark.sql.files.maxPartitionBytes", 100000000)
spark.conf.set("spark.sql.shuffle.partitions", 800)
# repartition the data by the partition column
merge_data_df_parti = merge_data_df.repartition("order_date")
merge_data_df_parti.write.partitionBy("order_date").option("orc.compress", "snappy").save(path=file_dir+'/custmoer_orders_product_2', format='orc', mode='overwrite')
# -----------------------------------------------------------------------------------------
# ----------------------- approach 3 ----------------------------------------------------------
# To solve the slow skew data problem we can split the insert part in 2 parts.
# 1st part -> insert only non skew data, in this case non 2020-01-01 data
# 2nd part -> insert only skew data, in this case 2020-01-01 data 
# I am writing the merge data to disk first, so I dont have to run this sql 2 times.
# # If you have big cluster can cache the data in memory, you can cache the data insted of writing it
# merge_data.hint("skew", "order_date") -- this does not work with reguler spark
merge_data_df.createOrReplaceTempView("merge_data_df")
spark.conf.set("spark.sql.files.maxPartitionBytes", 100000000)
spark.conf.set("spark.sql.shuffle.partitions", 1200)
# write the merge data to disk
merge_data_df.write.option("orc.compress", "snappy").save(path=file_dir+'/merge_data', format='orc', mode='overwrite')
# read the merge_data and filter for non skwed data and skwed data
merge_data_df = spark.read.orc(file_dir+'/merge_data/*.orc').filter("order_date <> '2020-01-01'")
# set dynamic partitions
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
merge_data_df_parti = merge_data_df.repartition("order_date")
spark.conf.set("spark.sql.files.maxPartitionBytes", 100000000)
spark.conf.set("spark.sql.shuffle.partitions", 800)
# write the non skewed data part by partitioning the data on partition column
merge_data_df_parti.write.partitionBy("order_date").option("orc.compress", "snappy").save(path=file_dir+'/custmoer_orders_product_3', format='orc', mode='overwrite')
# write the skew data. No need for repartition as the data is only for one order date
merge_data_skw = spark.read.orc(file_dir+'/merge_data/*.orc').filter("order_date = '2020-01-01'")
# cache it and the run count to force spark to run the prcess in parallel using shuffle.partitions number tasks
# Else this will only use the number of coalesce task, in this case only 1
merge_data_skw.cache()
merge_data_skw.count()
merge_data_skw.coalesce(1).write.partitionBy("order_date").option("orc.compress", "snappy").save(path=file_dir+'/custmoer_orders_product_3', format='orc', mode='append')
# ---------------------------------------------------------------------------------
'''
spark.sql("""insert overwrite table CUSTOMER_ORDERS_PRODUCT partition(order_date)
select 
    row_id,
    full_name,
    name,
    gender,
    address,
    email,
    telephone,
    city,
    state,
    birth_date,
    cust_id,
    user_create_date,
    order_id,
    shipping_date,
    delivery_date,
    shipping_company,
    uniq_id,
    quantity,
    product_name,
    manufacturer,
    price,
    description,
    total_sales,
    order_date    
from merge_data_parti""")
'''

# select count(distinct order_date) from ORDERS;
# 2557
# min size 2 MB and max 20 MB
# 15GB/2557 = 5MB
# avg dir size 5 MB
# if we keep month wise
# 5*30 = 150 MB, this looks like a great option. would be quick get a day of data not much performace hit.

# select count(*) from CUSTOMER_ORDERS_PRODUCT where order_date in ('2017-03-13', '2020-06-23', '2021-06-30');
# -- 8 sec
# -- 19 sec
# select count(*) from CUSTOMER_ORDERS_PRODUCT where order_date in ('2015-01-23', '2018-05-22', '2020-06-30');
# -- 9 sec
# -- 20 sec
# select count(*) from CUSTOMER_ORDERS_PRODUCT where order_date in ('2005-01-23', '2015-05-22', '2018-06-30');
# -- 11 sec
# -- new 22 sec
# select count(*) from CUSTOMER_ORDERS_PRODUCT where order_date in ('2005-01-23', '2015-05-22', '2018-06-30') and order_year in (2005, 2015, 2018) and order_month in (5,6,1);
# Time taken: 14.753 seconds, Fetched: 1 row(s

# select count(*) from CUSTOMER_ORDERS_PRODUCT where order_date in ('2005-01-23', '2015-05-22', '2018-06-30') and order_year in (2005, 2015, 2018);

# ---------------------------------------------------------------------------------

