from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# https://ghoshm21.medium.com/spark-write-single-file-per-hive-partitions-3d0ee1559a80
'''
In this code I showed how to work with skewed data.
1. How to split the data and conqure
2. Other 

'''

spark = SparkSession.builder.appName('optimizing_skew_data').getOrCreate()

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
# orders = spark.read.format('orc').schema(order_df_schema).load(file_dir+'/orders_skw_orc/*.orc')
orders = spark.read.format('orc').schema(order_df_schema).load(file_dir+'/orders_orc/*.orc')
product = spark.read.format('csv').option('delimiter', '|').option('header', 'true').load(file_dir+'/product/*.csv').cache()
customer = spark.read.format('orc').schema(customer_df_schema).load(file_dir+'/customer_orc/*.orc')
orders_product = spark.read.format('orc').schema(order_product_df_schema).load(file_dir+'/orders_product_orc/*.orc')

# register the temp tables
product.createOrReplaceTempView("product")
orders_product.createOrReplaceTempView("orders_product")
orders.createOrReplaceTempView("orders")
customer.createOrReplaceTempView("customer")

# merge data is created with the bellow SQL.
# Lets check all the joining column for the skew ness
spark.sql("select cust_id, count(1) as rec_count from customer group by cust_id order by count(1) desc").show(5,False)
+---------------------------------------+---------+
|cust_id                                |rec_count|
+---------------------------------------+---------+
|331990751321662984916119431657624421042|1        |
|331990761542095949256218981224794014386|1        |
|331990767563436300340308638334134239922|1        |
|331990772475582376224697569133859160754|1        |
|331990800205439256217215726874241778354|1        |
+---------------------------------------+---------+
spark.sql("select cust_id, count(1) as rec_count from orders group by cust_id order by count(1) desc").show(5,False)
+---------------------------------------+---------+
|cust_id                                |rec_count|
+---------------------------------------+---------+
|46140677648871197582688745161315566258 |23       |
|254293191103066977821570918218611278514|23       |
|256346305982744030324735983239333921458|23       |
|32418570489867457558547660609317749426 |23       |
|301886283562739742416256315686516933298|22       |
+---------------------------------------+---------+

spark.sql("select order_id, count(1) as rec_count from orders group by order_id order by count(1) desc").show(5,False)
+------------------------------------+---------+
|order_id                            |rec_count|
+------------------------------------+---------+
|bb705e73-4a6f-4eb2-9557-36c6bbf7fbe2|1        |
|268fe79f-0590-49a2-974e-422ed7658a46|1        |
|3606d454-1ac4-4d4a-92c8-5c391a9f7154|1        |
|57aad1cf-1cd6-4e48-b0ee-67c8c8545f2e|1        |
|c95f19f6-f59f-48e0-9cac-fbdb825b9c9f|1        |
+------------------------------------+---------+
spark.sql("select order_id, count(1) as rec_count from orders_product group by order_id order by count(1) desc").show(5,False)
+------------------------------------+---------+
|order_id                            |rec_count|
+------------------------------------+---------+
|c593c882-c86e-401c-8b56-2824b8b3f3a7|59       |
|0a240dcd-ebcb-4d6e-8bcf-1b24bcb98173|59       |
|d70a0895-5905-4c39-b005-f7ff54bd76d7|56       |
|3eefa79b-4610-4c49-8206-48e013008a95|55       |
+------------------------------------+---------+
spark.sql("select uniq_id, count(1) as rec_count from orders_product group by uniq_id order by count(1) desc").show(5,False)
+--------------------------------+---------+
|uniq_id                         |rec_count|
+--------------------------------+---------+
|2c6afb11c00c54e4086a9d75151520d4|859576   |
|579df77e6c87233a89e8521222b62bb8|859459   |
|eb70b58151c4228a1ced6f7bc262e349|858750   |
|cf5fe2e5a1fba22eb9929f9649747cfc|858677   |
|f1649d4d1296a83f682c59c8ec43f6fb|858586   |
+--------------------------------+---------+
spark.sql("select uniq_id, count(1) as rec_count from product group by uniq_id order by count(1) desc").show(5,False)
+--------------------------------+---------+
|uniq_id                         |rec_count|
+--------------------------------+---------+
|a904e69ec08ecbfde95106cc092c4983|1        |
|59e4236135f06417c21596277224eb76|1        |
|1f8fa428ae3e5fb62b1c0f4ff82312ca|1        |
|376e20ac60676238d9cb762c5aedae79|1        |
|a1560a2fd3cee20e0203ca785a80a15f|1        |
+--------------------------------+---------+
spark.sql("""select cast(orders.order_date as date) as order_date, count(1) as rec_count
from orders group by cast(orders.order_date as date) order by count(1) desc""").show(5,False)
+----------+---------+
|order_date|rec_count|
+----------+---------+
|2020-01-01|197356494|
|2021-02-11|377834   |
|2021-11-11|367344   |
|2021-02-16|364373   |
|2021-02-24|358530   |
+----------+---------+

# -----------------------------------------------------------------------------------------
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
# ----------------------- split the data approach ----------------------------------------------------------
# To solve the slow skew data problem we can split the insert part in 2 parts.
# 1st part -> insert only non skew data, in this case non 2020-01-01 data
# 2nd part -> insert only skew data, in this case 2020-01-01 data 
# I am writing the merge data to disk first, so spark dont have to run this sql 2 times.
# # If you have big cluster you can cache the data in memory. Keep in mind that this will take a lot of memory and can have 
# negative impact on cluster performace.
# merge_data.hint("skew", "order_date") -- this does not work with reguler spark
# merge_data_df.createOrReplaceTempView("merge_data_df")
spark.conf.set("spark.sql.files.maxPartitionBytes", 100000000)
spark.conf.set("spark.sql.shuffle.partitions", 1200)
# write the merge data to disk
merge_data_df.write.option("orc.compress", "snappy").save(path=file_dir+'/merge_data', format='orc', mode='overwrite')
# split 1 - non skewed data
# read the merge_data and filter for non skwed data and skwed data
merge_data_df = spark.read.orc(file_dir+'/merge_data/*.orc').filter("order_date <> '2020-01-01'")
# set dynamic partitions ON
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
merge_data_df_parti = merge_data_df.repartition("order_date")
spark.conf.set("spark.sql.files.maxPartitionBytes", 100000000)
spark.conf.set("spark.sql.shuffle.partitions", 800)
# write the non skewed data part by partitioning the data on partition column
merge_data_df_parti.write.partitionBy("order_date").option("orc.compress", "snappy").save(path=file_dir+'/custmoer_orders_product_3', format='orc', mode='overwrite')
# split 2 - write the skew data. No need for repartition as the data is only for one order date
merge_data_skw = spark.read.orc(file_dir+'/merge_data/*.orc').filter("order_date = '2020-01-01'")
# cache it and the run count to force spark to run the prcess in parallel using shuffle.partitions number of tasks
# Else this will only use the number of coalesce task, in this case only 1
merge_data_skw.cache()
merge_data_skw.count()
merge_data_skw.coalesce(20).write.partitionBy("order_date").option("orc.compress", "snappy").save(path=file_dir+'/custmoer_orders_product_3', format='orc', mode='append')