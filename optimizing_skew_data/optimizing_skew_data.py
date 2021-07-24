from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# https://ghoshm21.medium.com/spark-write-single-file-per-hive-partitions-3d0ee1559a80
'''
Use this code to follow step by step - optimizing the skew data

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
+-------------------------------------+---------+
|cust_id                              |rec_count|
+-------------------------------------+---------+
|6379354496207788799770000334416757426|1        |
|6379373035597817137624997223701136050|1        |
|6379391812672333018273006893617365682|1        |
|6379397517100034045305313628781789874|1        |
|6379399814716746958971103841556349618|1        |
+-------------------------------------+---------+
spark.sql("select cust_id, count(1) as rec_count from orders group by cust_id order by count(1) desc").show(5,False)
+---------------------------------------+---------+
|cust_id                                |rec_count|
+---------------------------------------+---------+
|221175601294661197258047859365864784562|18       |
|42659854088193970008207597703680737970 |17       |
|132461578677588917106957668115681624754|17       |
|235992096807546922312654577286071435954|16       |
|129602292762831843764452715230596940466|16       |
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