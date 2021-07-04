from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# https://ghoshm21.medium.com/spark-write-single-file-per-hive-partitions-3d0ee1559a80

spark.sql("use sales")
# read the data from hive
product = spark.read.table("product")
orders_product = spark.read.table("orders_product")
orders = spark.read.table("orders")
customer = spark.read.table("customer")

# define all the schema for spark dataframe
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

file_dir = '/home/sandipan/Training_Data/spark_test_data_new'
# read from files
orders = spark.read.format('orc').schema(order_df_schema).load(file_dir+'/orders_orc/*.orc')
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

spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "2560MB")

merge_data = spark.sql("""select
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
# merge_data.hint("skew", "order_date") -- this does not work with reguler spark
merge_data.createOrReplaceTempView("merge_data")
spark.conf.set("spark.sql.files.maxPartitionBytes", 100000000)
spark.conf.set("spark.sql.shuffle.partitions", 1200)
merge_data.write.option("orc.compress", "snappy").save(path=file_dir+'/merge_data', format='orc', mode='overwrite')

# read the merge_data and filter for non skwed data and skwed data
merge_data_df = spark.read.orc(file_dir+'/merge_data/*.orc').filter("order_date <> '2020-01-01'")
# set dynamic partitions
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
merge_data_df_parti = merge_data_df.repartition("order_date")
spark.conf.set("spark.sql.files.maxPartitionBytes", 100000000)
spark.conf.set("spark.sql.shuffle.partitions", 800)
merge_data_df_parti.write.partitionBy("order_date").option("orc.compress", "snappy").save(path=file_dir+'/custmoer_orders_product', format='orc', mode='overwrite')

merge_data_20 = spark.read.orc(file_dir+'/merge_data/*.orc').filter("order_date = '2020-01-01'")
merge_data_20.coalesce(1).write.partitionBy("order_date").option("orc.compress", "snappy").save(path=file_dir+'/custmoer_orders_product', format='orc', mode='overwrite')
# ---------------------------------------------------------------------------------
# insert into final table with out repartitons
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
from merge_data""")

# ---------------------------------------------------------------------------------
# set the saprk repartition by partition column
merge_data_parti = merge_data.repartition("order_date")
merge_data_parti.createOrReplaceTempView("merge_data_parti")
spark.conf.set("spark.sql.files.maxPartitionBytes", 100000000)
spark.conf.set("spark.sql.shuffle.partitions", 1200)
# if you want to save the data into a dir (S3 bucket, google bucket etc..)
# spark.conf.set("SPARK_LOCAL_DIRS", "/mnt/0bc681c8-67f8-454e-b120-7db5390aa04b/Data/spark_test_data_new/spark_temp");
merge_data_parti.write.partitionBy("order_date").option("orc.compress", "snappy").save(path=file_dir+'/custmoer_orders_product_adpt_2', format='orc', mode='append')

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
# ---------------------------------------------------------------------------------
merge_data = spark.sql("""select
cust.row_id ,
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
cast(ord.order_date as date) as order_date,
year(cast(ord.order_date as date)) as order_year,
month(cast(ord.order_date as date)) as order_month
FROM customer as cust
inner join orders as ord
    on cust.cust_id = ord.cust_id
inner join orders_product as ordp
    on ordp.order_id = ord.order_id
inner join product as prod
    on prod.uniq_id = ordp.uniq_id
""")
merge_data.createOrReplaceTempView("merge_data")
# set the saprk repartition
merge_data_parti = merge_data.repartition("order_year", "order_month")
merge_data_parti.createOrReplaceTempView("merge_data_parti")
# insert into final table
spark.sql("""insert overwrite table CUSTOMER_ORDERS_PRODUCT partition(order_year, order_month)
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
    order_date,
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
    order_year,
    order_month
from merge_data_parti""")

select count(distinct order_date) from ORDERS;
2557
min size 2 MB and max 20 MB
15GB/2557 = 5MB
avg dir size 5 MB
if we keep month wise
5*30 = 150 MB, this looks like a great option. would be quick get a day of data not much performace hit.

select count(*) from CUSTOMER_ORDERS_PRODUCT where order_date in ('2017-03-13', '2020-06-23', '2021-06-30');
-- 8 sec
-- 19 sec
select count(*) from CUSTOMER_ORDERS_PRODUCT where order_date in ('2015-01-23', '2018-05-22', '2020-06-30');
-- 9 sec
-- 20 sec
select count(*) from CUSTOMER_ORDERS_PRODUCT where order_date in ('2005-01-23', '2015-05-22', '2018-06-30');
-- 11 sec
-- new 22 sec
select count(*) from CUSTOMER_ORDERS_PRODUCT where order_date in ('2005-01-23', '2015-05-22', '2018-06-30') and order_year in (2005, 2015, 2018) and order_month in (5,6,1);
Time taken: 14.753 seconds, Fetched: 1 row(s

select count(*) from CUSTOMER_ORDERS_PRODUCT where order_date in ('2005-01-23', '2015-05-22', '2018-06-30') and order_year in (2005, 2015, 2018);

# ---------------------------------------------------------------------------------

