# log in to GCP
gcloud auth login

# copy data from local to bucket
gsutil -m rsync -r E:\Training_Data\spark_test_data_new gs://dataproc-temp-us-central1-291860049184-ziygoifv/data/spark_test_data_new

# once login to master node

# copy the data from bucket to master's local
gsutil -m rsync -r gs://dataproc-temp-us-central1-291860049184-ziygoifv/data/spark_test_data_new ./

# move to hdfs
hdfs dfs -ls .
hdfs dfs -mkdir -p ./data/spark_test_data_new
hdfs dfs -put ./ ./data/spark_test_data_new/
hdfs dfs -ls ./data/spark_test_data_new/
hdfs dfs -du -h ./data/spark_test_data_new/

#create hive tables
CREATE DATABASE IF NOT EXISTS SALES LOCATION './data/spark_test_data_new';
USE SALES;
# DROP TABLE IF EXISTS CUSTOMER;
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER(
    `row_id` INT,
    `full_name` STRING,
    `name` STRING,
    `gender` STRING,
    `address` STRING,
    `email` STRING,
    `telephone` STRING,
    `city` STRING,
    `state` STRING,
    `birth_date` STRING,
    `cust_id` STRING,
    `user_create_date` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION './data/spark_test_data_new/ghosh/customer'
;

# DROP TABLE IF EXISTS ORDERS;
CREATE EXTERNAL TABLE IF NOT EXISTS ORDERS(
    `order_id` STRING,
    `cust_id` STRING,
    `order_date` STRING,
    `shipping_date` STRING,
    `delivery_date` STRING,
    `shipping_company` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION './data/spark_test_data_new/ghosh/order'
;

# DROP TABLE IF EXISTS ORDERS_PRODUCT;
CREATE EXTERNAL TABLE IF NOT EXISTS ORDERS_PRODUCT(
    `order_id` STRING,
    `uniq_id` STRING,
    `quantity` INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION './data/spark_test_data_new/ghosh/order_product'
;


# DROP TABLE IF EXISTS PRODUCT;
CREATE EXTERNAL TABLE IF NOT EXISTS PRODUCT(
    `uniq_id` STRING,
    `product_name` STRING,
    `manufacturer` STRING,
    `price` FLOAT,
    `description` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION './data/spark_test_data_new/ghosh/product'
tblproperties('skip.header.line.count'='1')
;
# verify data
SELECT * FROM PRODUCT LIMIT 2;
SELECT * FROM ORDERS_PRODUCT LIMIT 2;
SELECT * FROM ORDERS LIMIT 2;
SELECT * FROM CUSTOMER LIMIT 2;

# count check
SELECT COUNT(*) FROM PRODUCT;
SELECT COUNT(*) FROM ORDERS_PRODUCT;
SELECT COUNT(*) FROM ORDERS;
SELECT COUNT(*) FROM CUSTOMER;

# CREATE FINAL TABLE
# DROP TABLE IF EXISTS CUSTOMER_ORDERS_PRODUCT;
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_ORDERS_PRODUCT(
    `row_id` INT,
    `full_name` STRING,
    `name` STRING,
    `gender` STRING,
    `address` STRING,
    `email` STRING,
    `telephone` STRING,
    `city` STRING,
    `state` STRING,
    `birth_date` STRING,
    `cust_id` STRING,
    `user_create_date` STRING,
    `order_id` STRING,
    `shipping_date` STRING,
    `delivery_date` STRING,
    `shipping_company` STRING,
    `uniq_id` STRING,
    `quantity` INT,
    `product_name` STRING,
    `manufacturer` STRING,
    `price` FLOAT,
    `description` STRING,
    `total_sales` FLOAT
)
PARTITIONED BY (`order_date` DATE)
STORED AS ORC
LOCATION './data/spark_test_data_new/ghosh/customer_orders_product'
tblproperties('orc.compress'='SNAPPY')
;

# CREATE FINAL TABLE PARTITION BY MONTH & YEAR NOT DATE
# DROP TABLE IF EXISTS CUSTOMER_ORDERS_PRODUCT;
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_ORDERS_PRODUCT(
    `row_id` INT,
    `full_name` STRING,
    `name` STRING,
    `gender` STRING,
    `address` STRING,
    `email` STRING,
    `telephone` STRING,
    `city` STRING,
    `state` STRING,
    `birth_date` STRING,
    `cust_id` STRING,
    `user_create_date` STRING,
    `order_id` STRING,
    `order_date` STRING,
    `shipping_date` STRING,
    `delivery_date` STRING,
    `shipping_company` STRING,
    `uniq_id` STRING,
    `quantity` INT,
    `product_name` STRING,
    `manufacturer` STRING,
    `price` FLOAT,
    `description` STRING,
    `total_sales` FLOAT
)
PARTITIONED BY (`order_year` INT, `order_month` INT)
STORED AS ORC
LOCATION './data/spark_test_data_new/ghosh/customer_orders_product'
tblproperties('orc.compress'='SNAPPY');
;