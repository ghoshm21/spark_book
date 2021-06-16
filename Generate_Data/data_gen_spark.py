# https://mimesis.readthedocs.io/index.html
# https://faker.readthedocs.io/en/master/index.html
# https://towardsdatascience.com/how-to-create-fake-data-with-faker-a835e5b7a9d9
# https://stackoverflow.com/questions/45574191/using-python-faker-generate-different-data-for-5000-rows
# gender = (0 - not known, 1 - male, 2 - female, 9 - not applicable)
# this code is not using faker. I am using mimesis, which way faster than faker.
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
# import UDF function
from pyspark.sql.functions import udf, struct, col

from faker import Faker
from random import randrange
import datetime as dt
import pandas as pd
import random
import uuid
from datetime import date
from datetime import timedelta

from mimesis import Person
from mimesis import Address
from mimesis import Business
from mimesis.enums import Gender
from mimesis import Datetime
person = Person('en')
# person = Person()
addess = Address()
bus = Business()
datetime = Datetime()

file_dir = '/mnt/0bc681c8-67f8-454e-b120-7db5390aa04b/Data/spark_test_data_new'
cust_no = 1000000
cust_loop = 100
order_loop = 30
product_loop_outer = 5
product_loop_inner = 20

# function to add years
def addYears(d, years):
    try:
        #Return same day of the current year        
        return d.replace(year = d.year + years)
    except ValueError:
        #If not same day, it will return other, i.e.  February 29 to March 1 etc.        
        return d + (date(d.year + years, 1, 1) - date(d.year, 1, 1))

# function to add day
def addDay(d, day):
    end_date = d + timedelta(days=day)
    return end_date

# Use faker for data generation. This process is very slow
def fake_datagenerate(id):
    fake = Faker('en_US')
    fake1 = Faker('en_GB')   # To generate phone numbers
    full_name = fake.name()
    FLname = full_name.split(" ")
    Fname = FLname[0]
    Lname = FLname[1]
    domain_name = "@testDomain.com"
    email_id = Fname +"."+ Lname + domain_name
    # main data point
    user_id = id
    Prefix = fake.prefix()
    name = fake.name()
    Birth_Date = fake.date(pattern="%d-%m-%Y", end_datetime=datetime.date(2008, 1,1))
    Phone_Number = fake1.phone_number()
    Additional_Email_Id = fake.email()
    Address = fake.address()
    Zip_Code = fake.zipcode()
    City =  fake.city()
    State = fake.state()
    Country = fake.country()
    Year = fake.year()
    Time = fake.time()
    Link = fake.url()
    Text = fake.word()
    return user_id,Prefix,name,Birth_Date,Phone_Number,Additional_Email_Id,Address,Zip_Code,City,State,Country,Year,Time,Link,Text

# Using mimesis, which is extrimly fast
# generate the fake customers
def customer_generator(id):
    row_id = id
    full_name = person.full_name(gender=None)
    name = person.name()
    gender = person.gender(iso5218=True,symbol=False)
    address = addess.address()
    email = person.email()
    city = addess.city()
    state = addess.state()
    # birth_date = datetime.datetime(end=2005)
    birth_date = datetime.date(end=2005)
    user_create_date = addYears(birth_date, 15)
    telephone = person.telephone()
    cust_id = uuid.uuid1()
    return row_id,full_name,name, gender,address,email,telephone,city,state,str(birth_date),str(cust_id.int),str(user_create_date)

# print(mimesis_datagenerate(200))
# generate fake orders for the customers
def orders_generator(cust_id, user_create_date):
    # user_create_year = dt.datetime.strptime(user_create_date, '%Y-%m-%d %H:%M:%S.%f').year
    user_create_year = dt.datetime.strptime(user_create_date, '%Y-%m-%d').year
    current_year = dt.datetime.today().year
    # get the main data points
    order_id = uuid.uuid4()
    order_date = datetime.datetime(start=user_create_year,end=current_year)
    shipping_date = addDay(order_date, randrange(15))
    delivery_date = addDay(shipping_date, randrange(45))
    shipping_company = bus.company()
    return str(order_id),str(cust_id),str(order_date),str(shipping_date),str(delivery_date),shipping_company

# print(orders_generator(1,'2016-01-13 09:04:49.470733'))
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

if __name__ == '__main__':
    spark = SparkSession.builder.appName('generate_data').getOrCreate()
    sc = spark.sparkContext
    records = cust_no
    # generate customer
    for cl in range(cust_loop):
        customer_rdd = sc.range(records).map(lambda x: customer_generator(x))
        customer_df = customer_rdd.toDF(schema=customer_df_schema)
        # customer_df.cache()
        # write to customer data to csv
        customer_df.write.option("compression","gzip").save(path=file_dir+'/customer', format='csv', mode='append', sep='|')
        print("Customer data generated with loop %s: " %cl)
    # read the customer data from disk
    customer_df = spark.read.format('csv') \
                .option('delimiter', '|') \
                .option('header', 'true') \
                .schema(customer_df_schema) \
                .load(file_dir+'/customer')
    customer_df.cache()
    customer_df.count()
    # customer_df.createOrReplaceTempView("customer_df")
    # generate orders based on cust_id, user_create_date
    for i in range(order_loop):
        order_rdd = customer_df.select("cust_id","user_create_date").sample(True, fraction=0.1).rdd.map(lambda x: orders_generator(x[0],x[1]))
        order_df = order_rdd.toDF(schema=order_df_schema)
        order_df.write.option("compression","gzip").save(path=file_dir+'/order', format='csv', mode='append', sep='|')
        print("Order generated for %s" %i)

    customer_df.unpersist()
    # get clean product data (already done - in product dir)
    # from pyspark.sql.functions import *
    # ama1 = spark.read.format('csv').option('delimiter', ',').option('header', 'true').load(file_dir+'/amazon_co-ecommerce_sample.csv')
    # temp1 = ama1.select("uniq_id", "product_name", "manufacturer", "price", "amazon_category_and_sub_category").where("price like '£%'").withColumn('price', regexp_replace('price', '£', ''))
    # temp1.show(2)

    # wal1 = spark.read.format('csv').option('delimiter', ',').option('header', 'true').load('/mnt/0bc681c8-67f8-454e-b120-7db5390aa04b/Data/spark_test_data/walmart_prod_detail_30k_clean.csv')
    # temp2 = wal1.select("uniq_Id", "Product_Name", "Brand", "List_Price", "description")
    # temp2.show(2)

    # random = spark.read.format('csv').option('delimiter', ';').option('header', 'true').load('/mnt/0bc681c8-67f8-454e-b120-7db5390aa04b/Data/spark_test_data/amazon-final.csv')
    # temp3 = random.select("asin", "product_name", "seller_name", "sale_price", "brand_name")
    # temp3.show(2)

    # final = temp1.unionAll(temp2).unionAll(temp3)
    # final.coalesce(1).write.option("header", "true").save(path='/mnt/0bc681c8-67f8-454e-b120-7db5390aa04b/Data/spark_test_data/product', format='csv', mode='overwrite', sep='|')

    # generate order data
    from pyspark.sql import functions as psf
    from pyspark.sql import window as psw
    from pyspark.sql.functions import to_json,col

    # import json
    # import pandas as pd
    # def generate_order_product(order_id):
    #     product_select = product.select("uniq_id").sample(True, fraction=0.0002)
    #     count=product_select.count()
    #     # product_select_new = product_select.withColumn("order_id",lit(order_id))
    #     # w = psw.Window().partitionBy(psf.lit('a')).orderBy(psf.lit('a'))
    #     # product_select_new = product_select_new.withColumn("quantity", psf.row_number().over(w)).collect()
    #     # # return product_select_new
    #     # result = product_select_new.toJSON().map(lambda j: json.loads(j)).collect()
    #     return count,order_id,"result"

    # print(generate_order_product('dcb87ff3-5b9a-4094-bf1f-abdab6e67129'))

    # df = orders.select("order_id").sample(True, fraction=0.4).rdd.map(lambda x: generate_order_product(str(x)))
    # df.take(3)

    # create unique quantity
    def quantity():
        return randrange(1,15)

    quantity_udf = udf(lambda row: quantity(), IntegerType())
    spark.udf.register("quantity_udf", quantity, IntegerType())
    # off auto broadcast
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    # read order and product data
    orders = spark.read.format('csv').option('delimiter', '|').option('header', 'false').schema(order_df_schema).load(file_dir+'/order/*.gz')
    product = spark.read.format('csv').option('delimiter', '|').option('header', 'true').load(file_dir+'/product/*.csv').cache()
    for i in range(product_loop_outer):
        print("running for outter %s" %i)
        ord_5 = orders.select("order_id").sample(True, fraction=0.5)
        ord_5.cache()
        for x in range(product_loop_inner):
            print("running for inner %s" %x)
            ord_sel = ord_5.select("order_id").sample(False, fraction=0.002)
            prod_sel = product.select("uniq_id").sample(True, fraction=0.0002)
            merge = ord_sel.crossJoin(prod_sel)
            merge.createOrReplaceTempView("merge")
            final = spark.sql("select order_id, uniq_id as product_id, quantity_udf() as quantity from merge")
            final.repartition(1).write.option("header", "true").save(path=file_dir+'/order_product', format='csv', mode='append', sep='|')
            print("Product generated for %s, %s" %(i,x))
        ord_5.unpersist()
    print("All Done")