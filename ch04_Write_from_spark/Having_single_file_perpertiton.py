

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