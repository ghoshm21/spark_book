Code to generate massive structure data for spark optimization test.
Count of data points
Customer = 143000000
Order = 429010505
Product = 38718
order_product = 473966231

SCHEMA
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
		
