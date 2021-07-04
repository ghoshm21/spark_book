from __future__ import print_function
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('python udf example') \
                        .getOrCreate()


# Example 1
sc.textFile('/mnt/e/Training_Data/5m-Sales-Records/*.csv') \
.map(lambda x: x.split(',')) \
.groupBy(lambda x: x[0]) \
.count()



# Example 1 with filter     
sc.textFile('/mnt/e/Training_Data/5m-Sales-Records/*.csv') \
.map(lambda x: x.split(',')) \
.filter(lambda x: int(x[5].split('/')[2]) >= 2020) \
.groupBy(lambda x: x[0]) \
.count()