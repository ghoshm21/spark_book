#!/usr/bin/env bash
source ~/.bashrc
cd "/mnt/c/Users/ghosh/Google Drive/personal_project/Writing_Personal/Spark_Book/Working with RDD and Dataframe/example3_Custom Parsing"
SECONDS=0
echo "Starting sprk process"
export PYSPARK_PYTHON=python3
/home/sandipan/spark-3.1.1-bin-hadoop3.2/bin/spark-submit --master spark://LAPTOP-7DUT93OF.localdomain:7077 --executor-memory 6700mb ./03_custom_parsing_spark.py
duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."

# GCC
SECONDS=0
echo "Starting sprk process"
spark-submit ./03_custom_parsing_spark.py
duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."


SECONDS=0
echo "Starting sprk process"
export PYSPARK_PYTHON=python3
$SPARK_HOME/bin/spark-submit --master spark://LAPTOP-7DUT93OF.localdomain:7077 --executor-memory 6700mb ./python_udf_example.py
duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."