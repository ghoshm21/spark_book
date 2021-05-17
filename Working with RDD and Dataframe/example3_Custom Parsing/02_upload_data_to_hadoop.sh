# this script will load the data to hadoop.
# I am using GCP dataproc for hadoop cluster.
# This should work in anyother hadoop cluster as well
#-------------------------------------------------------------------#
# make data data dir
hdfs dfs -ls .
hdfs dfs -mkdir ./data/
#-------------------------------------------------------------------#
# option 1: copy the input file directly to hdfs and then use it.
# due to not splittable file for it will not use spark's parallel process
# this process takes 2 min, however increse the spark processing time by 3 times
hdfs dfs -put ./ol_cdump_latest.txt.gz ./data/

#-------------------------------------------------------------------#
# option 2: uncompress the file on the fly and store the uncompress data in HDFS
# this required more space - 135GB and tkes time to uncompress
# this process takes 2 hours and it cut done the spark processing time by half

# uncompress and put to HDFS
zcat ./ol_cdump_latest.txt.gz | hdfs dfs -put - ./data/
hdfs dfs -mv ./data/- ./data/ol_cdump_latest.txt

#-------------------------------------------------------------------#
# option 3: upload the compress file in HDFS and then run a hadoop streaming job to uncompress and re-compress as splittable file format like BZip2
# you can use PIG script or hive script as well.
# this is time consuming - hadoop streaming job takes 4 hours and spark prcess time is reduce by half
# load the data
hdfs dfs -put ./ol_cdump_latest.txt.gz ./data/
# run the streaming job to uncompress and re-compress the data
hadoop jar /home/ghosh/hadoop-streaming.jar \
        -Dmapreduce.job.reduces=4 \
        -Dmapreduce.output.fileoutputformat.compress=true \
        -Dmapreduce.map.output.compress=true \
        -Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec \
        -input ./data/ol_cdump_latest.txt.gz \
        -output ./data/copy \
        -mapper /bin/cat \
        -reducer /bin/cat \
        -inputformat org.apache.hadoop.mapred.TextInputFormat \
        -outputformat org.apache.hadoop.mapred.TextOutputFormat
#-------------------------------------------------------------------#

# option 4: split the compress file in local , into multiple compress file.
# this require extra space in local file system
# split the compress file into miltiple compress files.
zcat ol_cdump_latest.txt.gz | split -l 4000000 --filter='gzip > ol_cdump_latest.$FILE.gz'

# delete original file

# load all the split files to hdfs