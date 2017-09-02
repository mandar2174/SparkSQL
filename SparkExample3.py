'''
Created on Aug 13, 2017

@author: mandar
'''

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import from_unixtime, when
from pyspark.sql.functions import udf
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.context import SQLContext
from pyspark.sql.context import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import math

from time import time


'''
How to run:

/usr/local/spark-2.0.2/bin/spark-submit  /home/mandar/Downloads/Spark_Example/pyspark/example/dataframe/SparkExample3.py

'''


if __name__ == '__main__':
    
    # Set Spark properties which will used to create sparkcontext
    conf = SparkConf().setAppName('SparkExample1').setMaster('local[*]')
    
    # create spark context and sql context
    sc = SparkContext(conf=conf)
    hive_context = HiveContext(sc)
    
        
    # read the input data file and create spark dataframe
    record_dataframe = hive_context.read.format("com.databricks.spark.csv")\
    .option("header", "false") \
    .option("inferschema", "true") \
    .option("delimiter", "\n") \
    .load("file:///home/mandar/Downloads/Spark_Example/resources/1").withColumnRenamed("_c0", "record")
    
    # meta config dataframe
    metaconfig_dataframe = hive_context.read.format("com.databricks.spark.csv")\
    .option("header", "true") \
    .option("inferschema", "true") \
    .option("delimiter", "\t") \
    .load("file:///home/mandar/Downloads/Spark_Example/resources/meta_config")
    
    metaconfig_dataframe.printSchema()
    metadata_result = metaconfig_dataframe.select("START", "LENGTH").collect()
    substring_query = ''
    
    index = 0
    column_name_list = list()
    start_time = time()
    
    for metadata in metadata_result:
        record_dataframe = record_dataframe.withColumn("Column_" + str(index), record_dataframe.record.substr(int(math.floor(metadata.START)), int(metadata.LENGTH)))
        print "Created column : " + str(index)
        column_name_list.append("Column_" + str(index))
        index = index + 1
        
    record_dataframe = record_dataframe.drop('record')
    
    # #-- Set Parameters --##
    record_total_length = 3652
    record_field_delimited = 'A'  # A = comma, B. Tab, C. Pipe, D. Semi-colon
    record_eof_choice = 'A'  # A. Unix - 0A , B. DOS (Windows) - 0D0A
    record_header_choice = 'Y'  # Want header rec - Y or N
    
    if record_header_choice == 'Y' or record_header_choice == 'N':
    
        if record_eof_choice == 'A':
            record_dataframe = record_dataframe.withColumn("record_eof_part", lit("\n").astype('string'))
        elif record_eof_choice == 'B':
            record_dataframe = record_dataframe.withColumn("record_eof_part", lit("\r\n").astype('string'))
        
        if record_field_delimited == 'A':
            record_dataframe.repartition(1).write.format('com.databricks.spark.csv').options(delimiter=',').save("file:///home/mandar/Downloads/Spark_Example/resources/tsp_result")
        elif record_field_delimited == 'B':
            record_dataframe.repartition(1).write.format('com.databricks.spark.csv').options(delimiter='\t').save("file:///home/mandar/Downloads/Spark_Example/resources/tsp_result")
        elif record_field_delimited == 'C':
            record_dataframe.repartition(1).write.format('com.databricks.spark.csv').options(delimiter='|').save("file:///home/mandar/Downloads/Spark_Example/resources/tsp_result")
        else:
            record_dataframe.repartition(1).write.format('com.databricks.spark.csv').options(delimiter=':').save("file:///home/mandar/Downloads/Spark_Example/resources/tsp_result")
        
    
    
    
    print 'Finished time : ' + str(time() - start_time)
    
