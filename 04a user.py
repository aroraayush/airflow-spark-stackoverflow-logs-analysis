from pyspark.sql.functions import *
import time
import pyspark.sql.functions as F
from pyspark.sql.types import *
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession

spark = SparkSession \
.builder \
.appName("My PySpark code") \
.getOrCreate()

text_file = sc.textFile("gs://stackoverflow-dataset-677/Users.xml")
filteredRDD = text_file.filter(lambda x: x.startswith("  <row "))
cleanedRDD = filteredRDD.map(lambda x: x.lstrip("  "))

def parse_xml(rdd):
    root = ET.fromstring(rdd)
    rec = []
    id = root.attrib['Id']
    if id == "-1":
        id = "1"
    rec.append(id)
    rec.append(root.attrib['DisplayName'])
    return rec
    
records_rdd = cleanedRDD.map(lambda x : parse_xml(x))

user_data = ["id","username"]
user_df = records_rdd.toDF(user_data)
user_df.repartition(1).write.csv("gs://stackoverflow-dataset-677/users_out1", sep=',')
