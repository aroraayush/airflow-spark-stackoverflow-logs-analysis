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

text_file = sc.textFile("gs://stackoverflow-dataset-677/Comments.xml")
filteredRDD = text_file.filter(lambda x: x.startswith("  <row "))
cleanedRDD = filteredRDD.map(lambda x: x.lstrip("  "))

def parse_xml(rdd):
    """
    Read the xml string from rdd, parse and extract the elements,
    then return a list of list.
    """
    root = ET.fromstring(rdd)
    rec = []
    
    if "PostId" in root.attrib:
        rec.append(int(root.attrib['PostId']))
    else:
        rec.append(0)

    
    if "Score" in root.attrib:
        rec.append(int(root.attrib['Score']))
    else:
        rec.append(0)

    
    if "Text" in root.attrib:
        rec.append(root.attrib['Text'])
    else:
        rec.append("N/A")
    
    if "CreationDate" in root.attrib:
        rec.append(root.attrib['CreationDate'])
    else:
        rec.append("N/A")

    if "UserId" in root.attrib:
        rec.append(int(root.attrib['UserId']))
    else:
        rec.append(0)
    return rec

records_rdd = cleanedRDD.map(lambda x : parse_xml(x))
comments_data = ["postId","score","text","creationDate","userId"]
comments_df = records_rdd.toDF(comments_data)
comments_df.createOrReplaceTempView("comments")
comments_sql_df = spark.sql("SELECT * FROM comments")

users_data = sc.textFile("gs://stackoverflow-dataset-677/users_out1/*.csv")

def create_user(rdd):
    rdd_split = rdd.split(",")
    return [int(rdd_split[0]),rdd_split[1]]

users_rdd = users_data.map(lambda x: create_user(x))
user_data = ["id","username"]
user_df = users_rdd.toDF(user_data)
user_df.createOrReplaceTempView("users")
comments_users_sql_df = spark.sql("SELECT * FROM users u JOIN comments c ON u.id = c.UserId")
comments_users_sql_df.repartition(1).write.csv("gs://stackoverflow-dataset-677/users_out1", sep=',')
