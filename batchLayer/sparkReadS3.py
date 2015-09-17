
m pyspark import SparkContext
sc = SparkContext(appName="TestApp")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# text_file = sc.textFile("s3://amazon-reviews/complete.json")
# text_file = sc.textFile(    "s3://patricks3db/meta_Movies_and_TV.json")
# df = sqlContext.read.json("s3://patricks3db/meta_Movies_and_TV.json")

# connect to cassandra
from cassandra.cluster import Cluster
# cluster = Cluster(['172.31.39.223', '172.31.39.224','172.31.39.225','172.31.39.226'])
cluster = Cluster(['172.31.39.226'])
session = cluster.connect()

insert_statment = session.prepare('INSERT INTO playground.metadata JSON ? ;')
# counts = text_file.map(lambda line: session.execute(insert_statment, line))

import json
# test another way
from boto.s3.connection import S3Connection
from boto.s3.key import Key
conn = S3Connection()
conn = S3Connection()
bucket = conn.get_bucket('patricks3db')
key = bucket.get_key("meta_Movies_and_TV.json")
for i,line in enumerate(key.get_contents_as_string().splitlines()[:]):
        line = line.replace("'","''")
        jsonObject = json.loads(line)
        if "title" in jsonObject.keys():
                jsonObject["title"] = jsonObject["title"]
        if "related" in jsonObject.keys():
                for key in jsonObject["related"].keys():
                        jsonObject["related"][key] = '|'.join(jsonObject["related"][key])
        if "categories" in jsonObject.keys():
                jsonObject["categories"] = ['|'.join(c) for c in jsonObject["categories"]]
        print i
        session.execute("insert into playground.metadata json '" + json.dumps(jsonObject) + "';")

key = bucket.get_key("reviews_Movies_and_TV.json")
for i,line in enumerate(key.get_contents_as_string().splitlines()[:]):
        line = line.replace("'","''")
        jsonObject = json.loads(line)
        session.execute("insert into playground.review json '" + json.dumps(jsonObject) + "';")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
qlContext.read.json("s3n://patricks3db/reviews_Movies_and_TV.json")
# text_file = sc.textFile("s3n://patricks3db/reviews_Movies_and_TV.json")
text_file = sc.textFile("s3n://patricks3db/meta_Movies_and_TV.json")
text_file.map(lambda x: (x,1))
