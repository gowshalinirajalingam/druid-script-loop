from lib2to3.fixer_util import String

from pyspark.sql import SparkSession
import requests
import json
from datetime import datetime,timedelta
import sys
from pyspark.sql.functions import datediff, to_date, unix_timestamp
import pyspark.sql.functions as f

from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("First Apache Spark Application using Scala - Demo") \
        .master("local[*]") \
        .getOrCreate()

    sc = spark.sparkContext
    def loop(x):
        print("First Apache Spark Application using Python Starts Here ...")



        headers = {
            'Content-Type': 'application/json',
        }

        params = (
            ('pretty', ''),
        )

        subday = (datetime.today() - timedelta(days=x)).strftime('%Y-%m-%dT00:00:00.000Z')
        subdayminus1 = (datetime.today() - timedelta(days=x+1)).strftime('%Y-%m-%dT00:00:00.000Z')


        with open('subscriberwise_protocolwise_Usage_download_upload_days_pivot.json', 'r+') as f:
            data = json.load(f)
            data['intervals'] = '{}/{}'.format(subday, subdayminus1)  # <--- add `interval` value.
            f.seek(0)  # <--- should reset file position to the beginning.
            json.dump(data, f, indent=4)
            f.truncate()

        print("replaced interval")
        data = open('subscriberwise_protocolwise_Usage_download_upload_days_pivot.json')  # open interval wriiten json #file again

        response = requests.post('http://localhost:8080/druid/v2', headers=headers, params=params, data=data)
        print(response)

        print("loaded data from druid")
        res = response.text
        # print(res)
        loaded_json = json.loads(res)

        schema = StructType([
            StructField("tcp_sum_download", StringType(), True),
            StructField("dns_sum_download", StringType(), True),
            StructField("google_gen_sum_download", StringType(), True),
            StructField("ssl_sum_download", StringType(), True),
            StructField("subscriber_id", StringType(), True),
            StructField("http_sum_download", StringType(), True),
            StructField("google_play_sum_download", StringType(), True),
            StructField("https_sum_download", StringType(), True),
            StructField("youtube_sum_download", StringType(), True),
            StructField("google_ads_sum_download", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("facebook_sum_download", StringType(), True)

        ])
        df1 = spark.createDataFrame([], schema) # spark is the Spark Session
        for x in loaded_json:  # Loop through jsonArray(In python we call json array as List)
            result = x['event']
            # print(json.dumps(result))   #json.loads means {'name':'asff','age':2}    json.dumps means {"name":"asff","age":2}
                                            #TO Convert to python dataframe json object should be json.dumps format
            jsarrayy = [json.dumps(result)]
            # print(jsarrayy)

            df = spark.read.json(sc.parallelize(jsarrayy))
            # df.show()
            df1 = df1.union(df)

        df1.show()


    startdate = sys.argv[1]
    enddate = sys.argv[2]
    print(enddate)
    print(startdate)







    l = [(enddate, startdate) ]

    df = spark.createDataFrame(l, ['enddate', 'startdate'])
    df.show()



    timeFmt = "yyyy-MM-dd"
    df = df.withColumn( "diff_days", datediff(to_date("enddate",timeFmt),to_date("startdate",timeFmt)))
    # add an index column
    df = df.withColumn('index', f.monotonically_increasing_id())

    #select specific value form dataframe
    result = df.select('diff_days').collect()[0]
    diffdays=result.asDict()['diff_days']

    dff1 = spark.createDataFrame(list(map(lambda x: loop(x), range(0, diffdays))))



