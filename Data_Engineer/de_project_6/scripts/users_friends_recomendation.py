import sys
import os
from math import radians, cos, sin, asin, sqrt, pi

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON']='/usr/bin/python3'

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark.sql.types import DoubleType,IntegerType
from pyspark.sql.window import Window

import pytz
from datetime import datetime

def get_distance_strangers(phi1, lm1, phi2, lm2):    
        K=pi/180
        distance=2 * 6371 * asin(sqrt(pow(sin((phi2 - phi1) * K),2)
                + cos(phi2 * K) * cos(phi1 * K) * pow(sin((lm2 - lm1) * K / 2), 2)))     

        return abs(distance)

def get_distance(phi1, lm1, phi2, lm2):
        K=pi/180
        phi1=float(phi1.replace(",","."))
        lm1=float(lm1.replace(",","."))
        distance=2 * 6371 * asin(sqrt(pow(sin((phi2 - phi1) * K),2)
            + cos(phi2 * K) * cos(phi1 * K) * pow(sin((lm2 - lm1) * K / 2), 2)))     

        return distance

class Geo:
    def __init__(self, rdd_geo) -> None:
        self.cities_list = rdd_geo

    def get_nearest_city(self, phi2, lm2):
        if not phi2 or not lm2:
            return None
        phi2=float(phi2)
        lm2=float(lm2)
        dist=[]
        for row in self.cities_list:
            distance=get_distance(row['lat'],row['lng'], phi2, lm2)
            d_list=(distance, row['id'])
            dist+=(d_list,)
        city_id=sorted(dist)[0][1]  
        
        return city_id 

def main():
        base_input_path = sys.argv[1]
        date = sys.argv[2]
        parquet_name = sys.argv[3]
        base_output_path = sys.argv[4]

        spark_session = SparkSession.builder \
                .master("yarn") \
                .appName("7project_step3") \
                .getOrCreate()

        df_geo=spark_session.read.option("delimiter", ";").csv('/user/nybydantse/geo.csv', header='true')

        events=spark_session\
         .read\
         .parquet(f"{base_input_path}/date={date}/{parquet_name}")

        geo_utils = Geo(df_geo.collect())
        nearest_city=F.udf(lambda phi2, lm2: geo_utils.get_nearest_city(phi2, lm2))

        subscription_users=events\
                .filter(F.col("event_type") == "subscription") \
                .filter(F.col("event.user").isNotNull()) \
                .filter(F.col("event.subscription_channel").isNotNull()) \
                .select(F.col("event.user").alias("user_id"), "event.subscription_channel") \
                .groupBy("user_id").agg(F.collect_set("subscription_channel").alias("subscription")) \
                .cache()

        last_messages=events\
        .filter(F.col("event_type")=='message')\
        .withColumn("city_id", nearest_city("lat","lon"))\
        .select(F.col("event.message_from").alias("user_id"),
                F.col("city_id"),
                F.col("lat"),
                F.col("lon"),
                F.coalesce("event.datetime","event.message_ts").cast("timestamp").alias("ts"))\
        .withColumn("row", F.row_number().over(Window().partitionBy("user_id").orderBy("ts")))\
        .filter("row=1").drop("ts","row").cache()      

        dialogs=events\
                .filter(F.col("event_type")=='message')\
                .filter(F.col("event.message_from").isNotNull())\
                .filter(F.col("event.message_to").isNotNull())\
                .select("event.message_from","event.message_to").distinct().cache()

        all_connections=dialogs\
                .select(F.col("message_from").alias("user_left"),
                        F.col("message_to").alias("user_right"))\
                .union(dialogs.select(F.col("message_to").alias("user_left"),
                                F.col("message_from").alias("user_right")))\
                .distinct().cache()

        user_left_msg=last_messages\
                .selectExpr("user_id as user_left","city_id","lat as left_lat","lon as left_lon")      

        user_right_msg=last_messages\
                .selectExpr("user_id as user_right","city_id","lat as right_lat", "lon as right_lon") 

        strangers=user_left_msg.join(user_right_msg, "city_id", "full").filter(F.col("user_left")!=F.col("user_right"))\
                .orderBy("city_id", "user_left")\
                .join(all_connections, ["user_left"], "anti")

        col_udf=F.udf(get_distance_strangers)
        strangers.withColumn("distance", col_udf(strangers.left_lat,strangers.left_lon,strangers.right_lat,strangers.right_lon))\
                .filter("distance <= 1.0")\
                .drop("left_lon", "left_lat", "right_lon", "right_lat")

        now = datetime.utcnow()

        strangers.join(subscription_users, strangers.user_left == subscription_users.user_id, "left") \
                .drop("user_id").withColumnRenamed("subscription", "left_subs") \
                .join(subscription_users, strangers.user_right == subscription_users.user_id, "left") \
                .drop("user_id").withColumnRenamed("subscription", "right_subs") \
                .filter(F.arrays_overlap("left_subs", "right_subs")) \
                .selectExpr("user_left", "user_right", "city_id as zone_id") \
                .withColumn("processed_dttm", F.lit(now)) \
                .withColumn("local_time", F.lit(now.astimezone(pytz.timezone("Australia/Sydney"))))\
                .write.mode("append").parquet(f"{base_output_path}/friends_recomendation")

if __name__ == "__main__":
        main()