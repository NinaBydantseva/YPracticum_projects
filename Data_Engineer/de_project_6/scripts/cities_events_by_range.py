import os
import sys

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON']='/usr/bin/python3'

from datetime import datetime
from datetime import timedelta
from math import pi, sin, cos, asin, sqrt, pow, inf

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

def get_distance(phi1, lm1, phi2, lm2):
    K=pi/180
    phi1=float(phi1.replace(",","."))
    lm1=float(lm1.replace(",","."))
    distance=2 * 6371 * asin(sqrt(pow(sin((phi2 - phi1) * K),2)
            + cos(phi2 * K) * cos(phi1 * K) * pow(sin((lm2 - lm1) * K / 2), 2)))     

    return distance

class GeoUtils:

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
# Задаем параметры для командной строки        
        base_input_path = sys.argv[1]
        date = sys.argv[2]
        parquet_name=sys.argv[3]
        base_output_path = sys.argv[4]

        #conf = SparkConf().setAppName(f"VerifiedTagsCandidatesJob-{date}-d{depth}-cut{suggested_count}")
        #sc = SparkContext(conf=conf)
        #sql = SQLContext(sc)

        spark_session = SparkSession.builder \
            .master("yarn") \
            .appName("7project_step2") \
            .getOrCreate()

        df_geo=spark_session.read.option("delimiter", ";").csv('/user/nybydantse/geo.csv', header='true')
        events=spark_session\
            .read\
            .parquet(f"{base_input_path}/date={date}/{parquet_name}")
        geo_utils = GeoUtils(df_geo.collect())
        nearest_city=F.udf(lambda phi2, lm2: geo_utils.get_nearest_city(phi2, lm2))

        events_by_city=events.withColumn("city_id", nearest_city("lat","lon")).filter(F.col("city_id").isNotNull())

        tmp_stats=events_by_city\
        .filter(F.col("event_type")=='message')\
        .select(F.col("event.datetime").alias("date"),
           F.date_trunc("month", "event.datetime").alias("month"),
           F.date_trunc("week", "event.datetime").alias("week"),
           F.col("city_id").alias("zone_id"),
           F.col("event.message_from").alias("user"))\
       .withColumn("first", F.row_number().over(Window().partitionBy("user").orderBy("date")))\
       .filter("first=1")

        month_stats=tmp_stats.select("month", "week", "zone_id",
            F.when(F.col("first").eqNullSafe(1), 1).otherwise(0).alias("month_user"))\
            .groupBy("month", "zone_id")\
            .agg(F.sum("month_user").alias("month_user"))

        week_stats=tmp_stats.select("month", "week", "zone_id",
            F.when(F.col("first").eqNullSafe(1), 1).otherwise(0).alias("week_user"))\
            .groupBy("week", "zone_id")\
            .agg(F.sum("week_user").alias("week_user"))

        users_stats=month_stats.join(week_stats, "zone_id", "full")\
            .select("month", "week", "zone_id",
            F.create_map(F.lit("month_user"), "month_user", F.lit("week_user"), "week_user").alias("map"))\
            .select("month", "week", "zone_id", F.explode("map").alias("type", "count"))

        events.withColumn("city_id", nearest_city("lat","lon"))\
                    .select(F.date_trunc("month", "event.datetime").alias("month"),
                        F.date_trunc("week", "event.datetime").alias("week"),
                        F.col("city_id").alias("zone_id"),
                        F.col("event_type").alias("type"))\
                    .groupBy("month", "week", "zone_id", "type").count()\
                    .union(users_stats)\
                    .groupBy("month", "week", "zone_id").pivot("type").sum("count")\
                    .select("month", "week", "zone_id",
                            F.col("message").alias("week_message"),
                            F.col("reaction").alias("week_reaction"),
                            F.col("subscription").alias("week_subscription"),
                            F.col("week_user"),
                            F.col("message").alias("month_message"),
                            F.col("reaction").alias("month_reaction"),
                            F.col("subscription").alias("month_subscription"),
                            F.col("month_user"))\
                    .write.mode("overwrite").parquet(base_output_path)

if __name__ == "__main__":
        main()