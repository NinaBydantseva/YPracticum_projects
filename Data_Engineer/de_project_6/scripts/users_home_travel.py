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
    parquet_name=sys.argv[3]    
    base_output_path = sys.argv[4]
    consecutive_days=int(sys.argv[5])

    spark_session = SparkSession.builder \
                .master("yarn") \
                .appName("7project_step1") \
                .getOrCreate()
    #считываем файл с городами и координатами центров
    df_geo=spark_session.read.option("delimiter", ";").csv('/user/nybydantse/geo.csv', header='true')
    events=spark_session\
        .read\
        .parquet(f"{base_input_path}/date={date}/{parquet_name}")
    geo_utils = Geo(df_geo.collect())
    nearest_city=F.udf(lambda phi2, lm2: geo_utils.get_nearest_city(phi2, lm2))

    window = Window().partitionBy("user_id")
    #Собираем таблицу со столбцами: user_id (из поля event.message_from), ts (время, когда отправлено сообщение), city (город, из которого отправлено сообщение)
    city_events=events\
        .filter(F.col("event_type")=='message')\
        .withColumn("city_id", nearest_city("lat","lon"))\
        .select(F.col("event.message_from").alias("user_id"),
                F.col("city_id").alias("id"),
                F.coalesce("event.datetime","event.message_ts").cast("timestamp").alias("ts"))\
        .join(df_geo, "id", "left").select("user_id","ts","city")

    #Собираем статистику, откуда пользователь отправлял сообщения
    travels=city_events\
            .withColumn("prev_city", F.lag("city").over(window.orderBy("ts")))\
            .filter("prev_city is null or prev_city<>city")\
            .groupBy("user_id").agg(F.collect_list("city").alias("travel_array"),
                                    F.count("city").alias("travel_count"))

    #Вычисляем домашний город отправителя
    home_cities=city_events\
                .withColumn("date", F.to_date("ts"))\
                .withColumn("rank", F.rank().over(window.orderBy("date")))\
                .withColumn("grp", F.date_sub("date", "rank"))\
                .groupBy("user_id", "city", "grp").agg(F.min("date").alias("min"), F.max("date").alias("max"))\
                .withColumn("const_days", F.least(F.datediff("max", "min"), F.lit(consecutive_days)))\
                .withColumn("last_city", F.row_number().over(window.orderBy(F.desc("const_days"),F.desc("max"))))\
                .filter("last_city=1").select("user_id", F.col("city").alias("home_city"))

    #Собираем витрину в разрезе пользователей
    city_events.withColumn("row_num", F.row_number().over(window.orderBy(F.desc("ts"))))\
                .filter("row_num=1")\
                .select(F.col("user_id"),
                        F.col("city").alias("act_city"),
                        F.from_utc_timestamp(F.col("ts"), F.lit("Australia/Sydney")).alias("local_time"))\
                .join(travels, "user_id", "left")\
                .join(home_cities, "user_id", "left")\
                .write.mode("overwrite").parquet(base_output_path)

if __name__ == "__main__":
        main()