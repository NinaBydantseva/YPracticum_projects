from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql import functions as f

from datetime import datetime
from pyspark.sql.functions import round
import psycopg2
import logging

# Настроим логгер
logging.basicConfig(level=logging.ERROR)  # или logging.DEBUG для более подробного логирования
logger = logging.getLogger(__name__)

# записываем df в PostgreSQL
def write_to_postgresql(df, postgresql_settings_out):
    try:
        df.write \
            .format('jdbc') \
            .option('driver', 'org.postgresql.Driver') \
            .option(**postgresql_settings_out) \
            .save()   
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}")
# записываем df в Kafka
def write_to_kafka(df):
    try:
        kafka_df=(df.select(f.to_json(
                f.struct("restaurant_id",
                "adv_campaign_id",
                "adv_campaign_content",
                "adv_campaign_owner",
                "adv_campaign_owner_contact",
                "adv_campaign_datetime_start",
                "adv_campaign_datetime_end",
                "datetime_created",
                "client_id",
                "trigger_datetime_created")).alias("value")))
                
        kafka_df.writeStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
            .option('kafka.security.protocol', 'SASL_SSL') \
            .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";') \
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
            .option('subscribe', 'NinaBydantseva_out') \
            .save()                
    except Exception as e:
        logger.error(f"Error writing to Kafka: {str(e)}")

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id, ):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    postgresql_settings_out = {
        'url': 'jdbc:postgresql://localhost:5432/de',
        'user': 'jovyan',
        'password': 'jovyan',
        'dbtable': 'subscribers_feedback',
        'schema': 'public'
    }
    write_to_postgresql(df, postgresql_settings_out)
    # создаём df для отправки в Kafka. Сериализация в json и отправляем сообщения в результирующий топик Kafka без поля feedback
    write_to_kafka(df)
    # очищаем память от df
    df.unpersist()
    
# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
def create_spark_session():
    spark_jars_packages = ",".join(
            [
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
                "org.postgresql:postgresql:42.4.0",
            ]
        )

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
    spark = SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
        
    return spark

# читаем из топика Kafka сообщения с акциями от ресторанов 
def read_kafka_stream():
    restaurant_read_stream_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('subscribe', 'NinaBydantseva_in') \
        .load()

    return restaurant_read_stream_df

# вычитываем всех пользователей с подпиской на рестораны    
def read_subscribers_data(postgresql_settings):
    subscribers_restaurant_df = spark.read \
                        .format('jdbc') \
                        .option('driver', 'org.postgresql.Driver') \
                        .option(**postgresql_settings) \
                        .load()
                        
    return subscribers_restaurant_df

def filter_join_and_transform_data(restaurant_read_stream_df, subscribers_data):
# определяем схему входного сообщения для json
    incomming_message_schema = StructType([
            StructField("restaurant_id", LongType()),
            StructField("adv_campaign_id", LongType()),
            StructField("adv_campaign_content", StringType()),
            StructField("adv_campaign_owner", StringType()),
            StructField("adv_campaign_owner_contact", StringType()),
            StructField("adv_campaign_datetime_start", TimestampType()),
            StructField("adv_campaign_datetime_end", TimestampType()),		
            StructField("datetime_created", TimestampType())		
            ])

    # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
    filtered_read_stream_df = (restaurant_read_stream_df
            .withColumn('value', f.col('value').cast(StringType()))
            .withColumn('event', f.from_json(f.col('value'), incomming_message_schema))
            .selectExpr('event.*')
            .withColumn('adv_campaign_datetime_start', f.col('adv_campaign_datetime_start').cast(TimestampType()))
            .withColumn('adv_campaign_datetime_end', f.col('adv_campaign_datetime_end').cast(TimestampType()))
            .filter((f.col('adv_campaign_datetime_start') <= f.unix_timestamp(f.lit(datetime.now())).cast('timestamp')) & 
            (f.col('adv_campaign_datetime_end') >= f.unix_timestamp(f.lit(datetime.now())).cast('timestamp'))))
        
# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
    result_df = (filtered_read_stream_df.join(subscribers_data, "restaurant_id")
            .withColumn('trigger_datetime_created',
                            f.unix_timestamp(f.lit(datetime.now())).cast('timestamp')))
                        
# код для сохранения данных в PostgreSQL и Kafka
def save_to_postgresql_and_kafka(df):
#создаем таблицу subscribers_feedback в Postgre
    ps_host='localhost'
    ps_port='5432'
    ps_dbname='de'
    ps_user='jovyan'
    ps_password='jovyan'
    
    conn = psycopg2.connect(f"host={ps_host} port={ps_port} dbname={ps_dbname} user={ps_user} password={ps_password}")
    with conn.cursor() as cur:
        cur.execute(
        """CREATE TABLE if not exists public.subscribers_feedback(
        id serial4 NOT NULL,
        restaurant_id text NOT NULL,
        adv_campaign_id text NOT NULL,
        adv_campaign_content text NOT NULL,
        adv_campaign_owner text NOT NULL,
        adv_campaign_owner_contact text NOT NULL,
        adv_campaign_datetime_start int8 NOT NULL,
        adv_campaign_datetime_end int8 NOT NULL,
        datetime_created int8 NOT NULL,
        client_id text NOT NULL,
        trigger_datetime_created int4 NOT NULL,
        feedback varchar NULL,
        CONSTRAINT id_pk PRIMARY KEY (id)
        ");""")
        
#добавляем поле feedback
    result_df=result_df.withColumn('feedback',f.lit(None).cast(StringType()))

# запускаем стриминг
    result_df.writeStream \
        .trigger(processingTime='60 seconds') \
        .foreachBatch(foreach_batch_function) \
        .start() \
        .awaitTermination()

def main():
    postgresql_settings_in = {
        'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
        'user': 'student',
        'password': 'de-student',
        'dbtable': 'subscribers_restaurants',
        'schema': 'public'
    }
    spark = create_spark_session()
    restaurant_read_stream_df = read_kafka_stream()
#    current_timestamp_utc = get_current_timestamp_utc()
#    filtered_data = filter_stream_data(restaurant_read_stream_df, current_timestamp_utc)
    subscribers_data = read_subscribers_data(postgresql_settings_in)
    result_df = filter_join_and_transform_data(restaurant_read_stream_df, subscribers_data)
    save_to_postgresql_and_kafka(result_df)
    spark.stop() 

if __name__ == "__main__":
    main() 