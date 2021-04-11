import random
import time
import argparse
import json
from datetime import datetime

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.utils import StreamingQueryException
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F


spark_check_point_dir = "checkpoint"

WATERMARK_DELAY_THRESHOLD_SECONDS = 5  # 窗口水位
WINDOW_DURATION_SECONDS = 60  # 窗口大小
WINDOW_SLIDE_DURATION_SECONDS = 5  # 窗口步长
WATERMARK_DELAY_THRESHOLD = "{} seconds".format(WATERMARK_DELAY_THRESHOLD_SECONDS)
WINDOW_DURATION = "{} seconds".format(WINDOW_DURATION_SECONDS)
WINDOW_SLIDE_DURATION = "{} seconds".format(WINDOW_SLIDE_DURATION_SECONDS)

transferSchema = T.StructType(
    [
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("school", T.StringType(), True),
        T.StructField("major", T.StringType(), True),
        T.StructField("data", T.BinaryType(), True),
    ]
)


def _kafka_args():
    parser = argparse.ArgumentParser(add_help=False)
    args = parser.add_argument_group("kafka configurations")
    args.add_argument(
        "-t",
        "--source_topics",
        help="Kafka source topics, separate by comma",
        default="kafka-default",
    )
    args.add_argument(
        "-a", "--address", help="Kafka address.", default="localhost:9092"
    )
    args.add_argument(
        "-st", "--sink_topic", help="Kafka sink topic", default="kafka-default-sink"
    )
    args.add_argument("-u", "--username", help="Kafka server's username.")
    args.add_argument("-p", "--password", help="Kafka server's password.")
    return parser


def start_algorithm_logic(sdf: DataFrame) -> DataFrame:
    @F.udf(returnType=transferSchema)
    def transferDF(data):
        # 根据传入的数据来做反序列化，可以是pb 也可以是json。具体结构可根据kafka producer里生产的数据结构调整
        # obj = ObjectInfo()
        # obj.ParseFromString(pb_bytes)
        # 在这里，我们将data定义为json dict.
        # {"timestamp": "2006-01-02Z03:04:05", "school": "cambridge", "major": "computer science", "name": "hanmeimei", "extra": ""}
        obj = json.loads(data)
        return (
            datetime.strptime(obj["timestamp"], "%Y-%m-%dT%H:%M:%S.%f"),
            obj["school"],
            obj["major"],
            data,
        )

    @F.udf(T.ArrayType(T.BinaryType()))
    def handle_grouped_data(data_list, window) -> T.ArrayType(T.BinaryType()):
        if not window or len(window) != 1:
            print("handle_grouped_data, should have only one window instance")
            return None
        if len(data_list) == 0:
            return None
        sample = data_list[0]
        sample_dict = json.loads(sample)
        print(
            f"date_list length:{len(data_list)},school: {sample_dict['school']}, major: {sample_dict['major']}, window start:{window[0].start}, window end:{window[0].end}"
        )

        # do whatever you want to do with the date set here.

        # return the result list which will produce to kafka sink topic
        results = [sample]
        return results

    # 从pb数据中拆出timestamp，region_id, camera_id用来group和做窗口
    sdf = sdf.select(transferDF(sdf.value).alias("window_data")).select(
        "window_data.timestamp",
        "window_data.school",
        "window_data.major",
        "window_data.data",
    )

    # 水位设置，将滑动窗口中数据以camera_id和region_id分组
    window_group = sdf.withWatermark("timestamp", WATERMARK_DELAY_THRESHOLD).groupBy(
        F.window(F.col("timestamp"), WINDOW_DURATION, WINDOW_SLIDE_DURATION),
        F.col("school"),
        F.col("major"),
    )
    # 处理数据
    result_df_set = window_group.agg(
        handle_grouped_data(
            F.collect_list("data"), F.collect_set("window")
        ).alias(  # 使用窗口数据
            "value_set"
        )
    ).withColumn(
        "value", F.explode(F.col("value_set"))
    )  # 拆分处理后的数据集

    # 返回结果
    return result_df_set.filter(result_df_set.value != b"")


def start_query(sc: SparkContext, args: argparse.Namespace):

    spark = SparkSession(sparkContext=sc)
    kafka_address = args.address
    kafka_source_topics = args.source_topics
    kafka_sink_topic = args.sink_topic
    kafka_username = args.username
    kafka_password = args.password
    if kafka_username and kafka_password:
        kafka_jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username={kafka_username} password={kafka_password};"
        # kafka source
        sdf = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_address)
            .option("subscribe", kafka_source_topics)
            .option("kafka.security.protocol", "SASL_PLAINTEXT")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", kafka_jaas_config)
            .option("kafka.reconnect.backoff.ms", 2000)
            .option("kafka.reconnect.backoff.max.ms", 10000)
            .option("failOnDataLoss", False)
            .option("backpressure.enabled", True)
            .load()
        )

        sdf = start_algorithm_logic(sdf)

        # out sink
        query = (
            sdf.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_address)
            .option("topic", kafka_sink_topic)
            .option("kafka.security.protocol", "SASL_PLAINTEXT")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", kafka_jaas_config)
            .option("kafka.reconnect.backoff.ms", 2000)
            .option("kafka.reconnect.backoff.max.ms", 10000)
            .option("checkpointLocation", spark_check_point_dir)
            .start()
        )
        return query
    else:
        # kafka source
        sdf = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_address)
            .option("subscribe", kafka_source_topics)
            .option("kafka.reconnect.backoff.ms", 2000)
            .option("kafka.reconnect.backoff.max.ms", 10000)
            .option("failOnDataLoss", False)
            .option("backpressure.enabled", True)
            .load()
        )

        sdf = start_algorithm_logic(sdf)

        # out sink
        query = (
            sdf.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_address)
            .option("topic", kafka_sink_topic)
            .option("kafka.reconnect.backoff.ms", 2000)
            .option("kafka.reconnect.backoff.max.ms", 10000)
            .option("checkpointLocation", spark_check_point_dir)
            .start()
        )
        return query


def main():

    parser = argparse.ArgumentParser(
        parents=[_kafka_args()],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    args = parser.parse_args()
    spark_conf = (
        SparkConf()
        .setMaster("local")
        .set(
            "spark.jars",
            "jars/*jar",
        )
        .set("spark.driver.memory", "10240m")
    )
    sc = SparkContext(conf=spark_conf)
    while True:
        query = start_query(sc, args)
        try:
            query.awaitTermination()
        except StreamingQueryException as err:
            wait = random.randint(0, 60)
            print(
                f"Query exception: {err},After {wait}seconds, query will be restarted."
            )
            time.sleep(wait)


if __name__ == "__main__":
    main()
