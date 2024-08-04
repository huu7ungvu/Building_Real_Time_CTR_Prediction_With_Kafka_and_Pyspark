from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql.functions import col
import module.preperation as pp

if __name__ == "__main__":
    print("Stream Data Processing Application Started ...")

    # create spark session to run the job
    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # config kafka topic 
    KAFKA_TOPIC_NAME_CONS = 'ctr_queue'
    KAFKA_BOOTSTRAP_SERVERS_CONS = 'kafka1:9093'

    # read trained model 
    # model = PipelineModel.load("D:\ITfiles\PythonFiles\Spark\prj\model")
    model = CrossValidatorModel.read().load("/opt/spark/data/project_data/trained_model/model")

    # read data from a topic kafka by spark streaming
    messages = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false")\
        .load()

    # parse message
    messages_parse = messages.selectExpr("CAST(value AS STRING)", "timestamp")
    
    # declare schema
    schema = '''index STRING,
        id STRING,
        click INT,
        hour STRING,
        C1 INT,
        banner_pos INT,
        site_id STRING,
        site_domain STRING,
        site_category STRING,
        app_id STRING,
        app_domain STRING,
        app_category STRING,
        device_id STRING,
        device_ip STRING,
        device_model STRING,
        device_type INT,
        device_conn_type INT,
        C14 INT,
        C15 INT,
        C16 INT,
        C17 INT,
        C18 INT,
        C19 INT,
        C20 INT,
        C21 INT'''

    # convert the raw CSV data from each Kafka message into a structured format that can be easily processed using Spark.
    ctr_df = messages_parse.select(
            from_csv(col("value"), schema).alias("data"),
            "timestamp"
        )

    # select only necessary cols
    ctr_df = ctr_df.select("data.*", "timestamp")
    
    # print schema
    ctr_df.printSchema()

    # preparation data
    ctr_pre_df = pp.preperation(ctr_df)

    # apply model to predict ctr
    prediction = model.transform(ctr_pre_df)
    result_df_1 = prediction.select('id','y', "prediction",'timestamp')
    result_df_2 = prediction.select('id','y','prediction')

    
    orders_agg_write_stream1 = result_df_2 \
        .writeStream \
        .trigger(processingTime = "5 seconds")\
        .outputMode("append") \
        .option("path", "/opt/spark/data/project_data/output")\
        .option("checkpointLocation", "opt/spark/data/kafka_stream_test_out/chk") \
        .format("csv") \
        .start()

    orders_agg_write_stream = result_df_1 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    orders_agg_write_stream1.awaitTermination()  
    orders_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")