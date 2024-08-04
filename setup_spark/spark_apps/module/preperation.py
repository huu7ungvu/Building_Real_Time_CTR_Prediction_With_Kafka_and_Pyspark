# tách cột hour ra 4 cột mới
from pyspark.sql.functions import month, dayofweek, dayofmonth, hour
from pyspark.sql.functions import col, when

def preperation (train):
    # create some feature from hour 
    train = train.withColumn("month", month("hour"))
    train = train.withColumn("dayofweek", dayofweek("hour"))
    train = train.withColumn("day", dayofmonth("hour"))
    train = train.withColumn("hour_time", hour("hour"))

    # drop cols
    train = train.drop("hour").drop("index")

    # rename col
    train = train.withColumnRenamed("click", "y").withColumnRenamed("hour_time", "hour")

    return train