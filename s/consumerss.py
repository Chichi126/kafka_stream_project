import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from dotenv import load_dotenv



load_dotenv()

#Loading the Kafka Credentials
kafka_broker = os.getenv("KAFKA_BROKER")
kafka_topic=os.getenv("KAFKA_TOPIC")


#Loading the Mongodb Credentials

MONGO_USER=os.getenv('MONGO_USER')
MONGO_PASSWORD= os.getenv("MONGO_PASSWORD")
MONGO_COLLECTION=os.getenv("MONGODB_COLLECTION")
MONGO_DATABASE=os.getenv("MONGODB_DATABASE")



MONGO_URI= f'mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@amdari-cluster.9bobwrn.mongodb.net/{MONGO_COLLECTION}?retryWrites=true&w=majority'


# Create Spark Session

class SparkConsumer:
    def __init__(self):
        self.spark= SparkSession.builder \
            .appName("RedditStreamProcessor") \
            .config("spark://spark-master:7077") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12-3.4.1,"
                    "org.mongodb.spark:mongo-spark-connector_2.12-10.4.1")\
            .config("spark.mongdb.connection.uri", MONGO_URI)\
            .config("spark.mongodb.database", MONGO_DATABASE)\
            .config("spark.mongodb.collection", MONGO_COLLECTION)\
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")

        self.reddit_schema= StructType([
            StructField("id", StringType(), True),
            StructType("title", StringType(), True),
            StructType("author", StringType(), True),
            StructType("subreddit", StringType(), True),
            StructType("score", IntegerType(), True),
            StructType("num_comments", IntegerType(), True)
        ])

        print("Spark session initialized")
   
    def process_stream(self):
        try:
            kafka_stream = self.spark.readStream \
                .format('kafka')\
                .option('kafka.bootstrap.servers', kafka_broker)\
                .option('subscribe', kafka_topic)\
                .option('startingOffsets', 'earliest')\
                .option('failOnDataLoss', 'False')\
                .load()
            print("Kafka stream initialized")

            # Deserializing the Message
            parsed_stream = kafka_stream.select(from_json(col('value').cast('string'), self.reddit_schema).alias('data'))\
                .select('data.*')
            
            # creating a new column with the current timestamp
            transformed_stream =  parsed_stream.withColumn('processing_time', current_timestamp())

            def write_to_mongo_and_print(batch_id, batch_df):
                if batch_df.count()>0:
                    print(f'Processing batch ID {batch_id} with {batch_df.count()} records')
                    batch_df.show(truncate=False)

                    batch_df.write\
                        .format('mongo')\
                        .mode('append')\
                        .option('uri', MONGO_URI)\
                        .option('database', MONGO_DATABASE)\
                        .option('collection', MONGO_COLLECTION)\
                        .save()
                    print(f' Batch {batch_id} written to MongoDB')
                else:
                    print(f' Batch {batch_id} is empty, skipping write')

            # Using foreachBatch to process each micro-batch to Mongodb and also print it on the console

            query = transformed_stream.writeStream \
                .foreachBatch(write_to_mongo_and_print) \
                .outputMode('append')\
                .option('checkpointLocation', '/tmp/kafka_to_mongo_checkpoint')\
                .start()
            print("Streaming Process started")
            query.awaitTermination()

        except Exception as e:
            print(f' error occured in spark: {e}')
        finally:
            self.spark.stop()
            print('spark stopped')

if __name__ == "__main__":
    spark_consumer=SparkConsumer()
    spark_consumer.process_stream()
    print('stream processing working')