import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import from_json, col, current_timestamp
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, DoubleType

# intiating the load_dotenv environmental variable
load_dotenv()

# importing kafka variables
kafka_boostrap_server = os.getenv('KAFKA_BROKER','kafka:29092')
kafka_topic = os.getenv('KAFKA_TOPIC', 'reddit_stream')

# Loading MONGODB credentials
MONGO_USERNAME = os.getenv('MONGO_USER')
MONGO_PASSSWORD = os.getenv('MONGO_PASSWORD')
MONGO_DB = os.getenv('MONGO_DB')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION')

MONGO_URI = f'mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSSWORD}@amdari-cluster.9bobwrn.mongodb.net/{MONGO_DB}?retryWrites=true&w=majority'

# Creating the spark session
class SparkConsumer:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName('RedditStreamProcessor') \
            .master('spark://spark:7077') \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1', 'org.mongodb.spark:mongo-spark-connector_2.12.:10.4.0') \
            .config('spark.mongodb.connection.uri', MONGO_URI) \
            .config('spark.mongodb.database', MONGO_DB) \
            .config('spark.mongodb.collection', MONGO_COLLECTION) \
            .config('spark.mongodb.write.connection.timeout.ms', '30000')\
            .config('spark.mongodb.read.connection.timeout.ms', '30000')\
            .config('spark.mongodb.operation.timeout.ms', '30000')\
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel('WARN')
        self.reddit_schema = StructType([
            StructField('id', StringType(), True),
            StructField('title', StringType(), True),
            StructField('author', StringType(), True),
            StructField('subreddit', StringType(), True),
            StructField('score', IntegerType(), True),
            StructField('num_comments', IntegerType(), True),
        ])
        
        print('spark session initialized')

    def process_stream(self):
        try:
            # read messages from the kafka topic
            kafka_stream = self.spark.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', kafka_boostrap_server) \
                .option('subscribe', kafka_topic) \
                .option('startingOffsets', 'earliest') \
                .option('failOnDataLoss', 'false') \
                .load()
            
            # Parse the Json data
            parsed_stream = kafka_stream \
                .select(from_json(col("value").cast("string"), self.reddit_schema).alias("data")) \
                .select('data.*')
            
            parsed_stream.printSchema()

            transformed_stream = parsed_stream.withColumn('time_stamp', current_timestamp())

            # Write to MongoDB
            def write_to_mongo(batch_df, batch_id):
                if batch_df.count() > 0:
                    print(f'processing batch ID :{batch_id} with {batch_df.count()} records ')
                    batch_df.show(truncate=False)
                    batch_df.write \
                        .format('mongodb') \
                        .mode('append') \
                        .option('database' , MONGO_DB) \
                        .option('collection', MONGO_COLLECTION) \
                        .save()
                    print(f' succesfully wrote batch {batch_id} to MongDB')
                else:
                    print(f' skipping empty batch {batch_id}')
            
            query = transformed_stream.writeStream \
                .foreachBatch(write_to_mongo) \
                .outputMode('append') \
                .option('checkpointLocation', '/tmp/mongodb_checkpoint') \
                .start()
            
            print('Streaming Process Started')
            query.awaitTermination()

        except Exception as e:
            print(f' Error in Spark consumer: {e}')
            raise e
        finally:
            self.spark.stop()
            print('Spark session stopped')

if __name__ == '__main__':
    try:
        spark_consumer = SparkConsumer()
        spark_consumer.process_stream()

    except Exception as e:
        print(f' failed to start or run Spark Consumer: {e}')