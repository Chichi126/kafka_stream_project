import praw
import json
import time
from dotenv import load_dotenv
import os
from confluent_kafka import Producer

#Initiallize the load_doenv envronmental variable
load_dotenv()


#Load the Reddit Api credentials

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
user_agent = os.getenv('USER_AGENT')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
subreddit_name = os.getenv('REDDIT_SUBREDDIT')

kafka_broker = os.getenv('KAFKA_BROKER')
kafka_topic = os.getenv('KAFKA_TOPIC')


reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    user_agent=user_agent,
    username=username,  
    password=password
)


producer = Producer({
            'bootstrap.servers': kafka_broker,
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 100
        })
        
print(f" Connected to Kafka at {kafka_broker} and producing to topic {kafka_topic}")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")



def stream_reddit_data():
    subreddit = reddit.subreddit(subreddit_name)
    
    for submission in subreddit.stream.submissions():
        data = {
            'id': submission.id,
            'title': submission.title,
            'author': submission.author.name,
            'score': submission.score,
            'num_comments': submission.num_comments,
            'subreddit': submission.subreddit.display_name,
        }

        json_data = json.dumps(data, indent=4)

        # Print data before sending it to Kafka
        print(" Sending Post to Kafka:")

        producer.produce(kafka_topic, value=json_data.encode('utf-8'), callback=delivery_report)

        #print("==" * 50)

        print(json_data)
        producer.flush()
        time.sleep(2)

stream_reddit_data()
