import praw
import time 
import json
from dotenv import load_dotenv
import os
from confluent_kafka import Producer




# Loading the environmental variables from the .env file
load_dotenv()

# Loading the Reddit Credentials from the .env file

reddit_client_id=os.getenv("CLIENT_ID")
reddit_secret_id=os.getenv("CLIENT_SECRET")
reddit_user_agent=os.getenv("USER_AGENT")
reddit_password=os.getenv("PASSWORD")
reddit_username=os.getenv("USERNAME")
reddit_subreddit=os.getenv("REDDIT_SUBREDDIT")


# Kafka Credentials
kafka_broker = os.getenv("KAFKA_BROKER")
kafka_topic = os.getenv("KAFKA_TOPIC")


reddit= praw.Reddit(
    client_id=reddit_client_id,
    client_secret=reddit_secret_id,
    username=reddit_username,
    password=reddit_password,
    user_agent=reddit_user_agent
)


producer= Producer({
        'bootstrap.servers': kafka_broker,
        'acks':'all',
        'retries': 4,
        'retry.backoff.ms': 100,
})

print(f'Connected to Kafka at {kafka_broker} and topic to stream :{kafka_topic}')


def delivery_report(err, msg):
    if err is not None:
        print(f' message delivery failed: {err}')
    else:
        print(f'message delivered to {msg.topic()} [{msg.partition()}]')




subreddit=reddit.subreddit(reddit_subreddit)
# subscribing to Reddit stream
for submission in subreddit.stream.submissions():
    data = {
            'id': submission.id,
            'title': submission.title,
            'author': submission.author.name,
            'score': submission.score,
            'num_comments': submission.num_comments,
            'subreddit': submission.subreddit.display_name
    }

    #convert the message to Json
    json_data = json.dumps(data, indent=4)

    # Sending data to kafka
    print('sending message to kafka')

    producer.produce(kafka_topic, value=json_data.encode('utf-8'), callback=delivery_report)

    print(json_data)
    producer.flush()
    time.sleep(2)


# calling the function

