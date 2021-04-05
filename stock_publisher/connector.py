import requests
import logging
import time
import os
from configparser import ConfigParser
from google.cloud import pubsub_v1
import json

# setting logger
logger = logging.getLogger("stock streaming logger")
start_write_all = time.time()

# instantiate publisher client
publisher = pubsub_v1.PublisherClient()

# instantiate config Parser
config = ConfigParser()
config.read("./config.cnf")

# fetch project and topic details
PROJECT_ID = config.get("pubsub_setup_config", "PROJECT_ID")
TOPIC = config.get("pubsub_setup_config", "TOPIC_NAME")
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)


def publish(message):
    """
    This method publishes the response from stock rest endpoint
    :param message: Json
    :return: response from publish client
    """
    str_body = json.dumps(message).encode("utf-8")
    #data = str_body.encode("utf8")
    return publisher.publish(topic_path, data=str_body)


def callback(message_future):
    """
    This method is a callback from response of pubsub topic
    :param message_future:
    """
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=10):
        logger.error('Publishing message on {} threw an Exception {}.'.format(
            TOPIC, message_future.exception()))
    else:
        logger.info("Message result {}".format(message_future.result()))


def stream_data(url):
    """
    This method is a continuous running process, polling the stock-market rest api every 10 secs
    and publishes to PubSub topic
    :param url: The stock-market rest endpoint
    """
    logger.info("Streaming data started...")
    while True:
            response = requests.get(url)
            if response.status_code == 200:
                message = response.json()
                message_future = publish(message)
                message_future.add_done_callback(callback)
            else:
                logger.error("Error in calling API, received response status code "+response.status_code)
            time.sleep(10)
    logger.info("Streaming data ended...")

# Starting point of the application
if __name__ == '__main__':
    #forming url for rest api for different companies configured
    stock_url = config.get("stock_rest_setup_config", "STOCK_URL")
    companies = config.get("stock_rest_setup_config", "ALLOWED_COMPANY")
    url = stock_url.format(companies)
    stream_data(url)