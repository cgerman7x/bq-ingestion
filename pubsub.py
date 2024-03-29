from google.cloud import pubsub_v1
import time
import datetime
import calendar
import uuid
import logging

logging.basicConfig()

class PubSubManager:
    def __init__(self, project_id):
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def create_topic(self, topic_id):
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        topic = self.publisher.create_topic(request={"name": topic_path})
        self.logger.info(f"Created topic: {topic.name}")
        return topic

    def delete_topic(self, topic_name):
        self.publisher.delete_topic(request={"topic": topic_name})
        self.logger.info(f"Deleted topic: {topic_name}")

    def create_subscription(self, topic, subscription_id):
        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_id)
        subscription = self.subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic.name, "retain_acked_messages": True}
        )
        self.logger.info(f"Subscription created: {subscription.name}")
        return subscription

    def delete_subscription(self, subscription_name):
        self.subscriber.delete_subscription(request={"subscription": subscription_name})
        self.logger.info(f"Deleted subscription: {subscription_name}")

    def publish_messages(self, topic_name, messages, sleep=1, schema_id=""):
        for msg in messages:
            ext_message_id = str(uuid.uuid4())
            date = datetime.datetime.utcnow() - datetime.timedelta(minutes=0)
            utc_time = calendar.timegm(date.utctimetuple())
            ext_message_time = str(utc_time)

            future = self.publisher.publish(topic_name,
                                            msg,
                                            ext_message_id=ext_message_id,
                                            ext_message_time = ext_message_time,
                                            schema_id=schema_id)
            message_id = future.result()
            self.logger.info(
                f"Publishing message_id={message_id}, ext_message_id={ext_message_id}, with ext_message_time={date} and schema_id={schema_id}")
            time.sleep(sleep)
