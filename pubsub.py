from google.cloud import pubsub_v1
import time
import uuid


class PubSubManager:
    def __init__(self, project_id):
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

    def create_topic(self, topic_id):
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        topic = self.publisher.create_topic(request={"name": topic_path})
        print(f"Created topic: {topic.name}")
        return topic

    def delete_topic(self, topic_name):
        self.publisher.delete_topic(request={"topic": topic_name})
        print(f"Deleted topic: {topic_name}")

    def create_subscription(self, topic, subscription_id):
        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_id)
        subscription = self.subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic.name}
        )
        print(f"Subscription created: {subscription.name}")
        return subscription

    def delete_subscription(self, subscription_name):
        self.subscriber.delete_subscription(request={"subscription": subscription_name})
        print(f"Deleted subscription: {subscription_name}")

    def publish_messages(self, topic_name, messages, sleep=1, schema_id=""):
        for msg in messages:
            print("Publishing in Topic")
            message_identifier = str(uuid.uuid4())
            self.publisher.publish(topic_name, msg, schema_id=schema_id, message_identifier=message_identifier)
            time.sleep(sleep)
