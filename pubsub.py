from google.cloud import pubsub_v1
import time


class PubSubManager:
    def __init__(self, project_id):
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()

    def create_topic(self, topic_id):
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        topic = self.publisher.create_topic(request={"name": topic_path})
        print(f"Created topic: {topic.name}")
        return topic

    def delete_topic(self, topic):
        self.publisher.delete_topic(request={"topic": topic})
        print(f"Deleted topic: {topic}")

    def create_subscription(self, topic_path, subscription_id):
        subscriber = pubsub_v1.SubscriberClient()

        with subscriber:
            subscription_path = subscriber.subscription_path(self.project_id, subscription_id)
            subscription = subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            )
            print(f"Subscription created: {subscription.name}")
            return subscription

    def publish_messages(self, topic, messages, sleep=1):
        for msg in messages:
            print("Publishing in Topic")
            self.publisher.publish(topic.name, msg)
            time.sleep(sleep)
