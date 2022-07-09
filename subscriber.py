from google.cloud import pubsub_v1
import time

subscription_path = "projects/operating-day-317714/subscriptions/target-subscription"
subscriber = pubsub_v1.SubscriberClient()

def callback(message):
    print("--------------------------------------------")
    print(f"Received Message with schemaId: {message.attributes['schemaId']}")
    print(f"Payload: {message.data}")

    message.ack()

subscriber.subscribe(subscription_path, callback=callback)

while True:
    time.sleep(1)
