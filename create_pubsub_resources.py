import pubsub

# Set up
PubSubManager = pubsub.PubSubManager("operating-day-317714")
source_topic = PubSubManager.create_topic("source-topic")
target_subscription = PubSubManager.create_subscription(source_topic, "target-subscription")
