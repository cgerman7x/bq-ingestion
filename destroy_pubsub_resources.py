import pubsub

# Clean up
PubSubManager = pubsub.PubSubManager("operating-day-317714")
PubSubManager.delete_subscription("projects/operating-day-317714/subscriptions/target-subscription")
PubSubManager.delete_topic("projects/operating-day-317714/topics/source-topic")
