from google.cloud import pubsub_v1
from fastavro import parse_schema, reader
import time
import json
from io import BytesIO

with open('schemas/schemaV1.avsc', 'rb') as f:
    avro_schema_v1 = json.loads(f.read())
    parsed_schema_v1 = parse_schema(avro_schema_v1)

with open('schemas/schemaV2.avsc', 'rb') as f:
    avro_schema_v2 = json.loads(f.read())
    parsed_schema_v2 = parse_schema(avro_schema_v2)

subscription_path = "projects/operating-day-317714/subscriptions/target-subscription"
subscriber = pubsub_v1.SubscriberClient()


def callback(message):
    print("----------------------------------------------------------------------------------------")
    print(f"Received message with schemaId: {message.attributes['schema_id']}")
    print(f"message_identifier: {message.attributes['message_identifier']}")
    print(f"data: {message.data}")

    if message.attributes['schema_id'] == "schemaV1":
        bytes_reader = BytesIO(message.data)
        record = reader(bytes_reader, parsed_schema_v1)
        for rec in record:
            print(rec)
    elif message.attributes['schema_id'] == "schemaV2":
        bytes_reader = BytesIO(message.data)
        record = reader(bytes_reader, parsed_schema_v2)
        for rec in record:
            print(rec)
    else:
        print("Unknown schema for this message")

    message.ack()


subscriber.subscribe(subscription_path, callback=callback)

while True:
    time.sleep(1)
