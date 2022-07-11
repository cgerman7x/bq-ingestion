from fastavro.read import SchemaResolutionError
from google.cloud import pubsub_v1
from fastavro import parse_schema, reader
import time
import datetime
import json
from io import BytesIO


def open_avro_schema_file(file_name):
    with open(file_name, 'rb') as f:
        avro_schema = json.loads(f.read())
        parsed_schema = parse_schema(avro_schema)

        return parsed_schema;


parsed_schema_v1 = open_avro_schema_file('schemas/schemaV1.avsc')
parsed_schema_v2 = open_avro_schema_file('schemas/schemaV2.avsc')

subscription_path = "projects/operating-day-317714/subscriptions/target-subscription"
subscriber = pubsub_v1.SubscriberClient()


def callback(message):
    print("----------------------------------------------------------------------------------------")
    print(f"Received message with schemaId: {message.attributes['schema_id']}")
    print(f"message_identifier: {message.attributes['message_identifier']}")
    print(f"message_time: {datetime.datetime.fromtimestamp(int(message.attributes['message_time']))}")
    print(f"data: {message.data}")

    if message.attributes['schema_id'] == "schemaV1":
        bytes_reader = BytesIO(message.data)
        try:
            record = reader(bytes_reader, parsed_schema_v1)
            for rec in record:
                print(rec)
        except SchemaResolutionError as e:
            print("Schema resolution error")
    elif message.attributes['schema_id'] == "schemaV2":
        bytes_reader = BytesIO(message.data)
        try:
            record = reader(bytes_reader, parsed_schema_v2)
            for rec in record:
                print(rec)
        except SchemaResolutionError as e:
            print("Schema resolution error")
    else:
        print("Unknown schema for this message")

    message.ack()


subscriber.subscribe(subscription_path, callback=callback)

while True:
    time.sleep(1)
