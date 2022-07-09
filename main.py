from fastavro.utils import generate_many
from fastavro import parse_schema, writer
from io import BytesIO
import json
import pprint
import pubsub

pp = pprint.PrettyPrinter()

def generate_fake_data(avro_schema, amount = 1):
    return list(generate_many(avro_schema, amount))

def main():
    PubSubManager = pubsub.PubSubManager("operating-day-317714")
    source_topic = PubSubManager.create_topic("source-topic")

    # Open avro schema file and load it as JSON
    with open('schemas/schemaV1.avsc', 'rb') as f:
        avro_schema = json.loads(f.read())
        parsed_schema = parse_schema(avro_schema)

        fake_data = generate_fake_data(parsed_schema, 5)

        bytes_writer = BytesIO()
        avro_messages = []
        for fm in fake_data:
            writer(bytes_writer, parsed_schema, [fm])
            avro_messages.append(bytes_writer.getvalue())

        PubSubManager.publish_messages(source_topic, avro_messages)

    PubSubManager.delete_topic(source_topic.name)

if __name__ == '__main__':
    main()
