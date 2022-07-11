from fastavro.utils import generate_many
from fastavro import parse_schema, writer
from io import BytesIO
import json
import pprint
import pubsub

pp = pprint.PrettyPrinter()


def generate_fake_data(parsed_schema, amount=1):
    return list(generate_many(parsed_schema, amount))


def main():
    # Open avro schema file and load it as JSON
    avro_messages_v1 = []
    with open('schemas/schemaV1.avsc', 'rb') as f:
        avro_schema = json.loads(f.read())
        parsed_schema = parse_schema(avro_schema)

        fake_data = generate_fake_data(parsed_schema, 5)

        for fm in fake_data:
            bytes_writer = BytesIO()
            writer(bytes_writer, parsed_schema, [fm])
            avro_messages_v1.append(bytes_writer.getvalue())

    avro_messages_v2 = []
    with open('schemas/schemaV2.avsc', 'rb') as f:
        avro_schema = json.loads(f.read())
        parsed_schema = parse_schema(avro_schema)

        fake_data = generate_fake_data(parsed_schema, 5)

        for fm in fake_data:
            bytes_writer = BytesIO()
            writer(bytes_writer, parsed_schema, [fm])
            avro_messages_v2.append(bytes_writer.getvalue())

    pubsub_manager = pubsub.PubSubManager("operating-day-317714")
    pubsub_manager.publish_messages("projects/operating-day-317714/topics/source-topic",
                                    avro_messages_v1,
                                    sleep=0.5,
                                    schema_id="schemaV1")
    pubsub_manager.publish_messages("projects/operating-day-317714/topics/source-topic",
                                    avro_messages_v2,
                                    sleep=0.5,
                                    schema_id="schemaV2")


if __name__ == '__main__':
    main()
