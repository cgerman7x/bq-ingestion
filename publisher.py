from fastavro.utils import generate_many
from fastavro import parse_schema, writer
from io import BytesIO
import json
import pubsub
from multiprocessing import Process


def generate_fake_data(parsed_schema, amount=1):
    return list(generate_many(parsed_schema, amount))

def encode_message(parsed_schema, message):
    bytes_writer = BytesIO()
    writer(bytes_writer, parsed_schema, [message])
    return bytes_writer.getvalue()


def main():
    # Open avro schema file and load it as JSON
    project_id = "operating-day-317714"
    topic = "projects/operating-day-317714/topics/source-topic"

    # Generate valid messages for schemaV1 -> they will be processed fine
    avro_messages_v1 = []
    with open('schemas/schemaV1.avsc', 'rb') as f:
        avro_schema = json.loads(f.read())
        parsed_schema = parse_schema(avro_schema)

        fake_data = generate_fake_data(parsed_schema, 10)

        for msg in fake_data:
            avro_messages_v1.append(encode_message(parsed_schema, msg))

    # Generate valid messages for schemaV2 -> they will be processed fine
    avro_messages_v2 = []
    with open('schemas/schemaV2.avsc', 'rb') as f:
        avro_schema = json.loads(f.read())
        parsed_schema = parse_schema(avro_schema)

        fake_data = generate_fake_data(parsed_schema, 10)

        for msg in fake_data:
            avro_messages_v2.append(encode_message(parsed_schema, msg))

    # Generate valid messages for schemaV3 -> they won't be processed because the schema is not whitelisted
    avro_messages_v3 = []
    with open('schemas/schemaV3.avsc', 'rb') as f:
        avro_schema = json.loads(f.read())
        parsed_schema = parse_schema(avro_schema)

        fake_data = generate_fake_data(parsed_schema, 10)

        for msg in fake_data:
            avro_messages_v3.append(encode_message(parsed_schema, msg))

    # Generate invalid message without schema_id attribute -> they won't be processed
    avro_messages_no_version = [b'invalid message & no schema_id']

    # Generate invalid message for schemaV1 -> they won't be processed because error during the decoding phase
    avro_messages_invalid_msg = [b'invalid message & valid schema_id']

    pubsub_manager = pubsub.PubSubManager(project_id)
    p1 = Process(target=pubsub_manager.publish_messages(topic,
                                                        avro_messages_v1,
                                                        sleep=0.1,
                                                        schema_id="schemaV1"))
    p1.start()
    p2 = Process(target=pubsub_manager.publish_messages(topic,
                                                        avro_messages_v2,
                                                        sleep=0.1,
                                                        schema_id="schemaV2"))
    p2.start()
    p3 = Process(target=pubsub_manager.publish_messages(topic,
                                                        avro_messages_v3,
                                                        sleep=0.1,
                                                        schema_id="schemaV3"))
    p3.start()
    p4 = Process(target=pubsub_manager.publish_messages(topic,
                                                        avro_messages_no_version,
                                                        sleep=0.1,
                                                        schema_id=""))
    p4.start()
    p5 = Process(target=pubsub_manager.publish_messages(topic,
                                                        avro_messages_invalid_msg,
                                                        sleep=0.1,
                                                        schema_id="schemaV1"))
    p5.start()
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()


if __name__ == '__main__':
    main()
