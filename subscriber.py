import random
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window, WithKeys, GroupByKey, PTransform, ParDo, DoFn
from fastavro import parse_schema, writer, reader
import json
from io import BytesIO
import os
from datetime import datetime, timezone
import logging

logging.basicConfig(level=logging.INFO)


class GroupMessagesByFixedWindows(PTransform):
    def __init__(self, window_size, num_shards):
        self.window_size = int(window_size)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
                pcoll
                | 'With timestamp' >> beam.Map(lambda row: beam.window.TimestampedValue(row, int(row.attributes['message_time'])))
                | 'Create window' >> beam.WindowInto(window.FixedWindows(self.window_size))
                | 'Create key' >> WithKeys(lambda row: random.randint(0, self.num_shards))
                | 'Group by key' >> GroupByKey()
        )


class WriteToFile(DoFn):
    def __init__(self, valid_schemas, parsed_schemas, cloud_storage_bucket):
        self.valid_schemas = valid_schemas
        self.parsed_schemas = parsed_schemas
        self.cloud_storage_bucket = cloud_storage_bucket

    def decode_message(self, schema_id, parsed_schema, message):
        bytes_reader = BytesIO(message.data)
        record = reader(bytes_reader, parsed_schema)
        for rec in record:
            return rec;

    def create_directory(self, technical_date, schema_id):
        dt = f"dt={technical_date.year}-{str(technical_date.month).zfill(2)}-{str(technical_date.day).zfill(2)}"
        schema_folder = f"{schema_id}"
        prefix_folder = "transactions"
        relative_path = os.path.join(prefix_folder, dt, schema_folder)

        if not os.path.exists(f"./{relative_path}"):
            os.makedirs(f"./{relative_path}")

        return relative_path

    def write_avro_file(self, schema_id, avro_messages, parsed_schema, shard_id, window):
        ts_format = "%Y%m%d_%H%M%S"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        technical_date = datetime.now(timezone.utc)

        relative_path = ""
        if len(avro_messages) > 0:
            # Create hive's style directory name if working local
            if not self.cloud_storage_bucket:
                relative_path = self.create_directory(technical_date, schema_id)

            filename = "-".join(["messages", window_start, window_end, str(shard_id)]) + ".avro"
            full_path = os.path.join(relative_path, filename)
            with open(full_path, "wb") as f:
                writer(f, parsed_schema, avro_messages)

    def process(self, elem, window=DoFn.WindowParam):
        # Get shard id and batch of messages
        shard_id, batch = elem
        avro_messages = {}

        # We need to decode each message according to the AVRO schema specified by the pub/sub attribute
        for message in batch:
            schema_id = message.attributes["schema_id"]

            logging.info(f'Shard {shard_id} received {schema_id if len(schema_id) > 0 else "no_schema"}')
            # We only consume known schemas
            if schema_id in self.valid_schemas:
                try:
                    message_decoded = self.decode_message(schema_id,
                                                          self.parsed_schemas[schema_id],
                                                          message)
                    # If the message was decoded fine into a json, we encode it again and queue it for later writing
                    if message_decoded:
                        avro_messages.setdefault(schema_id, [])
                        avro_messages[f'{schema_id}'].append(message_decoded)
                except Exception as e:
                    logging.error(f'Unable to parse message {message} with schema_id attribute {schema_id}')
            elif len(schema_id) > 0:
                logging.error('Received a message with an unknown schema_id attribute')
            else:
                logging.error('Received a message without schema_id attribute')

        # Write avro files
        for sch_id, encoded_messages in avro_messages.items():
            self.write_avro_file(sch_id, encoded_messages, self.parsed_schemas[sch_id], shard_id, window)


def main():
    target_subscription = "projects/operating-day-317714/subscriptions/target-subscription"
    cloud_storage_bucket = False

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    p = beam.Pipeline(options=options)

    window_size = 10
    num_shards = 3
    valid_schemas = ["schemaV1", "schemaV2"]
    parsed_schemas = {}

    for schema_id in valid_schemas:
        with open(f'schemas/{schema_id}.avsc', 'rb') as f:
            avro_schema = json.loads(f.read())
            parsed_schemas.setdefault(schema_id, parse_schema(avro_schema))

    pubsub_pipeline = (
            p
            | 'Read from pubsub topic' >> beam.io.ReadFromPubSub(subscription=target_subscription, with_attributes=True)
            | 'GroupMessagesByFixedWindows' >> GroupMessagesByFixedWindows(window_size=window_size, num_shards=num_shards)
            | 'WriteToFile' >> ParDo(WriteToFile(valid_schemas, parsed_schemas, cloud_storage_bucket))
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    main()