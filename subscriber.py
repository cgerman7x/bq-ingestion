import random
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window, WithKeys, GroupByKey, PTransform, ParDo, DoFn
from fastavro import parse_schema, writer, reader
import json
from io import BytesIO
import os
from datetime import datetime, timezone

target_subscription = "projects/operating-day-317714/subscriptions/target-subscription"
cloud_storage_bucket = False

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)

window_size = 60
num_shards = 3

with open('schemas/schemaV1.avsc', 'rb') as f:
    avro_schema = json.loads(f.read())
    parsed_schema_v1 = parse_schema(avro_schema)

with open('schemas/schemaV2.avsc', 'rb') as f:
    avro_schema = json.loads(f.read())
    parsed_schema_v2 = parse_schema(avro_schema)


class GroupMessagesByFixedWindows(PTransform):
    def __init__(self, windows_size=1, num_shards=2):
        self.window_size = int(window_size)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
                pcoll
                | 'With timestamp' >> beam.Map(lambda row: beam.window.TimestampedValue(row, int(row.attributes['message_time'])))
                | 'Create window' >> beam.WindowInto(window.FixedWindows(window_size))
                | 'Create key' >> WithKeys(lambda row: random.randint(0, num_shards))
                | 'Group by key' >> GroupByKey()
        )


class WriteToFile(DoFn):
    def __init__(self):
        pass

    def decode_message(self, schema_id, parsed_schema, message):
        bytes_reader = BytesIO(message.data)
        record = reader(bytes_reader, parsed_schema)
        for rec in record:
            return rec;

    def create_directory(self, technical_date, schema_id):
        dt = f"dt={technical_date.year}-{str(technical_date.month).zfill(2)}-{str(technical_date.day).zfill(2)}"
        schema_folder = f"schemaId={schema_id}"
        relative_path = os.path.join(dt, schema_folder)

        if not os.path.exists(f"./{relative_path}"):
            os.makedirs(f"./{relative_path}")

        return relative_path

    def write_avro_file(self, schema_id, avro_messages, parsed_schema, shard_id, window):
        ts_format = "%Y%m%d_%H%M%S"
        d_format = "%Y%m%d"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        technical_date = datetime.now(timezone.utc)

        relative_path = ""
        if len(avro_messages) > 0:
            if not cloud_storage_bucket:
                relative_path = self.create_directory(technical_date, schema_id)

            filename = "-".join(["messages", window_start, window_end, str(shard_id)]) + ".avro"
            full_path = os.path.join(relative_path, filename)
            with open(full_path, "wb") as f:
                writer(f, parsed_schema, avro_messages)

    def process(self, elem, window=DoFn.WindowParam):
        shard_id, batch = elem
        avro_messages_v1 = []
        avro_messages_v2 = []
        for message in batch:
            print(f'Received {message.attributes["schema_id"]}')
            if message.attributes["schema_id"] == "schemaV1":
                message_decoded = self.decode_message("schemaV1", parsed_schema_v1, message)
                if message_decoded:
                    avro_messages_v1.append(message_decoded)
            elif message.attributes["schema_id"] == "schemaV2":
                message_decoded = self.decode_message("schemaV2", parsed_schema_v2, message)
                if message_decoded:
                    avro_messages_v2.append(message_decoded)

        # Write avro files if content exist
        self.write_avro_file("schemaV1", avro_messages_v1, parsed_schema_v1, shard_id, window)
        self.write_avro_file("schemaV2", avro_messages_v2, parsed_schema_v2, shard_id, window)


pubsub_pipeline = (
        p
        | 'Read from pubsub topic' >> beam.io.ReadFromPubSub(subscription=target_subscription, with_attributes=True)
        | 'GroupMessagesByFixedWindows' >> GroupMessagesByFixedWindows(window_size, num_shards)
        | 'WriteToFile' >> ParDo(WriteToFile())
)

result = p.run()
result.wait_until_finish()
