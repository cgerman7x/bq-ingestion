import random
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window, WithKeys, GroupByKey, PTransform, ParDo, DoFn, io as gcpio
from apache_beam.utils.timestamp import Duration
from fastavro import parse_schema, writer, reader
import json
from io import BytesIO
import os
from datetime import datetime, timezone
import logging

logging.basicConfig(level=logging.INFO)


class GroupMessagesByFixedWindows(PTransform):
    def __init__(self, window_size, num_shards, allowed_lateness):
        self.window_size = int(window_size)
        self.num_shards = num_shards
        self.allowed_lateness = allowed_lateness

    def expand(self, pcoll):
        return (
                pcoll
                | 'With timestamp' >> beam.Map(lambda row: beam.window.TimestampedValue(row, int(row.attributes['ext_message_time'])))
                | 'Create window' >> beam.WindowInto(window.FixedWindows(self.window_size),
                                                     allowed_lateness=Duration(seconds=self.allowed_lateness))
                | 'Create key' >> WithKeys(lambda row: random.randint(0, self.num_shards))
                | 'Group by key' >> GroupByKey()
        )


class GetTimestampFn(beam.DoFn):
    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        timestamp_utc = datetime.utcfromtimestamp(float(timestamp))
        logging.info("Pub/Sub publish time: %s", timestamp_utc.strftime("%Y-%m-%d %H:%M:%S"))
        yield element


class WriteToFile(DoFn):
    def __init__(self, valid_schemas, parsed_schemas):
        self.valid_schemas = valid_schemas
        self.parsed_schemas = parsed_schemas

    @staticmethod
    def decode_message(parsed_schema, message):
        bytes_reader = BytesIO(message.data)
        record = reader(bytes_reader, parsed_schema)
        for rec in record:
            return rec

    @staticmethod
    def get_writer_schema(message):
        try:
            bytes_reader = BytesIO(message.data)
            record = reader(bytes_reader)
            writer_schema = record.writer_schema
        except:
            writer_schema = None

        return writer_schema

    @staticmethod
    def create_directory(relative_path):
        if not os.path.exists(f"./{relative_path}"):
            os.makedirs(f"./{relative_path}")

        return relative_path

    @staticmethod
    def write_avro_file(schema_id, avro_messages, parsed_schema, shard_id, window):
        ts_format = "%Y%m%d_%H%M%S"
        ts_format_with_ms = "%Y%m%d_%H%M%S_%f"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        technical_date = datetime.now(timezone.utc)
        point_in_time = technical_date.strftime(ts_format_with_ms)

        if len(avro_messages) > 0:
            # Create hive's style directory name if working local
            dt = f"dt={technical_date.year}-{str(technical_date.month).zfill(2)}-{str(technical_date.day).zfill(2)}"
            schema_folder = f"{schema_id}"
            prefix_folder = "transactions"
            relative_path = os.path.join(prefix_folder, dt, schema_folder)
            full_path = WriteToFile.create_directory(relative_path)

            filename = "-".join(["messages", window_start, window_end, point_in_time, str(shard_id)]) + ".avro"
            full_path_file_name = os.path.join(full_path, filename)

            if not os.path.exists(full_path_file_name):
                with open(full_path_file_name, "wb") as f:
                    writer(f, parsed_schema, avro_messages)
            else:
                logging.error(f"The file {full_path_file_name} already exist")

    def process(self, elem, window=DoFn.WindowParam):
        # Get shard id and batch of messages
        shard_id, batch = elem
        avro_messages = {}

        # We need to decode each message according to the AVRO schema specified by the pub/sub attribute
        for message in batch:
            message_id = message.message_id
            ext_message_id = message.attributes["ext_message_id"]
            schema_id = message.attributes["schema_id"]

            logging.info(f'Shard {shard_id} received message_id={message_id}, ext_message_id={ext_message_id} '  
                         f'and schema_id={schema_id if len(schema_id) > 0 else "no_schema"}')
            # We only consume known schemas
            if schema_id in self.valid_schemas:
                try:
                    message_decoded = WriteToFile.decode_message(self.parsed_schemas[schema_id],
                                                                 message)
                    # If the message was decoded fine into a json, we encode it again and queue it for later writing
                    if message_decoded:
                        avro_messages.setdefault(schema_id, [])
                        avro_messages[f'{schema_id}'].append(message_decoded)
                except Exception as e:
                    logging.error(f'Unable to parse message {message} with schema_id attribute {schema_id}')
            elif len(schema_id) > 0:
                logging.warning(f'Received a message with an unknown schema_id attribute {schema_id}')
                writer_schema = WriteToFile.get_writer_schema(message)

                if writer_schema:
                    self.parsed_schemas.setdefault(schema_id, writer_schema)

                    try:
                        message_decoded = WriteToFile.decode_message(self.parsed_schemas[schema_id],
                                                                     message)
                        if message_decoded:
                            avro_messages.setdefault(schema_id, [])
                            avro_messages[f'{schema_id}'].append(message_decoded)
                    except Exception as e:
                        logging.error(f'Unable to parse message {message} with unknown schema_id attribute {schema_id}')
                else:
                    logging.error(f'Unable to get schema from message {message} with unknown schema_id attribute {schema_id}')
            else:
                logging.error('Received a message without schema_id attribute')

        # Write avro files
        for sch_id, messages_decoded in avro_messages.items():
            WriteToFile.write_avro_file(sch_id, messages_decoded, self.parsed_schemas[sch_id], shard_id, window)


def main():
    target_subscription = "projects/operating-day-317714/subscriptions/target-subscription"

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    p = beam.Pipeline(options=options)

    # 1 minute window
    window_size = 60
    # Shards start with 0
    num_shards = 3
    # Only allow events up to 5 minutes behind the watermark
    allowed_lateness = 60*5
    # We only know these schemas
    valid_schemas = ["schemaV1", "schemaV2"]

    parsed_schemas = {}

    for schema_id in valid_schemas:
        with open(f'schemas/{schema_id}.avsc', 'rb') as f:
            avro_schema = json.loads(f.read())
            parsed_schemas.setdefault(schema_id, parse_schema(avro_schema))

    pubsub_pipeline = (
            p
            | 'Read from Pub/Sub topic' >> beam.io.ReadFromPubSub(subscription=target_subscription,
                                                                 with_attributes=True)
            | 'Print Pub/Sub publish time' >> ParDo(GetTimestampFn())
            | 'GroupMessagesByFixedWindows' >> GroupMessagesByFixedWindows(window_size=window_size,
                                                                           num_shards=num_shards,
                                                                           allowed_lateness=allowed_lateness)
            | 'WriteToFile' >> ParDo(WriteToFile(valid_schemas, parsed_schemas))
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    main()