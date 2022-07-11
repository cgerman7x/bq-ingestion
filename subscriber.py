import random

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window, WithKeys, GroupByKey, PTransform, ParDo, DoFn
from random import randint

target_subscription = "projects/operating-day-317714/subscriptions/target-subscription"

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)

window_size = 2
num_shards = 4


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

    def process(self, elem, window=DoFn.WindowParam):
        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        shard_id, batch = elem
        filename = "-".join(["output", window_start, window_end, str(shard_id), ".avro"])
        # TO DO - checking of schema_id
        with open(filename, "wb") as f:
            for message in batch:
                f.write(message.data)


pubsub_pipeline = (
        p
        | 'Read from pubsub topic' >> beam.io.ReadFromPubSub(subscription=target_subscription, with_attributes=True)
        # | beam.Map(print)
         | 'GroupMessagesByFixedWindows' >> GroupMessagesByFixedWindows(window_size, num_shards)
        # | beam.Map(print)
         | 'WriteToFile' >> ParDo(WriteToFile())
)

result = p.run()
result.wait_until_finish()
