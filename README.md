# bq-ingestion
POC about BigQuery ingestion that generates messages using the local pub/sub emulator that are consumed by an Apache Beam job that writes output AVRO files in different folders based on their AVRO schema.

It creates a pub/sub topic and subscription as first step. Then, all pub/sub messages are created with a <b>schema_id</b> attribute that specified the schema version used to generate the AVRO encoded payload. 
The important concept here is that the schema is always embedded in the message and in the output AVRO file. 

Custom ext_message_id and ext_message_time are also attached as pub/sub attributes.

The publisher produces several messages with:
<ol>
    <li>A valid schema_id attribute that the subscriber knows</li>
    <li>An unknown schema_id attribute</li>
    <li>An invalid payload and no schema_id attribute</li>
    <li>An invalid payload and a valid schema_id attribute</li>
</ol>

The subscriber is an Apache Beam job that processes messages:
<ul>
    <li>For cases 1 and 2 it processes business as usual because it already knows that they are known schemas</li>
    <li>For case 3, it uses the <b>writer schema</b> to consume these messages simulating a new schema_id being deployed in the source system</li>
    <li>For case 4, it generates an error message</li>
</ul>

<h1>Install python packages</h1>

```
pip install -r requirements.txt
```

<h1>Running the local environment</h1>
<h2>Local Pub/Sub emulator installation</h2>
Install the local pub/sub emulator following the instructions: https://cloud.google.com/pubsub/docs/emulator

<h2>Start the local pub/sub emulator</h2>
You can start it with a fake project_id as follows:

```
gcloud beta emulators pubsub start --project=operating-day-317714
```

<h2>Environmental variables for local pub/sub emulator</h2>

You need to set up the environmental variables related to pub/sub using the same fake project id from above on each terminal where you plan to launch the below python scripts:

<h3>Windows Command Line</h3>

```
set PUBSUB_PROJECT_ID="operating-day-317714"
set PUBSUB_EMULATOR_HOST="localhost:8085"
```

<h3>Windows Power Shell</h3>

Please, do not forget the $ at the beginning! :-)

```
$env:PUBSUB_PROJECT_ID = "operating-day-317714"
$env:PUBSUB_EMULATOR_HOST = "localhost:8085"
```

<h3>Linux</h3>

```
$(gcloud beta emulators pubsub env-init)
```

<h2>Create the topic and the subscription</h2>

```
python create_pubsub_resources.py
```

<h2>Start the Subscriber (Apache Beam job with DirectRunner)</h2>

```
python subscriber.py
```

<h2>Start the Publisher</h2>

```
python publisher.py
```

<h2>Create tables in BigQuery</h2>

```
CREATE TABLE `<project_id>.<dataset_name>.schemaV1`
(
  name STRING NOT NULL,
  middleName STRING,
  lastName STRING NOT NULL,
  age INT64,
  dt DATE
)
PARTITION BY dt;

CREATE TABLE `<project_id>.<dataset_name>.schemaV2`
(
  name STRING NOT NULL,
  middleName STRING,
  lastName STRING NOT NULL,
  age INT64,
  address STRING NOT NULL,
  dt DATE
)
PARTITION BY dt;
```

<h2>Ingest data into BigQuery using bq command</h2>
Once the files are created, you can ingest them into BigQuery with the following command:

```
bq load --source_format=AVRO --hive_partitioning_mode=CUSTOM --hive_partitioning_source_uri_prefix="gs://<bucket_name>/transactions/{dt:DATE}" <dataset_name>.<table_name> gs://<bucket_name>/transactions/dt=<date>/<schemaId>/*
```

<h2>Testing late events arrival</h2>
The window is set to 60 seconds and the allowed_lateness is set to 5 minutes, anything older is discarded by Apache Beam. 

If you want to test messages published with an old timestamp you can adjust
inside the publish_messages method in pubsub.py how old they should be.

<h1>Useful links</h1>

<h2>About AVRO specification and implementations</h2>
<a href="https://avro.apache.org/docs/1.11.0/gettingstartedpython.html">Apache AVRO</a>

<a href="https://fastavro.readthedocs.io/en/latest/#">fastavro</a>

<h2>About streaming</h2>
<a href="https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/">Streaming 101: The world beyond batch</a>

<a href="https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/">Streaming 102: The world beyond batch</a>

<h2>About Apache Beam</h2>
<a href="https://beam.apache.org/documentation/programming-guide/">Apache Beam Programming Guide</a>

<a href="https://github.com/apache/beam/tree/master/sdks/python/apache_beam">Apache Beam Python SDK</a>

<a href="https://beam.apache.org/blog/test-stream/">Testing Unbounded Pipelines in Apache Beam</a>

<h2>About Pub/Sub</h2>
<a href="https://cloud.google.com/pubsub/docs/emulator">Testing apps locally with the emulator</a>

<a href="https://googleapis.dev/python/pubsub/latest/index.html">Python Client for Google Cloud Pub/Sub</a>

<a href="https://cloud.google.com/pubsub/docs/stream-messages-dataflow">Stream messages from Pub/Sub by using Dataflow</a>

<h2>About BigQuery</h2>
<a href="https://cloud.google.com/bigquery/docs/hive-partitioned-loads-gcs#bq">Loading externally partitioned data</a>