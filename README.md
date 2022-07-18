# bq-ingestion
Quick POC of BigQuery ingestion that generates pub/sub messages that are consumed by a dataflow job that generates output AVRO files in different folders based on their AVRO schema. 

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

<h2>Start the Subscriber (Dataflow job with DirectRunner)</h2>

```
python subscriber.py
```

<h2>Start the Publisher</h2>

```
python publisher.py
```

<h2>Ingest data into BigQuery using bq command</h2>
Once the files are created, you can ingest them into BigQuery with the following command:

```
bq load --source_format=AVRO --hive_partitioning_mode=CUSTOM --hive_partitioning_source_uri_prefix="gs://<bucket_name>/transactions/{dt:DATE}" <dataset_name>.<table_name> gs://<bucket_name>/transactions/dt=<date>/<schemaId>/*
```
