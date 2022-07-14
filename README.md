# bq-ingestion
Test of BigQuery ingestion using PubSub messages with different AVRO schemas

<h2>Install python packages</h2>
pip install -r requirements.txt

<h2>Local Pub/Sub emulator</h2>
Install the local pub/sub emulator following the instructions: https://cloud.google.com/pubsub/docs/emulator

Then, you can start it with a fake project_id as follows:

gcloud beta emulators pubsub start --project=operating-day-317714

<h2>Environmental variables for local PubSub emulator</h2>

Before executing any script you need to set up the environmental variables:

<h3>Windows Command Line</h3>
set PUBSUB_PROJECT_ID=operating-day-317714<br>
set PUBSUB_EMULATOR_HOST=localhost:8085

<h3>Windows Power Shell</h3>

Please, do not forget the $ at the beginning! :-)

$env:PUBSUB_PROJECT_ID = "operating-day-317714"<br>
$env:PUBSUB_EMULATOR_HOST = "localhost:8085"

<h3>Linux</h3>
$(gcloud beta emulators pubsub env-init)
