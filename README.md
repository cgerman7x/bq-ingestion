# bq-ingestion
Test of BigQuery ingestion using PubSub messages with different AVRO schemas

<h3>Install python packages</h3>
pip install -r requirements.txt

<h3>Local PubSub emulator</h3>
gcloud beta emulators pubsub start --project=operating-day-317714

<h3>Environmental variables for local PubSub emulator</h3>
$(gcloud beta emulators pubsub env-init)
