# NiFi setup

The assignment asks for a downloaded NiFi flow definition at `nifi/flow.json`. NiFi flow-definition JSON is exported by NiFi itself and includes component IDs and version-specific metadata, so do not substitute a hand-written file that NiFi may reject.

1. Open https://localhost:8082/nifi and create a Process Group named `BikeFlow`.
2. Add `ListenHTTP` on port `8090` with base path `/contentListener`. The replayer reaches it at `http://nifi:8090/contentListener` inside the Compose network; no host port mapping is needed.
3. From the incoming FlowFile, use `ValidateRecord` with a JSON Reader and schema in `schema/bikeflow-event.json`. Send valid records to `PublishKafka` (`bikeflow.raw`, key attribute `station_id`) and invalid records to `bikeflow.dlq`. Use PLAINTEXT security and `kafka:9092`.
4. Add `UpdateRecord` for `ingest_time` and `source`, and set provenance/retention for the queues. Start the processors and confirm raw events appear in Kafka.
5. Right-click the `BikeFlow` group, choose **Download flow definition**, and save the downloaded JSON here as `flow.json`.

The assignment handout targets NiFi 2.12.0. The downloaded flow file is deliberately left for export from that running version rather than shipping a guessed serialization.
