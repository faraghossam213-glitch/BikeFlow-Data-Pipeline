I’m excited to share BikeFlow, my DEPI individual project: a real-time data pipeline for monitoring bike-share availability.

BikeFlow polls public station-status data, validates and routes events with Apache NiFi, streams them through Kafka, and processes them with Spark Structured Streaming. PostgreSQL serves the live station aggregates and low-stock alerts, while a Streamlit dashboard presents live and historical views. The project also writes raw and cleaned data to a partitioned Parquet lake and uses Docker Compose to bring the stack together.

Building this helped me practice event-time processing, deduplication, windowed aggregations, dead-letter handling, checkpointing, and idempotent database writes.

Architecture and screenshots: [add project image]
GitHub: [add your public repository link]

Thank you to Mohamed Abdullah and DEPI for the guidance and learning experience.

#DEPI #DataEngineering #Streaming #ApacheKafka #ApacheSpark #ApacheNiFi
