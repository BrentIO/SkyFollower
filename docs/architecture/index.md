# Architecture

The full message pipeline, from ADS-B reception through to archived
flights in S3, including the RabbitMQ offline-fallback path at each hop.

![SkyFollower message pipeline](./images/pipeline.svg)
