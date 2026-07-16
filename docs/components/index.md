# Components

SkyFollower is split into small, independently deployable containers — each
one described in its own page below. See [Deployment](/deployment/) for how
these map onto hosts and Docker Compose files.

- [Receiver](/components/receiver) — connects to readsb, tags and routes raw ADS-B messages to RabbitMQ
- [Processor](/components/processor) — flight state, enrichment, rules engine, MQTT notifications
- [Archive Processor](/components/archive-processor) — writes completed flights to S3 with a local Parquet index

Data runners (registration, operator, and airport data sources) have their
own section — see [Data Runners](/data-runners/). The [Shared Data
Models](/components/shared) library used by every component above is listed
under Reference in the sidebar.
