# ⚠️ In Development ⚠️

This repo is in active development.  Its full of bugs, incomplete implementation, invalid documentation, and just generally isn't ready for you to fork or test.  Star the repo and check back in a few weeks!

# SkyFollower

SkyFollower is a locally-hosted ADS-B aircraft tracking system. It receives
raw 1090 MHz and 978 MHz UAT messages from one or more readsb decoders, routes
them through RabbitMQ for processing, enriches each flight with registration and
operator data from Redis, evaluates configurable alert rules, archives completed
flights as gzipped JSON to AWS S3, and publishes real-time metrics and rule
notifications over MQTT (with Home Assistant autodiscovery).

**Full documentation: [brentio.github.io/SkyFollower](https://brentio.github.io/SkyFollower/)**

- [Getting Started](https://brentio.github.io/SkyFollower/getting-started/) — bring up each host and do a first-time data load
- [Deployment](https://brentio.github.io/SkyFollower/deployment/) — host topology, compose files, components, and configuration reference
- [Architecture](https://brentio.github.io/SkyFollower/architecture/) — pipeline diagram plus scaling processors horizontally
- [Rules & Areas](https://brentio.github.io/SkyFollower/rules-and-areas/) — condition types, example `rules.json`/`areas.json`
- [Data Runners](https://brentio.github.io/SkyFollower/data-runners/) — settings fields for every registration/airport data source
- [MQTT Reference](https://brentio.github.io/SkyFollower/specs/asyncapi) — every topic published, including Home Assistant autodiscovery behavior

## Development

Run the test suite:

```bash
python -m pytest
```

Tests live under `processor/tests/`, `receiver/tests/`, and `shared/tests/`.
