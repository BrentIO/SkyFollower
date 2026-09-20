---
layout: home

hero:
  name: SkyFollower
  text: ADS-B aircraft tracking & alerting
  tagline: Watch aircraft on a live map, get alerted when the ones you care about fly over, and keep a searchable history — all from your own locally-hosted ADS-B receiver.
  actions:
    - theme: brand
      text: Getting Started
      link: /getting-started/
    - theme: alt
      text: View on GitHub
      link: https://github.com/BrentIO/SkyFollower

features:
  - title: Live Map
    details: Real-time aircraft positions with trails and altitude-colored icons, rendered from your own receivers.
    link: /components/map
  - title: Multi-Receiver Coverage
    details: Aggregates 1090 MHz and 978 MHz UAT feeds from one or more receivers into a single picture.
    link: /architecture/
  - title: Registration & Route Enrichment
    details: Looks up registration, operator, and route details from around 46 national aviation registries.
    link: /runners/
  - title: Rules & Alerting
    details: Configurable rules and geographic areas trigger alerts when aircraft you care about show up.
    link: /rules-and-areas/
  - title: Flight Archive
    details: Every completed flight is archived to S3, building a long-term, queryable history.
    link: /components/archive-processor
  - title: Home Assistant Integration
    details: Publishes live metrics and rule alerts over MQTT, with Home Assistant autodiscovery built in.
    link: /specs/asyncapi
---
