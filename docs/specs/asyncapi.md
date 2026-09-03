<script setup>
import AsyncApiViewer from "./AsyncApiViewer.vue";
</script>

# MQTT Reference

Every MQTT topic published by the receiver, message processor, archive
processor, and rule notifications — rendered interactively from
[`specs/asyncapi.yaml`](https://github.com/BrentIO/SkyFollower/blob/main/specs/asyncapi.yaml).

::: tip Home Assistant
When MQTT is configured, every component publishes Home Assistant
autodiscovery payloads on connect — each message processor, receiver,
archive processor, and data runner appears as a device in Home Assistant
with sensor entities for its key metrics. Rule notifications (`SkyFollower/rule/{identifier}`)
carry the flight's current state as JSON and can be consumed by Home
Assistant automations directly.
:::

::: tip RabbitMQ / AMQP
This page covers MQTT only. The internal RabbitMQ exchanges and queues
that move messages between the receiver, message processor(s), and
archive processor — `skyfollower-adsb`, `skyfollower-adsb-unroutable`,
`skyfollower-archive`, and each message processor's own
`skyfollower-message-processor-{id}` queue — are documented separately in
[`specs/asyncapi-amqp.yaml`](https://github.com/BrentIO/SkyFollower/blob/main/specs/asyncapi-amqp.yaml),
kept as its own AsyncAPI document rather than a second server here.
:::

<ClientOnly>
  <AsyncApiViewer />
</ClientOnly>
