<script setup>
import AsyncApiViewer from "./AsyncApiViewer.vue";
</script>

# MQTT Reference

Every MQTT topic published by the receiver, processor, archive processor, and
rule notifications — rendered interactively from
[`specs/asyncapi.yaml`](https://github.com/BrentIO/SkyFollower/blob/main/specs/asyncapi.yaml).

::: tip Home Assistant
When MQTT is configured, every component publishes Home Assistant
autodiscovery payloads on connect — each processor, receiver, archive
processor, and data runner appears as a device in Home Assistant with sensor
entities for its key metrics. Rule notifications (`SkyFollower/rule/{identifier}`)
carry the flight's current state as JSON and can be consumed by Home
Assistant automations directly.
:::

<ClientOnly>
  <AsyncApiViewer />
</ClientOnly>
