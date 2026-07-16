<script setup>
import AsyncApiViewer from "./AsyncApiViewer.vue";
</script>

# AsyncAPI

Every MQTT topic published by the receiver, processor, archive processor, and
rule notifications — rendered interactively from
[`specs/asyncapi.yaml`](https://github.com/BrentIO/SkyFollower/blob/main/specs/asyncapi.yaml).

<ClientOnly>
  <AsyncApiViewer />
</ClientOnly>
