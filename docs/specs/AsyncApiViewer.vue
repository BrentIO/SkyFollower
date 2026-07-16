<script setup lang="ts">
import { onMounted } from "vue";
import { withBase } from "vitepress";

onMounted(async () => {
  if (!customElements.get("asyncapi-component")) {
    const script = document.createElement("script");
    script.src = withBase("/asyncapi/web-component.js");
    document.head.appendChild(script);
  }
  await customElements.whenDefined("asyncapi-component");
});
</script>

<template>
  <asyncapi-component
    :schemaUrl="withBase('/asyncapi/asyncapi.yaml')"
    :cssImportPath="withBase('/asyncapi/default.min.css')"
  />
</template>
