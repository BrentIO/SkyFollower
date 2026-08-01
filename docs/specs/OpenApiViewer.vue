<script setup lang="ts">
import { onMounted } from "vue";
import { SwaggerUIBundle } from "swagger-ui-dist";
import "swagger-ui-dist/swagger-ui.css";
import { withBase } from "vitepress";
import { useSwaggerDark } from "../.vitepress/theme/useSwaggerDark";

// Matches FireFly-Docs's own api_reference.vue pattern (same library, same
// dark-mode approach via useSwaggerDark) -- the house precedent this
// repo's AsyncAPI viewer already follows for @asyncapi/web-component.
useSwaggerDark();

onMounted(() => {
  SwaggerUIBundle({
    dom_id: "#swaggerContainer",
    url: withBase("/openapi/openapi.yaml"),
  });
});
</script>

<template>
  <div class="swagger" id="swaggerContainer" />
</template>
