<script setup lang="ts">
import { onMounted, watch } from "vue";
import { useData, withBase } from "vitepress";

const { isDark } = useData();

// Unlike AsyncApiViewer's <asyncapi-component> (which renders into a
// shadow root, requiring a manual CSS-injection workaround for dark
// mode), Stoplight Elements' <elements-api> renders into the light DOM
// and ships its own [data-theme=dark]/[data-theme=light] CSS selectors --
// setting the attribute directly is enough to follow the site's theme.
function applyTheme(dark: boolean) {
  const el = document.querySelector("elements-api");
  el?.setAttribute("data-theme", dark ? "dark" : "light");
}

onMounted(async () => {
  if (!customElements.get("elements-api")) {
    const link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = withBase("/openapi/styles.min.css");
    document.head.appendChild(link);

    const script = document.createElement("script");
    script.src = withBase("/openapi/web-components.min.js");
    document.head.appendChild(script);
  }
  await customElements.whenDefined("elements-api");
  applyTheme(isDark.value);
});

watch(isDark, (dark) => applyTheme(dark));
</script>

<template>
  <!--
    .attr forces these to be set as real HTML attributes rather than DOM
    properties -- same reasoning as AsyncApiViewer.vue: the dynamically
    loaded web component reads its config via getAttribute(), and a
    property-only assignment (Vue's default for custom elements) is
    silently invisible to it.
  -->
  <elements-api
    :apiDescriptionUrl.attr="withBase('/openapi/openapi.yaml')"
    router="hash"
    layout="sidebar"
    :hideTryIt.attr="true"
  />
</template>
