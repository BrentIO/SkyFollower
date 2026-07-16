<script setup lang="ts">
import { onMounted, watch } from "vue";
import { useData, withBase } from "vitepress";

const { isDark } = useData();

// The web component renders into its own shadow root, so VitePress's
// dark-theme CSS can't reach inside it — the component's own light-themed
// Tailwind utility classes win regardless of site theme. Overriding them
// directly in the shadow root (same approach as FireFly-Docs) is the only
// way to make it follow the site's dark mode.
const STYLE_ID = "vp-asyncapi-dark";

const darkCSS = `
  .bg-white, .bg-gray-100 { background-color: #1b1b1f !important; }
  .bg-gray-200 { background-color: #202127 !important; }
  .bg-gray-400 { background-color: #3c3c43 !important; }
  .bg-gray-600 { background-color: #3c3c43 !important; }
  .bg-gray-800 { background-color: #0e0e11 !important; }
  .bg-blue-50, .bg-blue-100 { background-color: #1e2535 !important; }
  .bg-blue-400, .bg-blue-500, .bg-blue-600, .bg-blue-700 { background-color: #2a3a5c !important; }
  .text-gray-900, .text-gray-800 { color: rgba(255,255,245,.86) !important; }
  .text-gray-700, .text-gray-600 { color: rgba(235,235,245,.60) !important; }
  .text-gray-500, .text-gray-200 { color: rgba(235,235,245,.45) !important; }
  .text-white { color: #ffffff !important; }
  .prose, .prose h1, .prose h2, .prose h3, .prose h4 { color: rgba(255,255,245,.86) !important; }
  .border-gray-400 { border-color: rgba(82,82,89,1) !important; }
`;

function applyTheme(dark: boolean) {
  const el = document.querySelector("asyncapi-component");
  if (!el?.shadowRoot) return;
  let styleEl = el.shadowRoot.getElementById(STYLE_ID) as HTMLStyleElement | null;
  if (!styleEl) {
    styleEl = document.createElement("style");
    styleEl.id = STYLE_ID;
    el.shadowRoot.appendChild(styleEl);
  }
  styleEl.textContent = dark ? darkCSS : "";
}

onMounted(async () => {
  if (!customElements.get("asyncapi-component")) {
    const script = document.createElement("script");
    script.src = withBase("/asyncapi/web-component.js");
    document.head.appendChild(script);
  }
  await customElements.whenDefined("asyncapi-component");
  // Give the component's internal React render a tick to populate the
  // shadow root before we style it.
  await new Promise((resolve) => setTimeout(resolve, 50));
  applyTheme(isDark.value);
});

watch(isDark, (dark) => applyTheme(dark));
</script>

<template>
  <asyncapi-component
    :schemaUrl="withBase('/asyncapi/asyncapi.yaml')"
    :cssImportPath="withBase('/asyncapi/default.min.css')"
  />
</template>
