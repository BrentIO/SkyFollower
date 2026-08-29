import { defineConfig } from "vitepress";
import { discoverComponents, discoverRunners, discoverTools } from "../scripts/discover.mjs";

const components = discoverComponents();
const runners = discoverRunners();
const tools = discoverTools();

// `shared` is a library, not a deployable component — it keeps its
// /components/shared URL but is listed under "Reference" in the sidebar.
const deployableComponents = components.filter((component) => component.name !== "shared");
const sharedComponent = components.find((component) => component.name === "shared");

export default defineConfig({
  title: "SkyFollower",
  description: "Documentation for the SkyFollower ADS-B tracking system",
  base: "/SkyFollower/",

  vue: {
    template: {
      compilerOptions: {
        // @asyncapi/web-component registers this as a native custom element
        // at runtime (see docs/specs/AsyncApiViewer.vue) — tell Vue's
        // compiler not to try to resolve it as a component.
        isCustomElement: (tag) => tag === "asyncapi-component",
      },
    },
  },

  themeConfig: {
    search: {
      provider: "local",
    },

    nav: [
      { text: "Getting Started", link: "/getting-started/" },
      { text: "Deployment", link: "/deployment/" },
      { text: "AWS Setup", link: "/aws-setup" },
      { text: "Architecture", link: "/architecture/" },
      { text: "Rules & Areas", link: "/rules-and-areas/" },
      { text: "Components", link: "/components/" },
      { text: "Data Runners", link: "/runners/" },
      { text: "Tools", link: "/tools/" },
    ],

    sidebar: [
      {
        text: "Guide",
        items: [
          { text: "Getting Started", link: "/getting-started/" },
          { text: "Deployment", link: "/deployment/" },
          { text: "AWS Setup", link: "/aws-setup" },
          { text: "Architecture", link: "/architecture/" },
          { text: "Timing & Cadences", link: "/architecture/timing" },
          { text: "Rules & Areas", link: "/rules-and-areas/" },
        ],
      },
      {
        text: "Components",
        items: [
          { text: "Overview", link: "/components/" },
          ...deployableComponents.map((component) => ({
            text: component.title,
            link: `/components/${component.name}`,
          })),
        ],
      },
      {
        text: "Data Runners",
        collapsed: true,
        items: [
          { text: "Overview", link: "/runners/" },
          ...runners.map((runner) => ({
            text: runner.title,
            link: `/runners/${runner.name}`,
          })),
        ],
      },
      {
        text: "Tools",
        collapsed: true,
        items: [
          { text: "Overview", link: "/tools/" },
          ...tools.map((tool) => ({
            text: tool.title,
            link: `/tools/${tool.name}`,
          })),
        ],
      },
      {
        text: "Reference",
        items: [
          {
            text: "Specs",
            items: [
              { text: "MQTT Reference", link: "/specs/asyncapi" },
              { text: "OpenAPI", link: "/specs/openapi" },
            ],
          },
          { text: sharedComponent.title, link: `/components/${sharedComponent.name}` },
        ],
      },
    ],

    socialLinks: [
      { icon: "github", link: "https://github.com/BrentIO/SkyFollower" },
    ],
  },
});
