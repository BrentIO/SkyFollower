import { defineConfig } from "vitepress";
import { discoverComponents, discoverRunners } from "../scripts/discover.mjs";

const components = discoverComponents();
const runners = discoverRunners();

// `shared` is a library, not a deployable component — it keeps its
// /components/shared URL but is listed under "Reference" in the sidebar.
const deployableComponents = components.filter((component) => component.name !== "shared");
const sharedComponent = components.find((component) => component.name === "shared");

export default defineConfig({
  title: "SkyFollower",
  description: "Documentation for the SkyFollower ADS-B tracking system",
  base: "/SkyFollower/",

  themeConfig: {
    nav: [
      { text: "Getting Started", link: "/getting-started/" },
      { text: "Deployment", link: "/deployment/" },
      { text: "Architecture", link: "/architecture/" },
      { text: "Components", link: "/components/" },
      { text: "Data Runners", link: "/data-runners/" },
      { text: "Specs", link: "/specs/" },
    ],

    sidebar: [
      {
        text: "Guide",
        items: [
          { text: "Getting Started", link: "/getting-started/" },
          { text: "Deployment", link: "/deployment/" },
          { text: "Architecture", link: "/architecture/" },
        ],
      },
      {
        text: "Components",
        items: [
          { text: "Overview", link: "/components/" },
          ...deployableComponents.map((component) => ({
            text: component.sidebarLabel,
            link: `/components/${component.name}`,
          })),
        ],
      },
      {
        text: "Data Runners",
        collapsed: true,
        items: [
          { text: "Overview", link: "/data-runners/" },
          ...runners.map((runner) => ({
            text: runner.title,
            link: `/data-runners/${runner.name}`,
          })),
        ],
      },
      {
        text: "Reference",
        items: [
          { text: "Specs", link: "/specs/" },
          { text: sharedComponent.sidebarLabel, link: `/components/${sharedComponent.name}` },
        ],
      },
    ],

    socialLinks: [
      { icon: "github", link: "https://github.com/BrentIO/SkyFollower" },
    ],
  },
});
