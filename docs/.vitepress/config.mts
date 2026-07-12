import { defineConfig } from "vitepress";
import { discoverComponents, discoverRunners } from "../scripts/discover.mjs";

const components = discoverComponents();
const runners = discoverRunners();

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
          ...components.map((component) => ({
            text: component.title,
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
        items: [{ text: "Specs", link: "/specs/" }],
      },
    ],

    socialLinks: [
      { icon: "github", link: "https://github.com/BrentIO/SkyFollower" },
    ],
  },
});
