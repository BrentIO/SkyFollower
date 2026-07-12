import { defineConfig } from "vitepress";

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
        text: "Reference",
        items: [
          { text: "Components", link: "/components/" },
          { text: "Specs", link: "/specs/" },
        ],
      },
    ],

    socialLinks: [
      { icon: "github", link: "https://github.com/BrentIO/SkyFollower" },
    ],
  },
});
