import { onMounted, watch } from "vue";
import { useData } from "vitepress";

// swagger-ui-dist has no built-in dark theme -- ported from FireFly-Docs's
// own useSwaggerDark.ts (the house pattern this repo's AsyncAPI viewer
// already follows for @asyncapi/web-component) rather than reinventing it.
const STYLE_ID = "vp-swagger-dark";

const darkCSS = `
  /* Root */
  .swagger-ui,
  .swagger-ui .wrapper,
  .swagger-ui .opblock-body pre.microlight { background: #1b1b1f; color: rgba(255,255,245,.86); }

  /* Info block */
  .swagger-ui .info .title, .swagger-ui .info p, .swagger-ui .info li { color: rgba(255,255,245,.86); }
  .swagger-ui .info a { color: #5b9cf6; }

  /* Scheme container */
  .swagger-ui .scheme-container { background: #202127; box-shadow: none; }
  .swagger-ui .schemes label { color: rgba(255,255,245,.86); }
  .swagger-ui select { background: #2c2c34; color: rgba(255,255,245,.86); border-color: rgba(82,82,89,1); }

  /* Tag headings and descriptions */
  .swagger-ui .opblock-tag { color: rgba(255,255,245,.86); border-bottom-color: rgba(82,82,89,1); }
  .swagger-ui .opblock-tag:hover { background: rgba(60,60,67,.3); }
  .swagger-ui .opblock-tag small,
  .swagger-ui .opblock-tag .renderedMarkdown p { color: rgba(235,235,245,.60); }

  /* Operation summary row */
  .swagger-ui .opblock .opblock-summary-description,
  .swagger-ui .opblock .opblock-summary-path,
  .swagger-ui .opblock .opblock-summary-path__deprecated { color: rgba(255,255,245,.86); }
  .swagger-ui .opblock .opblock-section-header { background: rgba(60,60,67,.6); }
  .swagger-ui .opblock .opblock-section-header label,
  .swagger-ui .opblock .opblock-section-header h4 { color: rgba(255,255,245,.86); }

  /* Operation description body */
  .swagger-ui .opblock-description-wrapper p,
  .swagger-ui .opblock-description-wrapper .renderedMarkdown p { color: rgba(255,255,245,.86); }

  /* All rendered markdown */
  .swagger-ui .renderedMarkdown p,
  .swagger-ui .renderedMarkdown li,
  .swagger-ui .markdown p,
  .swagger-ui .markdown li { color: rgba(255,255,245,.86); }

  /* Parameters table */
  .swagger-ui .parameters-col_description p,
  .swagger-ui .parameter__name,
  .swagger-ui .parameter__type,
  .swagger-ui .parameter__deprecated,
  .swagger-ui .parameter__empty__message,
  .swagger-ui table thead tr td,
  .swagger-ui table thead tr th { color: rgba(255,255,245,.86); }
  .swagger-ui .parameters-col_description input[type=text],
  .swagger-ui .body-param textarea { background: #202127; color: rgba(255,255,245,.86); border-color: rgba(82,82,89,1); }

  /* Example Value / Schema / Examples tabs */
  .swagger-ui .tab li button.tablinks { color: rgba(235,235,245,.60); }
  .swagger-ui .tab li button.tablinks.active,
  .swagger-ui .tab li button.tablinks:hover { color: rgba(255,255,245,.86); }

  /* Try it out button */
  .swagger-ui .try-out__btn { color: rgba(255,255,245,.86) !important; border-color: rgba(82,82,89,1) !important; }

  /* Response section */
  .swagger-ui .response-col_status,
  .swagger-ui .response-col_links,
  .swagger-ui .responses-inner h4,
  .swagger-ui .responses-inner h5 { color: rgba(255,255,245,.86); }
  .swagger-ui .response-control-media-type__title { color: rgba(235,235,245,.60); }

  /* Schema / models */
  .swagger-ui section.models { border-color: rgba(82,82,89,1); }
  .swagger-ui section.models h4 { color: rgba(255,255,245,.86); border-bottom-color: rgba(82,82,89,1); }
  .swagger-ui section.models .model-box,
  .swagger-ui .model-box { background: #202127; }
  .swagger-ui .model-title,
  .swagger-ui .model { color: rgba(255,255,245,.86); }
  .swagger-ui .prop-name { color: rgba(255,255,245,.86); }
  .swagger-ui .prop-type { color: #5b9cf6; }
  .swagger-ui .prop-format { color: rgba(235,235,245,.60); }
  .swagger-ui table.model tr.property-row td,
  .swagger-ui table.model tr.property-row .star { color: rgba(255,255,245,.86); }
  .swagger-ui .model .property.primitive { color: rgba(235,235,245,.60); }
  .swagger-ui .model-hint { color: rgba(235,235,245,.60); }

  /* Authorization modal */
  .swagger-ui .dialog-ux .modal-ux { background: #202127; border-color: rgba(82,82,89,1); }
  .swagger-ui .dialog-ux .modal-ux-header { background: #2c2c34; border-bottom-color: rgba(82,82,89,1); }
  .swagger-ui .dialog-ux .modal-ux-header h3 { color: rgba(255,255,245,.86); }
  .swagger-ui .dialog-ux .modal-ux-content,
  .swagger-ui .dialog-ux .modal-ux-content p,
  .swagger-ui .dialog-ux .modal-ux-content h4,
  .swagger-ui .auth-container h4,
  .swagger-ui .auth-container label,
  .swagger-ui .scopes h2,
  .swagger-ui .scope-def { color: rgba(255,255,245,.86); }
  .swagger-ui .dialog-ux .modal-ux-content input[type=text],
  .swagger-ui .auth-container input[type=text] { background: #1b1b1f; color: rgba(255,255,245,.86); border-color: rgba(82,82,89,1); }
  .swagger-ui .dialog-ux .modal-ux-content .auth-container { border-bottom-color: rgba(82,82,89,1); }

  /* Topbar */
  .swagger-ui .topbar { background: #0e0e11; }

  /* Links */
  .swagger-ui a.nostyle, .swagger-ui a.nostyle:visited { color: rgba(255,255,245,.86); }

  /* Code / microlight */
  .swagger-ui .highlight-code > .microlight { background: #0e0e11 !important; color: rgba(255,255,245,.86) !important; }

  /* SVG icons: expand/collapse carets and lock icons */
  .swagger-ui .expand-operation svg,
  .swagger-ui .opblock-control-arrow svg,
  .swagger-ui .opblock-tag svg,
  .swagger-ui .authorization__btn svg,
  .swagger-ui .expand-methods svg { fill: rgba(255,255,245,.86); opacity: 1; }

  /* Schema expand/collapse caret (background data-URL SVG, not inline) */
  .swagger-ui .model-toggle:after {
    background: url("data:image/svg+xml;charset=utf-8,<svg xmlns='http://www.w3.org/2000/svg' width='24' height='24' viewBox='0 0 24 24'><path fill='%23e4e6e6' d='M10 6 8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z'/></svg>") 50% no-repeat;
    background-size: 100%;
  }
`;

export function useSwaggerDark() {
  const { isDark } = useData();

  function applyTheme(dark: boolean) {
    let styleEl = document.getElementById(STYLE_ID) as HTMLStyleElement | null;
    if (!styleEl) {
      styleEl = document.createElement("style");
      styleEl.id = STYLE_ID;
      document.head.appendChild(styleEl);
    }
    styleEl.textContent = dark ? darkCSS : "";
  }

  onMounted(() => applyTheme(isDark.value));
  watch(isDark, applyTheme);
}
