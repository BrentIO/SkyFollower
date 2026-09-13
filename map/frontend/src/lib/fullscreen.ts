import { MAXIMIZE_ICON, MINIMIZE_ICON, type IconSpec } from "./actionIcons";

// Pure icon-selection logic for ControlsPanel's fullscreen toggle button,
// pulled out of the component so it's unit-testable without this project's
// source-text-extraction convention (see ControlsPanel.test.ts) -- the
// state->icon mapping itself is a plain value transform with no DOM
// dependency, unlike the fullscreenchange listener wiring in MapView.tsx.
export function fullscreenIcon(isFullscreen: boolean): IconSpec {
  return isFullscreen ? MINIMIZE_ICON : MAXIMIZE_ICON;
}
