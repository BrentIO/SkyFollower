import { MAXIMIZE_ICON, MINIMIZE_ICON, type IconSpec } from "./actionIcons";

// Pure icon-selection logic for ControlsPanel's fullscreen toggle button,
// pulled out of the component so it's plainly unit-testable.
export function fullscreenIcon(isFullscreen: boolean): IconSpec {
  return isFullscreen ? MINIMIZE_ICON : MAXIMIZE_ICON;
}
