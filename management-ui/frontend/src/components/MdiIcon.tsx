interface MdiIconProps {
  path: string;
  size?: number;
  className?: string;
}

// Renders a single Material Design Icons path string (from @mdi/js) -- a
// small local wrapper instead of pulling in @mdi/react for a two-icon need.
export function MdiIcon({ path, size = 16, className }: MdiIconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="currentColor"
      className={className}
      aria-hidden="true"
    >
      <path d={path} />
    </svg>
  );
}
