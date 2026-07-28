import { FileCheck, MapPin, Search } from "lucide-react";
import { NavLink } from "react-router-dom";

// Section list -- deliberately a plain array, not hardcoded JSX, so adding
// a section is a one-line change.
const SECTIONS = [
  { path: "/rules", label: "Rules", icon: FileCheck },
  { path: "/areas", label: "Areas", icon: MapPin },
  { path: "/lookup", label: "Lookup", icon: Search },
];

interface SideNavProps {
  // Layout.tsx passes this on the mobile drawer copy so picking a section
  // also closes the drawer; the persistent desktop copy omits it.
  onNavigate?: () => void;
}

export function SideNav({ onNavigate }: SideNavProps) {
  return (
    <nav className="flex h-full w-48 shrink-0 flex-col border-r border-slate-200 bg-slate-50 p-4 dark:border-slate-700 dark:bg-slate-900">
      <div className="mb-6 px-2 text-sm font-semibold leading-snug text-slate-500 dark:text-slate-400">
        SkyFollower Management Console
      </div>
      <ul className="flex flex-col gap-1">
        {SECTIONS.map((section) => (
          <li key={section.path}>
            <NavLink
              to={section.path}
              onClick={onNavigate}
              className={({ isActive }) =>
                `flex items-center gap-2 px-3 py-2 text-sm ${
                  isActive
                    ? "font-bold text-sky-600 dark:text-sky-400"
                    : "font-medium text-slate-600 hover:text-sky-600 dark:text-slate-300 dark:hover:text-sky-400"
                }`
              }
            >
              <section.icon size={16} />
              {section.label}
            </NavLink>
          </li>
        ))}
      </ul>
    </nav>
  );
}
