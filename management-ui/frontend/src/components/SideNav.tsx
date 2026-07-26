import { FileCheck } from "lucide-react";
import { NavLink } from "react-router-dom";

// Section list -- Rules is the only entry for now; a future areas editor
// adds an "Areas" entry here. Deliberately a plain array, not hardcoded
// JSX, so adding a section is a one-line change.
const SECTIONS = [{ path: "/rules", label: "Rules", icon: FileCheck }];

export function SideNav() {
  return (
    <nav className="flex h-full w-48 shrink-0 flex-col border-r border-slate-200 bg-slate-50 p-4 dark:border-slate-700 dark:bg-slate-900">
      <div className="mb-6 px-2 text-sm font-semibold uppercase tracking-wide text-slate-400">
        SkyFollower
      </div>
      <ul className="flex flex-col gap-1">
        {SECTIONS.map((section) => (
          <li key={section.path}>
            <NavLink
              to={section.path}
              className={({ isActive }) =>
                `flex items-center gap-2 border-l-2 px-3 py-2 text-sm font-medium ${
                  isActive
                    ? "border-sky-600 text-sky-600 dark:text-sky-400"
                    : "border-transparent text-slate-600 hover:text-sky-600 dark:text-slate-300 dark:hover:text-sky-400"
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
