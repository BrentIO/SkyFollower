import { NavLink } from "react-router-dom";

// Section list -- Rules is the only entry for now; a future areas editor
// adds an "Areas" entry here. Deliberately a plain array, not hardcoded
// JSX, so adding a section is a one-line change.
const SECTIONS = [{ path: "/rules", label: "Rules" }];

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
                `block rounded-md px-3 py-2 text-sm font-medium ${
                  isActive
                    ? "bg-sky-600 text-white"
                    : "text-slate-700 hover:bg-slate-200 dark:text-slate-200 dark:hover:bg-slate-800"
                }`
              }
            >
              {section.label}
            </NavLink>
          </li>
        ))}
      </ul>
    </nav>
  );
}
