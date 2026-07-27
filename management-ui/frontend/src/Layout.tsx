import { Menu, X } from "lucide-react";
import { useState } from "react";
import { Outlet } from "react-router-dom";
import { SideNav } from "./components/SideNav";

// Top-level frame: SideNav + routed content area. Generic on purpose so a
// future areas editor (and later sections) slot into SideNav/App.tsx
// without touching this file.
//
// Below the `md` breakpoint, SideNav is hidden behind a hamburger button
// in a mobile top bar and slides in as an overlay drawer instead of
// occupying permanent width -- there's no room for a fixed 192px rail
// next to the rule editor on a phone-sized viewport. At `md` and above,
// the drawer markup is simply never rendered (mobileNavOpen state still
// exists but the button that sets it is hidden), and SideNav sits
// statically in the flex row as before.
export function Layout() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  return (
    <div className="flex h-screen w-screen flex-col overflow-hidden bg-white text-slate-900 dark:bg-slate-950 dark:text-slate-100 md:flex-row">
      <header className="flex items-center gap-3 border-b border-slate-200 p-3 dark:border-slate-700 md:hidden">
        <button
          type="button"
          onClick={() => setMobileNavOpen(true)}
          aria-label="Open navigation menu"
          className="rounded-md p-1.5 text-slate-600 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-800"
        >
          <Menu size={20} />
        </button>
        <span className="text-sm font-semibold text-slate-500 dark:text-slate-400">
          SkyFollower Management Console
        </span>
      </header>

      <div className="hidden md:block">
        <SideNav />
      </div>

      {mobileNavOpen && (
        <div className="fixed inset-0 z-40 md:hidden">
          <div
            className="absolute inset-0 bg-black/40"
            onClick={() => setMobileNavOpen(false)}
            aria-hidden="true"
          />
          <div className="absolute inset-y-0 left-0 flex">
            <SideNav onNavigate={() => setMobileNavOpen(false)} />
            <button
              type="button"
              onClick={() => setMobileNavOpen(false)}
              aria-label="Close navigation menu"
              className="mt-4 ml-2 h-8 w-8 rounded-md bg-white/90 text-slate-600 shadow dark:bg-slate-800/90 dark:text-slate-300"
            >
              <X size={18} className="mx-auto" />
            </button>
          </div>
        </div>
      )}

      <main className="flex-1 overflow-y-auto p-4 md:p-6">
        <Outlet />
      </main>
    </div>
  );
}
