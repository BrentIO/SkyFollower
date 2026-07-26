import { Outlet } from "react-router-dom";
import { SideNav } from "./components/SideNav";

// Top-level frame: SideNav + routed content area. Generic on purpose so a
// future areas editor (and later sections) slot into SideNav/App.tsx
// without touching this file.
export function Layout() {
  return (
    <div className="flex h-screen w-screen overflow-hidden bg-white text-slate-900 dark:bg-slate-950 dark:text-slate-100">
      <SideNav />
      <main className="flex-1 overflow-y-auto p-6">
        <Outlet />
      </main>
    </div>
  );
}
