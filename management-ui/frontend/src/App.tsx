import { Navigate, Route, BrowserRouter, Routes } from "react-router-dom";
import { Layout } from "./Layout";
import { ToastProvider } from "./components/ToastContainer";
import { AreasView } from "./views/AreasView";
import { RulesView } from "./views/RulesView";

// BrowserRouter -- nginx.conf's try_files falls back to /index.html for any
// unmatched path, so a deep-link reload (e.g. /rules) resolves correctly.
export default function App() {
  return (
    <ToastProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<Navigate to="/rules" replace />} />
            <Route path="rules" element={<RulesView />} />
            <Route path="areas" element={<AreasView />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </ToastProvider>
  );
}
