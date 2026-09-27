import { Navigate, Route, BrowserRouter, Routes } from "react-router-dom";
import { Layout } from "./Layout";
import { ToastProvider } from "./components/ToastContainer";
import { AreasView } from "./views/AreasView";
import { HistoryView } from "./views/HistoryView";
import { LookupView } from "./views/LookupView";
import { RulesView } from "./views/RulesView";

// BrowserRouter relies on nginx.conf's try_files fallback to /index.html for deep links.
export default function App() {
  return (
    <ToastProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<Navigate to="/rules" replace />} />
            <Route path="rules" element={<RulesView />} />
            <Route path="areas" element={<AreasView />} />
            <Route path="lookup" element={<LookupView />} />
            <Route path="history" element={<HistoryView />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </ToastProvider>
  );
}
