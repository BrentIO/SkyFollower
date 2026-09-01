import { useContext, type ReactNode } from "react";
import { ToastContext, useToastState } from "../hooks/useToast";

export function ToastProvider({ children }: { children: ReactNode }) {
  const state = useToastState();
  return (
    <ToastContext.Provider value={state}>
      {children}
      <ToastContainer />
    </ToastContext.Provider>
  );
}

// Only ever rendered by ToastProvider above, directly inside the provider
// it just created, so the context is guaranteed to be present.
function ToastContainer() {
  const { toasts, dismissToast } = useContext(ToastContext)!;

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2">
      {toasts.map((toast) => (
        <div
          key={toast.id}
          role="alert"
          className={`min-w-64 max-w-md rounded-md px-4 py-3 text-sm text-white shadow-lg ${
            toast.kind === "success" ? "bg-emerald-600" : "bg-red-600"
          }`}
        >
          <div className="flex items-start justify-between gap-3">
            <div className="max-h-64 overflow-y-auto">{toast.message}</div>
            <button
              type="button"
              onClick={() => dismissToast(toast.id)}
              className="shrink-0 text-white/80 hover:text-white"
              aria-label="Dismiss"
            >
              ×
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}
