import { createContext, useCallback, useContext, useState } from "react";

export type ToastKind = "success" | "error";

export interface Toast {
  id: number;
  kind: ToastKind;
  message: string;
}

interface ToastContextValue {
  toasts: Toast[];
  showToast: (kind: ToastKind, message: string) => void;
  dismissToast: (id: number) => void;
}

export const ToastContext = createContext<ToastContextValue | null>(null);

let nextId = 1;

// Owns the toast list; ToastProvider (in ToastContainer.tsx) wraps the app
// with ToastContext.Provider using this, and any component calls useToast()
// to enqueue one -- e.g. after a rule save succeeds or fails.
export function useToastState(): ToastContextValue {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const dismissToast = useCallback((id: number) => {
    setToasts((current) => current.filter((t) => t.id !== id));
  }, []);

  const showToast = useCallback(
    (kind: ToastKind, message: string) => {
      const id = nextId++;
      setToasts((current) => [...current, { id, kind, message }]);
      setTimeout(() => dismissToast(id), 5000);
    },
    [dismissToast],
  );

  return { toasts, showToast, dismissToast };
}

export function useToast(): Pick<ToastContextValue, "showToast"> {
  const ctx = useContext(ToastContext);
  if (!ctx) {
    throw new Error("useToast must be used within a ToastProvider");
  }
  return ctx;
}
