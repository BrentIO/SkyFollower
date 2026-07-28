import { type FormEvent, useState } from "react";
import {
  getAircraft,
  getAirport,
  getOperator,
  getRoute,
  type AircraftRecord,
  type AirportRecord,
  type OperatorRecord,
  type RouteLookup,
} from "../api/reference";
import { ApiError } from "../api/client";
import { useToast } from "../hooks/useToast";

type TabKey = "aircraft" | "operator" | "airport" | "route";

const TABS: { key: TabKey; label: string; placeholder: string }[] = [
  { key: "aircraft", label: "Aircraft", placeholder: "ICAO hex (A8AE7F) or registration (N659DL)" },
  { key: "operator", label: "Operator", placeholder: "ICAO airline designator (DAL)" },
  { key: "airport", label: "Airport", placeholder: "ICAO (KJFK) or IATA (JFK) code" },
  { key: "route", label: "Route", placeholder: "Flight ident (DAL2)" },
];

// 6 hex digits -> icao_hex; anything else -> registration. Registration
// formats vary too much by country to validate further client-side (see
// #558) -- non-hex input is just passed through and a 404 means "not found."
const HEX_PATTERN = /^[0-9A-Fa-f]{6}$/;

type LookupResult =
  | { tab: "aircraft"; data: AircraftRecord }
  | { tab: "operator"; data: OperatorRecord }
  | { tab: "airport"; data: AirportRecord }
  | { tab: "route"; data: RouteLookup };

function humanizeKey(key: string): string {
  return key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

// These endpoints return whatever's actually in Redis, not a fixed schema
// (see api/reference.ts's comment) -- render every key present rather than
// a hardcoded field list, so nothing real ends up hidden. Handles nested
// objects (e.g. an aircraft's registrant/powerplant) and arrays (a list of
// primitives joined inline, a list of objects rendered as sub-blocks)
// recursively.
function RecordFields({ data }: { data: Record<string, unknown> }) {
  return (
    <div>
      {Object.entries(data).map(([key, value]) => {
        if (value === undefined || value === null || value === "") return null;
        return <FieldValue key={key} label={humanizeKey(key)} value={value} />;
      })}
    </div>
  );
}

function FieldValue({ label, value }: { label: string; value: unknown }) {
  if (Array.isArray(value)) {
    if (value.length === 0) return null;
    if (typeof value[0] === "object" && value[0] !== null) {
      return (
        <div className="border-b border-slate-100 py-1.5 dark:border-slate-800">
          <div className="text-sm text-slate-500 dark:text-slate-400">{label}</div>
          <ul className="ml-4 list-disc py-1">
            {value.map((item, i) => (
              <li key={i} className="mb-1">
                <RecordFields data={item as Record<string, unknown>} />
              </li>
            ))}
          </ul>
        </div>
      );
    }
    return <FieldRow label={label} value={value.join(", ")} />;
  }
  if (typeof value === "object") {
    return (
      <div className="border-b border-slate-100 py-1.5 dark:border-slate-800">
        <div className="text-sm text-slate-500 dark:text-slate-400">{label}</div>
        <div className="ml-4">
          <RecordFields data={value as Record<string, unknown>} />
        </div>
      </div>
    );
  }
  return <FieldRow label={label} value={value} />;
}

function FieldRow({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="flex gap-2 border-b border-slate-100 py-1.5 text-sm last:border-0 dark:border-slate-800">
      <span className="w-40 shrink-0 text-slate-500 dark:text-slate-400">{label}</span>
      <span className="text-slate-900 dark:text-slate-100">{String(value)}</span>
    </div>
  );
}

function airportLabel(a: AirportRecord): string {
  return a.name ? `${a.icao_code} — ${a.name}` : a.icao_code;
}

function RouteResultView({ data }: { data: RouteLookup }) {
  return (
    <div>
      <div className="mb-3 text-base font-semibold text-slate-900 dark:text-slate-100">
        {airportLabel(data.origin)} → {airportLabel(data.destination)}
      </div>
      <span className="text-sm text-slate-500 dark:text-slate-400">Full stop sequence</span>
      <ol className="mt-1 list-decimal pl-5 text-sm text-slate-900 dark:text-slate-100">
        {data.stops.map((stop, i) => (
          <li key={i}>{airportLabel(stop)}</li>
        ))}
      </ol>
    </div>
  );
}

export function LookupView() {
  const { showToast } = useToast();
  const [tab, setTab] = useState<TabKey>("aircraft");
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<LookupResult | null>(null);
  const [notFound, setNotFound] = useState(false);

  function selectTab(next: TabKey) {
    setTab(next);
    setQuery("");
    setResult(null);
    setNotFound(false);
  }

  async function handleSearch(e: FormEvent) {
    e.preventDefault();
    const trimmed = query.trim();
    if (!trimmed) return;

    setLoading(true);
    setResult(null);
    setNotFound(false);
    try {
      switch (tab) {
        case "aircraft": {
          const data = HEX_PATTERN.test(trimmed)
            ? await getAircraft({ icaoHex: trimmed })
            : await getAircraft({ registration: trimmed });
          setResult({ tab: "aircraft", data });
          break;
        }
        case "operator":
          setResult({ tab: "operator", data: await getOperator(trimmed) });
          break;
        case "airport":
          setResult({ tab: "airport", data: await getAirport(trimmed) });
          break;
        case "route":
          setResult({ tab: "route", data: await getRoute(trimmed) });
          break;
      }
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) {
        // Redis can't distinguish "never seen," "TTL expired," or "not yet
        // enriched" -- one generic message for all three, per #558.
        setNotFound(true);
      } else {
        showToast("error", err instanceof ApiError ? err.message : "Lookup failed.");
      }
    } finally {
      setLoading(false);
    }
  }

  const activeTabInfo = TABS.find((t) => t.key === tab)!;

  return (
    <div className="flex flex-col gap-4">
      <div className="flex gap-2">
        {TABS.map((t) => (
          <button
            key={t.key}
            type="button"
            onClick={() => selectTab(t.key)}
            className={`rounded-md px-3 py-2 text-sm font-medium ${
              tab === t.key
                ? "bg-sky-600 text-white"
                : "bg-slate-100 text-slate-700 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      <form onSubmit={handleSearch} className="flex max-w-lg gap-2">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={activeTabInfo.placeholder}
          className="flex-1 rounded-md border border-slate-300 px-3 py-1.5 text-sm dark:border-slate-600 dark:bg-slate-900"
        />
        <button
          type="submit"
          disabled={loading || !query.trim()}
          className="rounded-md bg-sky-600 px-4 py-1.5 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-40"
        >
          Search
        </button>
      </form>

      <div className="max-w-lg">
        {loading && <p className="text-slate-400">Searching...</p>}
        {!loading && notFound && <p className="text-slate-500 dark:text-slate-400">No data found.</p>}
        {!loading && result?.tab === "route" && <RouteResultView data={result.data} />}
        {!loading && result && result.tab !== "route" && <RecordFields data={result.data} />}
      </div>
    </div>
  );
}
