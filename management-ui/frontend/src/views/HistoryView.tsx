import { ChevronDown, ChevronUp, Download, Trash2 } from "lucide-react";
import { type ReactNode, useEffect, useState } from "react";
import { ConfirmModal } from "../components/ConfirmModal";
import { NewSearchModal } from "../components/NewSearchModal";
import {
  createArchiveSearch,
  deleteArchiveSearch,
  downloadArchiveFlight,
  getArchiveSearchDetail,
  getArchiveSearchResults,
  listArchiveSearches,
  type ArchiveSearchResultsPage,
  type ArchiveSearchSummary,
} from "../api/archiveSearch";
import { ApiError } from "../api/client";
import { useToast } from "../hooks/useToast";

// A couple of seconds' interval, per the design -- frequent enough that
// RUNNING -> COMPLETE/FAILED/ABORTED feels live, not so frequent it hammers
// the backend (which itself only polls Athena on the same kind of cadence).
const POLL_INTERVAL_MS = 3000;

type BadgeColor = "yellow" | "red" | "slate";

const BADGE_CLASSES: Record<BadgeColor, string> = {
  yellow: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
  red: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
  slate: "bg-slate-200 text-slate-700 dark:bg-slate-700 dark:text-slate-200",
};

function Badge({ color, children }: { color: BadgeColor; children: ReactNode }) {
  return <span className={`rounded px-2 py-0.5 text-xs font-semibold ${BADGE_CLASSES[color]}`}>{children}</span>;
}

function StatusBadge({ status }: { status: ArchiveSearchSummary["status"] }) {
  switch (status) {
    case "RUNNING":
      return <Badge color="yellow">Running</Badge>;
    case "FAILED":
      return <Badge color="red">Failed</Badge>;
    case "ABORTED":
      return <Badge color="slate">Aborted</Badge>;
    case "COMPLETE":
      return null; // Only shown for a non-COMPLETE status, per the design.
  }
}

// Athena returns "YYYY-MM-DD HH:MM:SS.sss" (UTC, no offset) -- swapping in
// "T"/"Z" makes it a real ISO string every browser's Date parser accepts,
// so the table can show it in the reader's own locale like everything
// else in this app rather than Athena's raw wire format.
function formatAthenaTimestamp(raw: string): string {
  const parsed = new Date(raw.replace(" ", "T") + "Z");
  return Number.isNaN(parsed.getTime()) ? raw : parsed.toLocaleString();
}

const PAGE_SIZE = 100;

interface SearchResultsPanelProps {
  search: ArchiveSearchSummary;
  results: ArchiveSearchResultsPage | null;
  resultsLoading: boolean;
  page: number;
  onPageChange: (page: number) => void;
  onViewFlight: (token: string) => void;
  whereClause: string | null;
  whereClauseLoading: boolean;
  onResubmit: () => void;
}

// Shown for a FAILED or ABORTED search: the reason, the WHERE clause that
// was submitted (fetched lazily -- see HistoryView's details-fetch effect),
// and a way to try again without retyping it from scratch.
function FailedSearchDetail({
  reason,
  whereClause,
  whereClauseLoading,
  onResubmit,
}: {
  reason: ReactNode;
  whereClause: string | null;
  whereClauseLoading: boolean;
  onResubmit: () => void;
}) {
  return (
    <div className="flex flex-col gap-3 p-4">
      <p className="text-sm text-red-600 dark:text-red-400">{reason}</p>
      <div>
        <p className="mb-1 text-xs font-semibold uppercase text-slate-500 dark:text-slate-400">
          WHERE clause submitted
        </p>
        {whereClauseLoading || whereClause === null ? (
          <p className="text-sm text-slate-400">Loading&hellip;</p>
        ) : (
          <pre className="overflow-x-auto rounded-md bg-slate-50 p-3 font-mono text-xs text-slate-700 dark:bg-slate-900 dark:text-slate-300">
            {whereClause}
          </pre>
        )}
      </div>
      <button
        type="button"
        onClick={onResubmit}
        disabled={whereClause === null}
        className="self-start rounded-md border border-sky-600 px-3 py-1.5 text-sm font-medium text-sky-600 hover:bg-sky-50 disabled:opacity-40 dark:border-sky-400 dark:text-sky-400 dark:hover:bg-sky-950"
      >
        Edit &amp; Resubmit
      </button>
    </div>
  );
}

// Shared between the desktop right panel and the mobile accordion -- same
// content either way, only the surrounding layout differs.
function SearchResultsPanel({
  search,
  results,
  resultsLoading,
  page,
  onPageChange,
  onViewFlight,
  whereClause,
  whereClauseLoading,
  onResubmit,
}: SearchResultsPanelProps) {
  if (search.status === "RUNNING") {
    return <p className="p-4 text-sm text-slate-400">Search is running&hellip;</p>;
  }
  if (search.status === "FAILED") {
    return (
      <FailedSearchDetail
        reason={search.error ?? "The search failed."}
        whereClause={whereClause}
        whereClauseLoading={whereClauseLoading}
        onResubmit={onResubmit}
      />
    );
  }
  if (search.status === "ABORTED") {
    return (
      <FailedSearchDetail
        reason="Search took too long -- try narrowing your filters."
        whereClause={whereClause}
        whereClauseLoading={whereClauseLoading}
        onResubmit={onResubmit}
      />
    );
  }

  // COMPLETE from here on.
  if (resultsLoading || results === null) {
    return <p className="p-4 text-sm text-slate-400">Loading results&hellip;</p>;
  }
  if (results.total_rows === 0) {
    return <p className="p-4 text-sm text-slate-400">No flights matched this search.</p>;
  }

  const totalPages = Math.max(1, Math.ceil(results.total_rows / PAGE_SIZE));

  return (
    <div className="flex h-full flex-col gap-3 overflow-hidden">
      <div className="overflow-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-slate-200 text-xs uppercase text-slate-500 dark:border-slate-700 dark:text-slate-400">
              <th className="px-2 py-1.5">Registration</th>
              <th className="px-2 py-1.5">ICAO Hex</th>
              <th className="px-2 py-1.5">Ident</th>
              <th className="px-2 py-1.5">Operator</th>
              <th className="px-2 py-1.5">Type</th>
              <th className="px-2 py-1.5">Military</th>
              <th className="px-2 py-1.5">First Message</th>
              <th className="px-2 py-1.5">Last Message</th>
              <th className="px-2 py-1.5" />
            </tr>
          </thead>
          <tbody>
            {results.rows.map((row) => (
              <tr key={row.uuid} className="border-b border-slate-100 dark:border-slate-800">
                <td className="px-2 py-1.5 font-mono">{row.registration}</td>
                <td className="px-2 py-1.5 font-mono">{row.icao_hex}</td>
                <td className="px-2 py-1.5 font-mono">{row.ident}</td>
                <td className="px-2 py-1.5">{row.operator_designator}</td>
                <td className="px-2 py-1.5">{row.type_designator}</td>
                <td className="px-2 py-1.5">{row.military && <Badge color="yellow">Military</Badge>}</td>
                <td className="px-2 py-1.5 whitespace-nowrap">{formatAthenaTimestamp(row.first_message)}</td>
                <td className="px-2 py-1.5 whitespace-nowrap">{formatAthenaTimestamp(row.last_message)}</td>
                <td className="px-2 py-1.5">
                  <button
                    type="button"
                    onClick={() => onViewFlight(row.token)}
                    aria-label="View flight"
                    title="View flight"
                    className="flex items-center gap-1 rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                  >
                    <Download size={12} />
                    View
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex items-center justify-end gap-3 text-sm">
        <button
          type="button"
          disabled={page <= 1}
          onClick={() => onPageChange(page - 1)}
          className="rounded-md border border-slate-300 px-3 py-1 font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
        >
          Prev
        </button>
        <span className="text-slate-500 dark:text-slate-400">
          Page {page} of {totalPages}
        </span>
        <button
          type="button"
          disabled={page >= totalPages}
          onClick={() => onPageChange(page + 1)}
          className="rounded-md border border-slate-300 px-3 py-1 font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
        >
          Next
        </button>
      </div>
    </div>
  );
}

export function HistoryView() {
  const { showToast } = useToast();
  const [searches, setSearches] = useState<ArchiveSearchSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedUuid, setSelectedUuid] = useState<string | null>(null);
  const [resultsCache, setResultsCache] = useState<Record<string, ArchiveSearchResultsPage>>({});
  const [resultsLoading, setResultsLoading] = useState(false);
  const [resultsPage, setResultsPage] = useState(1);
  const [mobileListOpen, setMobileListOpen] = useState(false);
  const [newSearchModalOpen, setNewSearchModalOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<ArchiveSearchSummary | null>(null);
  const [whereClauseCache, setWhereClauseCache] = useState<Record<string, string>>({});
  const [whereClauseLoading, setWhereClauseLoading] = useState(false);
  // Seeds the New Search modal when resubmitting a failed/aborted search;
  // null for a blank "+ New Search" open.
  const [resubmitSeed, setResubmitSeed] = useState<{ name: string; whereClause: string } | null>(null);

  const selectedSearch = searches.find((s) => s.uuid === selectedUuid) ?? null;

  // Initial load.
  useEffect(() => {
    let cancelled = false;
    listArchiveSearches()
      .then((loaded) => {
        if (!cancelled) setSearches(loaded);
      })
      .catch((err) => {
        if (!cancelled) showToast("error", err instanceof Error ? err.message : "Failed to load searches.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Live status updates: keep polling the list while anything is RUNNING.
  useEffect(() => {
    if (!searches.some((s) => s.status === "RUNNING")) return;
    let cancelled = false;
    const timer = setTimeout(() => {
      listArchiveSearches()
        .then((updated) => {
          if (!cancelled) setSearches(updated);
        })
        .catch(() => {
          // Transient poll failure -- this effect re-runs on the next
          // render regardless, so the next tick just retries.
        });
    }, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [searches]);

  // Fetch results for the selected search once it's COMPLETE, or when the
  // page changes -- cached per-uuid so switching back to an already-viewed
  // search doesn't refetch.
  useEffect(() => {
    if (!selectedSearch || selectedSearch.status !== "COMPLETE") return;
    const cacheKey = `${selectedSearch.uuid}:${resultsPage}`;
    if (resultsCache[cacheKey]) return;
    let cancelled = false;
    setResultsLoading(true);
    getArchiveSearchResults(selectedSearch.uuid, resultsPage)
      .then((page) => {
        if (!cancelled) setResultsCache((current) => ({ ...current, [cacheKey]: page }));
      })
      .catch((err) => {
        if (!cancelled) showToast("error", err instanceof Error ? err.message : "Failed to load results.");
      })
      .finally(() => {
        if (!cancelled) setResultsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedSearch?.uuid, selectedSearch?.status, resultsPage]);

  // Fetch the submitted WHERE clause for a FAILED/ABORTED search, so it can
  // be shown and offered back for resubmission instead of forcing the user
  // to retype it from scratch -- cached per-uuid like results above.
  useEffect(() => {
    if (!selectedSearch || (selectedSearch.status !== "FAILED" && selectedSearch.status !== "ABORTED")) return;
    if (whereClauseCache[selectedSearch.uuid]) return;
    let cancelled = false;
    setWhereClauseLoading(true);
    getArchiveSearchDetail(selectedSearch.uuid)
      .then((detail) => {
        if (!cancelled) {
          setWhereClauseCache((current) => ({ ...current, [detail.uuid]: detail.where_clause }));
        }
      })
      .catch((err) => {
        if (!cancelled) showToast("error", err instanceof Error ? err.message : "Failed to load the submitted WHERE clause.");
      })
      .finally(() => {
        if (!cancelled) setWhereClauseLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedSearch?.uuid, selectedSearch?.status]);

  function selectSearch(search: ArchiveSearchSummary) {
    setSelectedUuid((current) => (current === search.uuid ? null : search.uuid));
    setResultsPage(1);
  }

  async function handleNewSearchConfirm(name: string, whereClause: string) {
    setCreating(true);
    try {
      const { uuid } = await createArchiveSearch(name, whereClause);
      const updated = await listArchiveSearches();
      setSearches(updated);
      setSelectedUuid(uuid);
      setResultsPage(1);
      setNewSearchModalOpen(false);
      setResubmitSeed(null);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to start search.");
    } finally {
      setCreating(false);
    }
  }

  function handleResubmit(search: ArchiveSearchSummary) {
    const whereClause = whereClauseCache[search.uuid];
    if (whereClause === undefined) return;
    setResubmitSeed({ name: search.name, whereClause });
    setNewSearchModalOpen(true);
  }

  async function handleDeleteConfirmed() {
    if (!deleteTarget) return;
    try {
      await deleteArchiveSearch(deleteTarget.uuid);
      setSearches((current) => current.filter((s) => s.uuid !== deleteTarget.uuid));
      if (selectedUuid === deleteTarget.uuid) setSelectedUuid(null);
      showToast("success", `Search '${deleteTarget.name}' deleted.`);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to delete search.");
    } finally {
      setDeleteTarget(null);
    }
  }

  async function handleViewFlight(token: string) {
    try {
      await downloadArchiveFlight(token);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to download flight.");
    }
  }

  if (loading) {
    return <p className="text-slate-400">Loading search history...</p>;
  }

  const resultsForSelected = selectedSearch
    ? (resultsCache[`${selectedSearch.uuid}:${resultsPage}`] ?? null)
    : null;

  return (
    <div className="flex flex-col gap-4 md:h-full md:flex-row md:gap-6">
      <div className="flex flex-col gap-2 md:w-72 md:shrink-0">
        <button
          type="button"
          onClick={() => {
            setResubmitSeed(null);
            setNewSearchModalOpen(true);
          }}
          className="rounded-md border border-sky-600 px-3 py-2 text-sm font-medium text-sky-600 hover:bg-sky-50 dark:border-sky-400 dark:text-sky-400 dark:hover:bg-sky-950"
        >
          + New Search
        </button>

        <button
          type="button"
          onClick={() => setMobileListOpen((open) => !open)}
          aria-expanded={mobileListOpen}
          className="flex items-center justify-center gap-2 rounded-md bg-slate-100 px-3 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700 md:hidden"
        >
          {mobileListOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
          <span>Searches</span>
          {mobileListOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
        </button>

        <ul
          className={`${mobileListOpen ? "flex" : "hidden"} flex-col gap-1 overflow-y-auto md:flex md:max-h-none`}
        >
          {searches.map((search) => {
            const isSelected = selectedUuid === search.uuid;
            return (
              <li
                key={search.uuid}
                className={`rounded-r-md border-l-4 ${
                  isSelected
                    ? "border-sky-600 bg-slate-100 dark:border-sky-400 dark:bg-slate-800"
                    : "border-transparent hover:bg-slate-100 dark:hover:bg-slate-800"
                }`}
              >
                <div className="flex items-center gap-1 px-3 py-2">
                  <button
                    type="button"
                    onClick={() => selectSearch(search)}
                    className="flex flex-1 flex-col items-start gap-0.5 text-left"
                  >
                    <span className="flex w-full items-center justify-between gap-2">
                      <span
                        className={`truncate text-sm ${
                          isSelected ? "font-semibold text-sky-700 dark:text-sky-400" : "text-slate-700 dark:text-slate-200"
                        }`}
                      >
                        {search.name}
                      </span>
                      <StatusBadge status={search.status} />
                    </span>
                    <span className="text-xs text-slate-400 dark:text-slate-500">
                      Search results will automatically be purged on {new Date(search.expires_at).toLocaleString()}
                    </span>
                  </button>
                  <button
                    type="button"
                    onClick={() => setDeleteTarget(search)}
                    aria-label="Delete search"
                    title="Delete search"
                    className="shrink-0 rounded-md p-1.5 text-slate-400 hover:bg-red-50 hover:text-red-600 dark:hover:bg-red-950 dark:hover:text-red-400"
                  >
                    <Trash2 size={14} />
                  </button>
                </div>

                {/* Mobile accordion: results render inline under the
                    selected entry instead of in a separate right panel,
                    since there's no persistent side-by-side space below
                    md:. Desktop shows the same content in the right panel
                    below instead (hidden here via md:hidden). */}
                {isSelected && (
                  <div className="border-t border-slate-200 dark:border-slate-700 md:hidden">
                    <SearchResultsPanel
                      search={search}
                      results={resultsForSelected}
                      resultsLoading={resultsLoading}
                      page={resultsPage}
                      onPageChange={setResultsPage}
                      onViewFlight={handleViewFlight}
                      whereClause={whereClauseCache[search.uuid] ?? null}
                      whereClauseLoading={whereClauseLoading}
                      onResubmit={() => handleResubmit(search)}
                    />
                  </div>
                )}
              </li>
            );
          })}
          {searches.length === 0 && (
            <li className="px-3 py-2 text-sm text-slate-400">No searches yet. Start a new one.</li>
          )}
        </ul>
      </div>

      <div className="hidden overflow-hidden rounded-md border border-slate-200 dark:border-slate-700 md:block md:min-h-0 md:flex-1">
        {selectedSearch ? (
          <SearchResultsPanel
            search={selectedSearch}
            results={resultsForSelected}
            resultsLoading={resultsLoading}
            page={resultsPage}
            onPageChange={setResultsPage}
            onViewFlight={handleViewFlight}
            whereClause={whereClauseCache[selectedSearch.uuid] ?? null}
            whereClauseLoading={whereClauseLoading}
            onResubmit={() => handleResubmit(selectedSearch)}
          />
        ) : (
          <p className="p-4 text-sm text-slate-400">Select a search from the list, or start a new one.</p>
        )}
      </div>

      <NewSearchModal
        open={newSearchModalOpen}
        onConfirm={handleNewSearchConfirm}
        onCancel={() => {
          if (creating) return;
          setNewSearchModalOpen(false);
          setResubmitSeed(null);
        }}
        initialName={resubmitSeed?.name}
        initialWhereClause={resubmitSeed?.whereClause}
        title={resubmitSeed ? "Resubmit Search" : "New Search"}
      />

      <ConfirmModal
        open={deleteTarget !== null}
        title="Delete search?"
        message={
          deleteTarget ? (
            <>
              This will permanently delete '{deleteTarget.name}' and its cached results.
            </>
          ) : (
            ""
          )
        }
        confirmLabel="Delete"
        onConfirm={handleDeleteConfirmed}
        onCancel={() => setDeleteTarget(null)}
      />
    </div>
  );
}
