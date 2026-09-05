import { ChevronDown, ChevronUp, Copy, Download, Eye, Trash2 } from "lucide-react";
import { type ReactNode, useEffect, useState } from "react";
import { ConfirmModal } from "../components/ConfirmModal";
import { FlightViewModal } from "../components/FlightViewModal";
import { NewSearchModal } from "../components/NewSearchModal";
import {
  archiveSearchDownloadUrl,
  createArchiveSearch,
  deleteArchiveSearch,
  downloadArchiveFlight,
  getArchiveSearchDetail,
  getArchiveSearchResults,
  listArchiveSearches,
  type ArchiveSearchDetail,
  type ArchiveSearchResultsPage,
  type ArchiveSearchSortColumn,
  type ArchiveSearchSummary,
} from "../api/archiveSearch";
import { ApiError } from "../api/client";
import { useToast } from "../hooks/useToast";
import { nextSortState, type ResultsSortState } from "../lib/resultsSort";

// A couple of seconds' interval, per the design -- frequent enough that
// RUNNING -> COMPLETE/FAILED/ABORTED feels live, not so frequent it hammers
// the backend (which itself only polls Athena on the same kind of cadence).
const POLL_INTERVAL_MS = 3000;

type BadgeColor = "yellow" | "green" | "red" | "slate";

const BADGE_CLASSES: Record<BadgeColor, string> = {
  yellow: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
  green: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
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

// Default results-table page size.
const DEFAULT_PAGE_SIZE = 100;
const PAGE_SIZE_OPTIONS = [25, 50, 100, 200] as const;

// Every column header in the results table that's sortable, in display
// order -- mirrors main.py's _SORTABLE_COLUMNS. `military` is deliberately
// not listed here -- it no longer has its own column (the badge now lives
// inline in Operator), and it was never a commonly-used sort relative to
// registration/ident/operator, so no replacement sort affordance is
// offered. It remains a valid ArchiveSearchSortColumn/backend sort option
// even though no UI control reaches it.
const SORTABLE_COLUMNS: { column: ArchiveSearchSortColumn; label: string }[] = [
  { column: "registration", label: "Registration" },
  { column: "icao_hex", label: "ICAO Hex" },
  { column: "ident", label: "Ident" },
  { column: "operator_designator", label: "Operator" },
  { column: "type_designator", label: "Type" },
  { column: "first_message", label: "First Message" },
  { column: "last_message", label: "Last Message" },
];

// Sort state folded into the cache key alongside uuid/page/page-size --
// the same page number under a different sort order is different data.
// "none" stands in for the unsorted (server-default) order so it never
// collides with a real column name.
function resultsCacheKey(uuid: string, page: number, pageSize: number, sort: ResultsSortState | null): string {
  return `${uuid}:${page}:${pageSize}:${sort ? `${sort.column}:${sort.dir}` : "none"}`;
}

interface SearchResultsPanelProps {
  search: ArchiveSearchSummary;
  results: ArchiveSearchResultsPage | null;
  resultsLoading: boolean;
  page: number;
  onPageChange: (page: number) => void;
  pageSize: number;
  onPageSizeChange: (pageSize: number) => void;
  sort: ResultsSortState | null;
  onSortChange: (column: ArchiveSearchSortColumn) => void;
  onViewFlight: (token: string) => void;
  onDownloadFlight: (token: string) => void;
  // Carries where_clause plus the RESOLVED start_date/end_date actually
  // queried -- fetched lazily per search (see HistoryView's detail-fetch
  // effect), regardless of status, since the resolved range may have come
  // from derivation rather than anything the operator typed.
  detail: ArchiveSearchDetail | null;
  detailLoading: boolean;
  onResubmit: () => void;
  onDuplicate: (detail: ArchiveSearchDetail) => void;
  onDownloadCsv: () => void;
}

// "2022-01-01 to 2026-09-04 (UTC)" -- absent entirely for a legacy record
// with no persisted range (both fields null).
function RequestedRangeNote({ detail, loading }: { detail: ArchiveSearchDetail | null; loading: boolean }) {
  if (loading || detail === null) return null;
  if (!detail.requested_start_date && !detail.requested_end_date) return null;
  return (
    <p className="text-xs text-slate-400 dark:text-slate-500">
      Searched {detail.requested_start_date ?? "the start of the archive"} to{" "}
      {detail.requested_end_date ?? "today"} (UTC)
    </p>
  );
}

// One clickable, sortable column header -- shows an up/down chevron only
// for the currently active column so an inactive header stays uncluttered.
function SortableColumnHeader({
  label,
  column,
  sort,
  onSortChange,
}: {
  label: string;
  column: ArchiveSearchSortColumn;
  sort: ResultsSortState | null;
  onSortChange: (column: ArchiveSearchSortColumn) => void;
}) {
  const active = sort?.column === column;
  return (
    <th className="px-2 py-1.5">
      <button
        type="button"
        onClick={() => onSortChange(column)}
        aria-sort={active ? (sort?.dir === "asc" ? "ascending" : "descending") : "none"}
        className="flex items-center gap-1 uppercase tracking-wide text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
      >
        {label}
        {active && (sort?.dir === "asc" ? <ChevronUp size={12} /> : <ChevronDown size={12} />)}
      </button>
    </th>
  );
}

// The submitted WHERE clause, labeled and in a <pre> block -- shown for
// FAILED/ABORTED searches (inside FailedSearchDetail) and, per this view's
// design, above a COMPLETE search's results table too, so the text behind
// any search's results looks the same wherever it's shown. Fetched lazily
// per search (see HistoryView's detail-fetch effect), regardless of status.
function WhereClauseBlock({
  detail,
  detailLoading,
  onDuplicate,
}: {
  detail: ArchiveSearchDetail | null;
  detailLoading: boolean;
  onDuplicate: (detail: ArchiveSearchDetail) => void;
}) {
  return (
    <div>
      <div className="mb-1 flex items-center justify-between gap-2">
        <p className="text-xs font-semibold uppercase text-slate-500 dark:text-slate-400">WHERE clause submitted</p>
        <button
          type="button"
          onClick={() => detail && onDuplicate(detail)}
          disabled={detailLoading || detail === null}
          aria-label="Duplicate search"
          title="Duplicate search"
          className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md border border-slate-200 text-slate-500 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:text-slate-400 dark:hover:bg-slate-700"
        >
          <Copy size={14} />
        </button>
      </div>
      {detailLoading || detail === null ? (
        <p className="text-sm text-slate-400">Loading&hellip;</p>
      ) : (
        <pre className="overflow-x-auto rounded-md bg-slate-50 p-3 font-mono text-xs text-slate-700 dark:bg-slate-900 dark:text-slate-300">
          {detail.where_clause}
        </pre>
      )}
    </div>
  );
}

// Shown for a FAILED or ABORTED search: the reason, the WHERE clause and
// resolved date range that were submitted (fetched lazily -- see
// HistoryView's detail-fetch effect), and a way to try again without
// retyping either from scratch.
function FailedSearchDetail({
  reason,
  detail,
  detailLoading,
  onResubmit,
  onDuplicate,
}: {
  reason: ReactNode;
  detail: ArchiveSearchDetail | null;
  detailLoading: boolean;
  onResubmit: () => void;
  onDuplicate: (detail: ArchiveSearchDetail) => void;
}) {
  return (
    <div className="flex flex-col gap-3 p-4">
      <p className="text-sm text-red-600 dark:text-red-400">{reason}</p>
      <div>
        <WhereClauseBlock detail={detail} detailLoading={detailLoading} onDuplicate={onDuplicate} />
        <RequestedRangeNote detail={detail} loading={detailLoading} />
      </div>
      <button
        type="button"
        onClick={onResubmit}
        disabled={detail === null}
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
  pageSize,
  onPageSizeChange,
  sort,
  onSortChange,
  onViewFlight,
  onDownloadFlight,
  detail,
  detailLoading,
  onResubmit,
  onDuplicate,
  onDownloadCsv,
}: SearchResultsPanelProps) {
  if (search.status === "RUNNING") {
    return <p className="p-4 text-sm text-slate-400">Search is running&hellip;</p>;
  }
  if (search.status === "FAILED") {
    return (
      <FailedSearchDetail
        reason={search.error ?? "The search failed."}
        detail={detail}
        detailLoading={detailLoading}
        onResubmit={onResubmit}
        onDuplicate={onDuplicate}
      />
    );
  }
  if (search.status === "ABORTED") {
    return (
      <FailedSearchDetail
        reason="Search took too long -- try narrowing your filters."
        detail={detail}
        detailLoading={detailLoading}
        onResubmit={onResubmit}
        onDuplicate={onDuplicate}
      />
    );
  }

  // COMPLETE from here on.
  if (resultsLoading || results === null) {
    return (
      <div className="flex flex-col gap-2 p-4">
        <WhereClauseBlock detail={detail} detailLoading={detailLoading} onDuplicate={onDuplicate} />
        <RequestedRangeNote detail={detail} loading={detailLoading} />
        <p className="text-sm text-slate-400">Loading results&hellip;</p>
      </div>
    );
  }
  if (results.total_rows === 0) {
    return (
      <div className="flex flex-col gap-2 p-4">
        <WhereClauseBlock detail={detail} detailLoading={detailLoading} onDuplicate={onDuplicate} />
        <RequestedRangeNote detail={detail} loading={detailLoading} />
        <p className="text-sm text-slate-400">No flights matched this search.</p>
      </div>
    );
  }

  const totalPages = Math.max(1, Math.ceil(results.total_rows / pageSize));
  // total_rows is exact when not truncated, and the cache cap when it is
  // (the true count beyond the cap is never computed -- see the truncation
  // note below).
  const resultCountLabel = results.truncated
    ? `${results.total_rows.toLocaleString()}+ results`
    : `${results.total_rows.toLocaleString()} result${results.total_rows === 1 ? "" : "s"}`;

  return (
    <div className="flex h-full flex-col gap-3 overflow-hidden p-4">
      <WhereClauseBlock detail={detail} detailLoading={detailLoading} onDuplicate={onDuplicate} />
      <RequestedRangeNote detail={detail} loading={detailLoading} />
      {results.truncated && (
        <p className="text-xs text-amber-600 dark:text-amber-400">
          More than {results.total_rows} results -- showing the first {results.total_rows}. Use Download for the
          full set.
        </p>
      )}
      <div className="overflow-auto">
        <table className="w-full min-w-max text-left text-sm">
          <thead>
            <tr className="border-b border-slate-200 text-xs uppercase text-slate-500 dark:border-slate-700 dark:text-slate-400">
              {SORTABLE_COLUMNS.map(({ column, label }) => (
                <SortableColumnHeader key={column} label={label} column={column} sort={sort} onSortChange={onSortChange} />
              ))}
              <th className="px-2 py-1.5" />
            </tr>
          </thead>
          <tbody>
            {results.rows.map((row) => (
              <tr
                key={row.uuid}
                className="border-b border-slate-100 odd:bg-slate-50 dark:border-slate-800 dark:odd:bg-slate-900"
              >
                <td className="px-2 py-1.5">{row.registration}</td>
                <td className="px-2 py-1.5 font-mono">{row.icao_hex}</td>
                <td className="px-2 py-1.5">{row.ident}</td>
                <td className="px-2 py-1.5">
                  <div className="flex items-center gap-1.5">
                    <span>{row.operator_designator}</span>
                    {row.military && <Badge color="green">Military</Badge>}
                  </div>
                </td>
                <td className="px-2 py-1.5">{row.type_designator}</td>
                <td className="px-2 py-1.5 whitespace-nowrap">{formatAthenaTimestamp(row.first_message)}</td>
                <td className="px-2 py-1.5 whitespace-nowrap">{formatAthenaTimestamp(row.last_message)}</td>
                <td className="px-2 py-1.5">
                  <div className="flex items-center gap-1">
                    <button
                      type="button"
                      onClick={() => onViewFlight(row.token)}
                      aria-label="View flight"
                      title="View flight"
                      className="flex items-center gap-1 rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                    >
                      <Eye size={12} />
                      View
                    </button>
                    <button
                      type="button"
                      onClick={() => onDownloadFlight(row.token)}
                      aria-label="Download flight"
                      title="Download flight"
                      className="flex items-center gap-1 rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                    >
                      <Download size={12} />
                      Download
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex flex-col gap-3 text-sm md:flex-row md:items-center md:justify-between">
        <div className="flex flex-col gap-2 md:flex-row md:items-center md:gap-3">
          <button
            type="button"
            onClick={onDownloadCsv}
            className="flex w-full items-center justify-center gap-1.5 rounded-md border border-slate-300 px-3 py-2.5 font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700 md:w-auto md:justify-start md:py-1"
          >
            <Download size={14} />
            Download CSV
          </button>
          <span className="text-slate-500 dark:text-slate-400">{resultCountLabel}</span>
        </div>

        <div className="flex flex-col gap-3 md:flex-row md:items-center md:gap-3">
          <label className="flex items-center gap-1.5 text-slate-500 dark:text-slate-400">
            Rows per page
            <select
              value={pageSize}
              onChange={(event) => onPageSizeChange(Number(event.target.value))}
              className="rounded-md border border-slate-300 bg-white px-2 py-1 text-slate-700 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
            >
              {PAGE_SIZE_OPTIONS.map((size) => (
                <option key={size} value={size}>
                  {size}
                </option>
              ))}
            </select>
          </label>

          {/* Prev/Next as large, equal-width touch targets on mobile with
              the page label between them; desktop reverts to the original
              compact inline sizing. */}
          <div className="flex items-center gap-3">
            <button
              type="button"
              disabled={page <= 1}
              onClick={() => onPageChange(page - 1)}
              className="flex-1 rounded-md border border-slate-300 px-3 py-2.5 font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700 md:flex-none md:py-1"
            >
              Prev
            </button>
            <span className="shrink-0 text-slate-500 dark:text-slate-400">
              Page {page} of {totalPages}
            </span>
            <button
              type="button"
              disabled={page >= totalPages}
              onClick={() => onPageChange(page + 1)}
              className="flex-1 rounded-md border border-slate-300 px-3 py-2.5 font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700 md:flex-none md:py-1"
            >
              Next
            </button>
          </div>
        </div>
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
  const [resultsPageSize, setResultsPageSize] = useState<number>(DEFAULT_PAGE_SIZE);
  const [resultsSort, setResultsSort] = useState<ResultsSortState | null>(null);
  const [newSearchModalOpen, setNewSearchModalOpen] = useState(false);
  const [viewFlightToken, setViewFlightToken] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<ArchiveSearchSummary | null>(null);
  const [deletingSearch, setDeletingSearch] = useState(false);
  // Per-search where_clause + resolved start_date/end_date, fetched lazily
  // (see the detail-fetch effect below) regardless of status -- needed for
  // FAILED/ABORTED's "Edit & Resubmit" and to show the resolved range next
  // to any search's results, since that range may have come from
  // derivation rather than anything the operator typed.
  const [searchDetailCache, setSearchDetailCache] = useState<Record<string, ArchiveSearchDetail>>({});
  const [detailLoading, setDetailLoading] = useState(false);
  // Seeds the New Search modal when resubmitting a failed/aborted search or
  // duplicating any search; null for a blank "+ New Search" open. `title`
  // is set explicitly by whichever handler populates the seed rather than
  // inferred from its presence -- both flows populate a non-null seed, so
  // inferring from presence alone can't tell them apart.
  const [searchModalSeed, setSearchModalSeed] = useState<
    { title: string; name: string; whereClause: string; startDate: string; endDate: string } | null
  >(null);

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
  // page, page size, or sort changes -- cached per-uuid/page/page-size/sort
  // so switching back to an already-viewed combination doesn't refetch.
  useEffect(() => {
    if (!selectedSearch || selectedSearch.status !== "COMPLETE") return;
    const cacheKey = resultsCacheKey(selectedSearch.uuid, resultsPage, resultsPageSize, resultsSort);
    if (resultsCache[cacheKey]) return;
    let cancelled = false;
    setResultsLoading(true);
    getArchiveSearchResults(
      selectedSearch.uuid,
      resultsPage,
      resultsPageSize,
      resultsSort?.column,
      resultsSort?.dir,
    )
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
  }, [selectedSearch?.uuid, selectedSearch?.status, resultsPage, resultsPageSize, resultsSort]);

  // Fetch the submitted WHERE clause and resolved date range for whichever
  // search is selected, regardless of status -- FAILED/ABORTED needs it for
  // "Edit & Resubmit", and every status shows the resolved range next to
  // its results, since that range may have come from derivation rather
  // than anything the operator typed. Cached per-uuid like results above.
  useEffect(() => {
    if (!selectedSearch) return;
    if (searchDetailCache[selectedSearch.uuid]) return;
    let cancelled = false;
    setDetailLoading(true);
    getArchiveSearchDetail(selectedSearch.uuid)
      .then((detail) => {
        if (!cancelled) {
          setSearchDetailCache((current) => ({ ...current, [detail.uuid]: detail }));
        }
      })
      .catch((err) => {
        if (!cancelled) showToast("error", err instanceof Error ? err.message : "Failed to load search details.");
      })
      .finally(() => {
        if (!cancelled) setDetailLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedSearch?.uuid]);

  function selectSearch(search: ArchiveSearchSummary) {
    setSelectedUuid((current) => (current === search.uuid ? null : search.uuid));
    setResultsPage(1);
  }

  // Changing the page size invalidates the current page number -- e.g. page
  // 3 at 25/page may no longer exist at 200/page -- so always snap back to
  // page 1 rather than risk landing out of range.
  function handlePageSizeChange(pageSize: number) {
    setResultsPageSize(pageSize);
    setResultsPage(1);
  }

  // A new sort order re-ranks the entire result set, so whatever page
  // number was showing may no longer make sense -- snap back to page 1.
  function handleSortChange(column: ArchiveSearchSortColumn) {
    setResultsSort((current) => nextSortState(current, column));
    setResultsPage(1);
  }

  async function handleNewSearchConfirm(name: string, whereClause: string, startDate: string, endDate: string) {
    setCreating(true);
    try {
      const { uuid } = await createArchiveSearch(name, whereClause, startDate, endDate);
      const updated = await listArchiveSearches();
      setSearches(updated);
      setSelectedUuid(uuid);
      setResultsPage(1);
      setNewSearchModalOpen(false);
      setSearchModalSeed(null);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to start search.");
    } finally {
      setCreating(false);
    }
  }

  function handleResubmit(search: ArchiveSearchSummary) {
    const detail = searchDetailCache[search.uuid];
    if (detail === undefined) return;
    // Carries the persisted, already-RESOLVED dates forward verbatim (not
    // re-derived) -- see main.py's create_archive_search docstring on why
    // dropping these here would silently change what the resubmit matches.
    setSearchModalSeed({
      title: "Resubmit Search",
      name: search.name,
      whereClause: detail.where_clause,
      startDate: detail.start_date ?? "",
      endDate: detail.end_date ?? "",
    });
    setNewSearchModalOpen(true);
  }

  // Pre-fills the New Search dialog from an existing search's WHERE clause
  // and resolved date range, same as handleResubmit, except the Name field
  // is left blank and this always creates a brand new, independent search
  // rather than editing in place -- available for every status (COMPLETE
  // included), not just FAILED/ABORTED.
  function handleDuplicate(detail: ArchiveSearchDetail) {
    setSearchModalSeed({
      title: "New Search",
      name: "",
      whereClause: detail.where_clause,
      startDate: detail.start_date ?? "",
      endDate: detail.end_date ?? "",
    });
    setNewSearchModalOpen(true);
  }

  // A real browser navigation (new tab), not a fetch() -- the endpoint 307s
  // to a presigned S3 URL, and only a real navigation follows that without
  // the S3 bucket needing its own CORS policy (see archiveSearchDownloadUrl).
  // One code path for every result size: the backend re-runs the search via
  // a second, sanitized query and serves the CSV straight from S3, so this
  // never reads the result set into the browser at all, unlike the paged
  // table view's capped, in-memory cache.
  function handleDownloadCsv(search: ArchiveSearchSummary) {
    window.open(archiveSearchDownloadUrl(search.uuid), "_blank");
  }

  async function handleDeleteConfirmed() {
    if (!deleteTarget) return;
    setDeletingSearch(true);
    try {
      await deleteArchiveSearch(deleteTarget.uuid);
      setSearches((current) => current.filter((s) => s.uuid !== deleteTarget.uuid));
      if (selectedUuid === deleteTarget.uuid) setSelectedUuid(null);
      showToast("success", `Search '${deleteTarget.name}' deleted.`);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to delete search.");
    } finally {
      setDeletingSearch(false);
      setDeleteTarget(null);
    }
  }

  async function handleDownloadFlight(token: string) {
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
    ? (resultsCache[resultsCacheKey(selectedSearch.uuid, resultsPage, resultsPageSize, resultsSort)] ?? null)
    : null;

  return (
    <div className="flex flex-col gap-4 md:h-full md:flex-row md:gap-6">
      <div className="flex flex-col gap-2 md:w-72 md:shrink-0">
        <button
          type="button"
          onClick={() => {
            setSearchModalSeed(null);
            setNewSearchModalOpen(true);
          }}
          className="rounded-md border border-sky-600 px-3 py-2 text-sm font-medium text-sky-600 hover:bg-sky-50 dark:border-sky-400 dark:text-sky-400 dark:hover:bg-sky-950"
        >
          + New Search
        </button>

        <ul className="flex flex-col gap-2 overflow-y-auto md:gap-1">
          {searches.map((search) => {
            const isSelected = selectedUuid === search.uuid;
            return (
              <li
                key={search.uuid}
                className={`rounded-md border-l-4 md:rounded-l-none md:rounded-r-md ${
                  isSelected
                    ? "border-sky-600 bg-slate-100 dark:border-sky-400 dark:bg-slate-800"
                    : "border-transparent bg-slate-50 hover:bg-slate-100 dark:bg-slate-800/40 dark:hover:bg-slate-800 md:bg-transparent dark:md:bg-transparent"
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
                  // No padding here -- every SearchResultsPanel branch
                  // brings its own p-4, so the desktop right panel (whose
                  // wrapper is border-only) and this mobile accordion stay
                  // visually identical.
                  <div className="border-t border-slate-200 dark:border-slate-700 md:hidden">
                    <SearchResultsPanel
                      search={search}
                      results={resultsForSelected}
                      resultsLoading={resultsLoading}
                      page={resultsPage}
                      onPageChange={setResultsPage}
                      pageSize={resultsPageSize}
                      onPageSizeChange={handlePageSizeChange}
                      sort={resultsSort}
                      onSortChange={handleSortChange}
                      onViewFlight={setViewFlightToken}
                      onDownloadFlight={handleDownloadFlight}
                      detail={searchDetailCache[search.uuid] ?? null}
                      detailLoading={detailLoading}
                      onResubmit={() => handleResubmit(search)}
                      onDuplicate={handleDuplicate}
                      onDownloadCsv={() => handleDownloadCsv(search)}
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
            pageSize={resultsPageSize}
            onPageSizeChange={handlePageSizeChange}
            sort={resultsSort}
            onSortChange={handleSortChange}
            onViewFlight={setViewFlightToken}
            onDownloadFlight={handleDownloadFlight}
            detail={searchDetailCache[selectedSearch.uuid] ?? null}
            detailLoading={detailLoading}
            onResubmit={() => handleResubmit(selectedSearch)}
            onDuplicate={handleDuplicate}
            onDownloadCsv={() => handleDownloadCsv(selectedSearch)}
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
          setSearchModalSeed(null);
        }}
        initialName={searchModalSeed?.name}
        initialWhereClause={searchModalSeed?.whereClause}
        initialStartDate={searchModalSeed?.startDate}
        initialEndDate={searchModalSeed?.endDate}
        title={searchModalSeed?.title ?? "New Search"}
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
        confirmLoading={deletingSearch}
        onConfirm={handleDeleteConfirmed}
        onCancel={() => setDeleteTarget(null)}
      />

      <FlightViewModal token={viewFlightToken} onClose={() => setViewFlightToken(null)} />
    </div>
  );
}
