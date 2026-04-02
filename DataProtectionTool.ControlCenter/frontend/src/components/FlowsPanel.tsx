import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { FlowSource, FlowDest } from "./FullRunModal";
import type { StatusEvent, StatusEventStep } from "./StatusBar";
import "./FlowsPanel.css";

interface ConsolidatedStep extends StatusEventStep {
  pollCount?: number;
}

function consolidateSteps(steps: StatusEventStep[]): ConsolidatedStep[] {
  const result: ConsolidatedStep[] = [];
  for (const step of steps) {
    if (!step.message.startsWith("Polling ")) {
      result.push({ ...step });
      continue;
    }
    const prefix = step.message.replace(/:.*$/, "");
    const prev = result[result.length - 1];
    if (prev && prev.message.startsWith("Polling ") && prev.message.replace(/:.*$/, "") === prefix) {
      prev.message = step.message;
      prev.status = step.status;
      prev.timestamp = step.timestamp;
      prev.pollCount = (prev.pollCount ?? 1) + 1;
    } else {
      result.push({ ...step, pollCount: 1 });
    }
  }
  return result;
}

export interface FlowItem {
  rowKey: string;
  sourceJson: string;
  destJson: string;
  createdAt: string;
}

interface FlowsPanelProps {
  agentPath: string;
  statusEvents: StatusEvent[];
  connectionsBadgeCount?: number;
  flowsBadgeCount?: number;
  newFlowRowKeys?: Set<string>;
  onDismissNewFlowBadge?: (rowKey: string) => void;
  onSwitchPanel: (panel: "connections" | "flows") => void;
  onRunFlows?: (flows: FlowItem[]) => void;
  mockFlows?: FlowItem[];
}

type SortField = "source" | "destination";
type SortDir = "asc" | "desc";

interface ParsedFlow {
  rowKey: string;
  createdAt: string;
  srcServer: string;
  srcDatabase: string;
  srcSchema: string;
  srcTable: string;
  destServer: string;
  destDatabase: string;
  destSchema: string;
  destTable: string;
}

interface GroupedFlow {
  groupKey: string;
  flowRowKeys: string[];
  srcServer: string;
  srcDatabase: string;
  tables: { schema: string; table: string; rowKey: string }[];
  destServer: string;
  destDatabase: string;
  destSchema: string;
  latestCreatedAt: string;
}

const COLUMNS: { key: SortField; label: string }[] = [
  { key: "source", label: "Source" },
  { key: "destination", label: "Destination" },
];

function parseJson<T>(json: string): T | null {
  try {
    return JSON.parse(json);
  } catch {
    return null;
  }
}

function parseFlow(flow: FlowItem): ParsedFlow {
  const src = parseJson<FlowSource>(flow.sourceJson);
  const dest = parseJson<FlowDest>(flow.destJson);
  return {
    rowKey: flow.rowKey,
    createdAt: flow.createdAt ?? "",
    srcServer: src?.serverName || "—",
    srcDatabase: src?.databaseName || src?.serverName || "—",
    srcSchema: src?.schema || "—",
    srcTable: src?.tableName || "—",
    destServer: dest?.serverName || "—",
    destDatabase: dest?.databaseName || dest?.serverName || "—",
    destSchema: dest?.schema || "—",
    destTable: dest?.tableName || "—",
  };
}

function groupFlows(parsedFlows: ParsedFlow[]): GroupedFlow[] {
  const map = new Map<string, GroupedFlow>();
  for (const f of parsedFlows) {
    const gk = `${f.srcServer}|${f.srcDatabase}|${f.destServer}|${f.destDatabase}|${f.destSchema}`;
    const existing = map.get(gk);
    if (existing) {
      existing.flowRowKeys.push(f.rowKey);
      existing.tables.push({ schema: f.srcSchema, table: f.srcTable, rowKey: f.rowKey });
      if (f.createdAt > existing.latestCreatedAt) {
        existing.latestCreatedAt = f.createdAt;
      }
    } else {
      map.set(gk, {
        groupKey: gk,
        flowRowKeys: [f.rowKey],
        srcServer: f.srcServer,
        srcDatabase: f.srcDatabase,
        tables: [{ schema: f.srcSchema, table: f.srcTable, rowKey: f.rowKey }],
        destServer: f.destServer,
        destDatabase: f.destDatabase,
        destSchema: f.destSchema,
        latestCreatedAt: f.createdAt,
      });
    }
  }
  return Array.from(map.values());
}

function formatHistoryTime(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString([], {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch {
    return "";
  }
}

export default function FlowsPanel({
  agentPath,
  statusEvents,
  connectionsBadgeCount: _connectionsBadgeCount,
  flowsBadgeCount: _flowsBadgeCount,
  newFlowRowKeys,
  onDismissNewFlowBadge,
  onSwitchPanel: _onSwitchPanel,
  onRunFlows,
  mockFlows,
}: FlowsPanelProps) {
  const [flows, setFlows] = useState<FlowItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchText, setSearchText] = useState("");
  const [sortField, setSortField] = useState<SortField | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [actionOpen, setActionOpen] = useState(false);
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);
  const [confirmRunOpen, setConfirmRunOpen] = useState(false);
  const [selectedFlowRowKey, setSelectedFlowRowKey] = useState<string | null>(null);
  const [expandedHistoryIdx, setExpandedHistoryIdx] = useState<number | null>(null);
  const actionRef = useRef<HTMLDivElement>(null);

  const fetchFlows = useCallback(async () => {
    if (mockFlows) {
      setFlows(mockFlows);
      setLoading(false);
      return;
    }
    if (!agentPath) return;
    setLoading(true);
    try {
      const res = await fetch(`/api/agents/${agentPath}/flows`);
      if (res.ok) {
        const data = await res.json();
        setFlows(Array.isArray(data) ? data : []);
      }
    } catch {
      // best-effort
    } finally {
      setLoading(false);
    }
  }, [agentPath, mockFlows]);

  useEffect(() => {
    fetchFlows();
  }, [fetchFlows]);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (actionRef.current && !actionRef.current.contains(e.target as Node)) {
        setActionOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const selectedFlowId = useMemo(() => {
    if (!selectedFlowRowKey) return null;
    return selectedFlowRowKey.startsWith("flow_")
      ? selectedFlowRowKey.slice("flow_".length)
      : selectedFlowRowKey;
  }, [selectedFlowRowKey]);

  const historyEvents = useMemo(() => {
    if (!selectedFlowId) return [];
    return [...statusEvents]
      .filter((e) => e.type === "dp_run" && e.flowId === selectedFlowId)
      .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
  }, [statusEvents, selectedFlowId]);

  function handleRowClick(group: GroupedFlow, e: React.MouseEvent) {
    const target = e.target as HTMLElement;
    if (target.tagName === "INPUT" || target.closest(".flows-td-checkbox")) return;
    for (const rk of group.flowRowKeys) {
      if (newFlowRowKeys?.has(rk)) {
        onDismissNewFlowBadge?.(rk);
      }
    }
    const firstKey = group.flowRowKeys[0];
    setSelectedFlowRowKey((prev) => (prev === firstKey ? null : firstKey));
  }

  const parsed = useMemo(() => flows.map(parseFlow), [flows]);

  const grouped = useMemo(() => groupFlows(parsed), [parsed]);

  const selectedParsedFlow = useMemo(
    () => (selectedFlowRowKey ? parsed.find((f) => f.rowKey === selectedFlowRowKey) ?? null : null),
    [parsed, selectedFlowRowKey],
  );

  const filtered = useMemo(() => {
    if (!searchText.trim()) return grouped;
    const q = searchText.toLowerCase();
    return grouped.filter((g) =>
      g.srcServer.toLowerCase().includes(q) ||
      g.srcDatabase.toLowerCase().includes(q) ||
      g.destServer.toLowerCase().includes(q) ||
      g.destDatabase.toLowerCase().includes(q) ||
      g.destSchema.toLowerCase().includes(q) ||
      g.tables.some((t) =>
        t.schema.toLowerCase().includes(q) ||
        t.table.toLowerCase().includes(q)
      )
    );
  }, [grouped, searchText]);

  const sorted = useMemo(() => {
    if (!sortField) {
      return [...filtered].sort((a, b) => (b.latestCreatedAt ?? "").localeCompare(a.latestCreatedAt ?? ""));
    }
    const dir = sortDir === "asc" ? 1 : -1;
    return [...filtered].sort((a, b) => {
      let va: string, vb: string;
      if (sortField === "source") {
        va = `${a.srcServer} ${a.srcDatabase}`.toLowerCase();
        vb = `${b.srcServer} ${b.srcDatabase}`.toLowerCase();
      } else {
        va = `${a.destServer} ${a.destDatabase}`.toLowerCase();
        vb = `${b.destServer} ${b.destDatabase}`.toLowerCase();
      }
      if (va < vb) return -1 * dir;
      if (va > vb) return 1 * dir;
      return 0;
    });
  }, [filtered, sortField, sortDir]);

  function handleSort(field: SortField) {
    if (sortField === field) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir("asc");
    }
  }

  function handleSelectAll() {
    const allKeys = sorted.flatMap((g) => g.flowRowKeys);
    const allSelected = allKeys.length > 0 && allKeys.every((k) => selected.has(k));
    if (allSelected) {
      setSelected((prev) => {
        const next = new Set(prev);
        for (const k of allKeys) next.delete(k);
        return next;
      });
    } else {
      setSelected((prev) => new Set([...prev, ...allKeys]));
    }
  }

  function handleSelectGroup(group: GroupedFlow) {
    const allGroupSelected = group.flowRowKeys.every((k) => selected.has(k));
    setSelected((prev) => {
      const next = new Set(prev);
      if (allGroupSelected) {
        for (const k of group.flowRowKeys) next.delete(k);
      } else {
        for (const k of group.flowRowKeys) next.add(k);
      }
      return next;
    });
  }

  const allFilteredSelected = sorted.length > 0 && sorted.flatMap((g) => g.flowRowKeys).every((k) => selected.has(k));
  const someFilteredSelected = sorted.some((g) => g.flowRowKeys.some((k) => selected.has(k)));

  function handleDeleteSelected() {
    setActionOpen(false);
    if (selected.size === 0) return;
    setConfirmDeleteOpen(true);
  }

  async function confirmDelete() {
    setConfirmDeleteOpen(false);
    const rowKeys = [...selected];
    if (rowKeys.length === 0) return;
    try {
      const res = await fetch(`/api/agents/${agentPath}/delete-flows`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rowKeys }),
      });
      if (res.ok) {
        setSelected(new Set());
        fetchFlows();
      }
    } catch {
      // best-effort
    }
  }

  function handleRunSelected() {
    setActionOpen(false);
    if (selected.size === 0) return;
    setConfirmRunOpen(true);
  }

  function confirmRun() {
    setConfirmRunOpen(false);
    const selectedFlows = flows.filter((f) => selected.has(f.rowKey));
    if (selectedFlows.length === 0) return;
    onRunFlows?.(selectedFlows);
  }

  const selectedGroupsForRun = useMemo(
    () => grouped.filter((g) => g.flowRowKeys.some((k) => selected.has(k))),
    [grouped, selected],
  );

  return (
    <div className="flows-panel-full">
      <div className="flows-panel-header">
        <div className="flows-panel-header-left">
          <span className="panel-title-text">End-to-End Flows</span>
          <div className="flows-search-wrapper">
            <svg className="flows-search-icon" width="14" height="14" viewBox="0 0 14 14" fill="none">
              <circle cx="6" cy="6" r="4.5" stroke="currentColor" strokeWidth="1.3" />
              <path d="M9.5 9.5L13 13" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
            </svg>
            <input
              className="flows-search-input"
              type="text"
              placeholder="Search flows..."
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
            />
          </div>
          <div className="flows-action-dropdown" ref={actionRef}>
            <button
              className="flows-action-btn"
              onClick={() => setActionOpen((v) => !v)}
              disabled={selected.size === 0}
            >
              Action on Selected Items
              <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
                <path d="M2.5 4L5 6.5L7.5 4" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </button>
            {actionOpen && (
              <div className="flows-action-menu">
                <button className="flows-action-menu-item" onClick={handleRunSelected}>
                  Run
                </button>
                <button className="flows-action-menu-item flows-action-menu-item-danger" onClick={handleDeleteSelected}>
                  Delete
                </button>
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="flows-content-area">
        <div className={`flows-table-container${selectedFlowRowKey ? " flows-table-container-shrunk" : ""}`}>
          {loading ? (
            <p className="flows-panel-empty">Loading...</p>
          ) : flows.length === 0 ? (
            <p className="flows-panel-empty">No flows saved yet.</p>
          ) : sorted.length === 0 ? (
            <p className="flows-panel-empty">No flows match the search.</p>
          ) : (
            <table className="flows-table flows-table-grouped">
              <thead>
                <tr>
                  <th className="flows-th-checkbox">
                    <input
                      type="checkbox"
                      checked={allFilteredSelected}
                      ref={(el) => {
                        if (el) el.indeterminate = someFilteredSelected && !allFilteredSelected;
                      }}
                      onChange={handleSelectAll}
                    />
                  </th>
                  {COLUMNS.map((col) => (
                    <th
                      key={col.key}
                      className="flows-th"
                      onClick={() => handleSort(col.key)}
                    >
                      <span className="flows-th-label">
                        {col.label}
                        {sortField === col.key && (
                          <span className="flows-sort-arrow">
                            {sortDir === "asc" ? "▲" : "▼"}
                          </span>
                        )}
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sorted.map((group) => {
                  const isGroupSelected = group.flowRowKeys.every((k) => selected.has(k));
                  const someGroupSelected = group.flowRowKeys.some((k) => selected.has(k));
                  const isActive = group.flowRowKeys.includes(selectedFlowRowKey ?? "");
                  const hasNew = group.flowRowKeys.some((rk) => newFlowRowKeys?.has(rk));
                  return (
                    <tr
                      key={group.groupKey}
                      className={`${isGroupSelected ? "flows-row-selected" : ""}${isActive ? " flows-row-active" : ""}`}
                      onClick={(e) => handleRowClick(group, e)}
                    >
                      <td className="flows-td-checkbox">
                        <input
                          type="checkbox"
                          checked={isGroupSelected}
                          ref={(el) => {
                            if (el) el.indeterminate = someGroupSelected && !isGroupSelected;
                          }}
                          onChange={() => handleSelectGroup(group)}
                        />
                      </td>
                      <td className="flows-td-source">
                        <div className="flows-source-card">
                          <div className="flows-source-card-title">
                            <span className="flows-source-server">{group.srcServer}</span>
                            <span className="flows-source-db">{group.srcDatabase}</span>
                          </div>
                          <div className="flows-source-tables">
                            {group.tables.map((t) => (
                              <span key={t.rowKey} className="flows-source-table-badge">
                                {t.schema}.{t.table}
                              </span>
                            ))}
                          </div>
                          {hasNew && <span className="flow-new-badge">new</span>}
                        </div>
                      </td>
                      <td className="flows-td-dest">
                        <div className="flows-dest-card">
                          <div className="flows-dest-card-title">
                            <span className="flows-dest-server">{group.destServer}</span>
                            <span className="flows-dest-db">{group.destDatabase}</span>
                          </div>
                          <div className="flows-dest-schema">
                            Schema: {group.destSchema}
                          </div>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>

        {selectedFlowRowKey && (
          <div className="flows-history-panel">
            <div className="flows-history-header">
              <div className="flows-history-title">
                <span className="flows-history-title-label">Execution History</span>
                {selectedParsedFlow && (
                  <span className="flows-history-title-flow">
                    {selectedParsedFlow.srcSchema}.{selectedParsedFlow.srcTable}
                  </span>
                )}
              </div>
              <button
                className="flows-history-close"
                onClick={() => setSelectedFlowRowKey(null)}
                title="Close"
              >
                <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                  <path d="M3 3L11 11M11 3L3 11" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
                </svg>
              </button>
            </div>
            <div className="flows-history-body">
              {historyEvents.length === 0 ? (
                <p className="flows-history-empty">No executions recorded for this flow.</p>
              ) : (
                historyEvents.map((evt, idx) => {
                  const isError = evt.summary.toLowerCase().includes("error") || evt.summary.toLowerCase().includes("failed");
                  const isTimeout = evt.summary.toLowerCase().includes("timeout");
                  const badgeClass = isError ? "flows-history-badge-error" : isTimeout ? "flows-history-badge-warn" : "flows-history-badge-success";
                  const hasSteps = Array.isArray(evt.steps) && evt.steps.length > 0;
                  const isExpanded = expandedHistoryIdx === idx;
                  return (
                    <div className="flows-history-item" key={idx}>
                      <div
                        className={`flows-history-item-header${hasSteps ? " flows-history-item-expandable" : ""}`}
                        onClick={() => hasSteps && setExpandedHistoryIdx(isExpanded ? null : idx)}
                      >
                        {hasSteps && (
                          <span className={`flows-history-chevron${isExpanded ? " flows-history-chevron-open" : ""}`}>
                            <svg width="10" height="10" viewBox="0 0 10 10">
                              <path d="M3 2 L7 5 L3 8" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                            </svg>
                          </span>
                        )}
                        <span className="flows-history-item-time">
                          {formatHistoryTime(evt.timestamp)}
                        </span>
                        <span className={`flows-history-item-badge ${badgeClass}`}>
                          dp run
                        </span>
                      </div>
                      <div className="flows-history-item-summary">{evt.summary}</div>
                      {evt.detail && (
                        <div className="flows-history-item-detail">{evt.detail}</div>
                      )}
                      {isExpanded && hasSteps && (
                        <div className="flows-history-steps">
                          {consolidateSteps(evt.steps!).map((step, si) => (
                            <div className="flows-history-step" key={si}>
                              <span className={`flows-history-step-icon flows-history-step-icon-${step.status}`}>
                                {step.status === "done" && (
                                  <svg width="10" height="10" viewBox="0 0 10 10">
                                    <path d="M2 5 L4.5 7.5 L8 2.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                                  </svg>
                                )}
                                {step.status === "skipped" && (
                                  <svg width="10" height="10" viewBox="0 0 10 10">
                                    <path d="M1.5 2 L4.5 5 L1.5 8" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                                    <path d="M5.5 2 L8.5 5 L5.5 8" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                                  </svg>
                                )}
                                {step.status === "running" && <span className="flows-history-step-spinner" />}
                                {step.status === "error" && (
                                  <svg width="10" height="10" viewBox="0 0 10 10">
                                    <path d="M3 3 L7 7 M7 3 L3 7" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                                  </svg>
                                )}
                              </span>
                              <span className="flows-history-step-message">{step.message}</span>
                              {step.pollCount != null && step.pollCount > 1 && (
                                <span className="flows-history-step-poll-count">x{step.pollCount}</span>
                              )}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  );
                })
              )}
            </div>
          </div>
        )}
      </div>

      {confirmDeleteOpen &&
        createPortal(
          <div className="flows-confirm-overlay" onMouseDown={() => setConfirmDeleteOpen(false)}>
            <div className="flows-confirm-dialog" onMouseDown={(e) => e.stopPropagation()}>
              <h3 className="flows-confirm-title">Confirm Delete</h3>
              <p className="flows-confirm-body">
                Are you sure you want to delete {selected.size}{" "}
                {selected.size === 1 ? "flow" : "flows"}? This action cannot be undone.
              </p>
              <div className="flows-confirm-actions">
                <button
                  className="flows-confirm-btn flows-confirm-btn-cancel"
                  onClick={() => setConfirmDeleteOpen(false)}
                >
                  Cancel
                </button>
                <button
                  className="flows-confirm-btn flows-confirm-btn-delete"
                  onClick={confirmDelete}
                >
                  Delete
                </button>
              </div>
            </div>
          </div>,
          document.body
        )}

      {confirmRunOpen &&
        createPortal(
          <div className="flows-confirm-overlay" onMouseDown={() => setConfirmRunOpen(false)}>
            <div className="flows-confirm-dialog flows-confirm-dialog-wide" onMouseDown={(e) => e.stopPropagation()}>
              <h3 className="flows-confirm-title">Confirm Run</h3>
              <p className="flows-confirm-body">
                Run {selected.size}{" "}
                {selected.size === 1 ? "flow" : "flows"}?
              </p>
              <div className="flows-confirm-list">
                <table className="flows-confirm-list-table">
                  <thead>
                    <tr>
                      <th>Source</th>
                      <th>Destination</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedGroupsForRun.map((g) => (
                      <tr key={g.groupKey}>
                        <td>
                          <span className="flows-confirm-server">{g.srcServer}</span>
                          {" / "}
                          <span>{g.srcDatabase}</span>
                          <div className="flows-confirm-tables">
                            {g.tables.map((t) => (
                              <span key={t.rowKey} className="flows-confirm-table-badge">{t.schema}.{t.table}</span>
                            ))}
                          </div>
                        </td>
                        <td>
                          <span className="flows-confirm-server">{g.destServer}</span>
                          {" / "}
                          <span>{g.destDatabase}</span>
                          <div className="flows-confirm-schema">Schema: {g.destSchema}</div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="flows-confirm-actions">
                <button
                  className="flows-confirm-btn flows-confirm-btn-cancel"
                  onClick={() => setConfirmRunOpen(false)}
                >
                  Cancel
                </button>
                <button
                  className="flows-confirm-btn flows-confirm-btn-run"
                  onClick={confirmRun}
                >
                  Run
                </button>
              </div>
            </div>
          </div>,
          document.body
        )}
    </div>
  );
}
