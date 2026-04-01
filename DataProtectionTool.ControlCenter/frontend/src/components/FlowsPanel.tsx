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
  onSwitchPanel: (panel: "connections" | "flows") => void;
  onRunFlows?: (flows: FlowItem[]) => void;
}

type SortField = "srcDatabase" | "srcSchema" | "srcTable" | "destDatabase" | "destSchema" | "destTable";
type SortDir = "asc" | "desc";

interface ParsedFlow {
  rowKey: string;
  srcDatabase: string;
  srcSchema: string;
  srcTable: string;
  destDatabase: string;
  destSchema: string;
  destTable: string;
}

const COLUMNS: { key: SortField; label: string }[] = [
  { key: "srcDatabase", label: "Source Database" },
  { key: "srcSchema", label: "Source Schema" },
  { key: "srcTable", label: "Source Table" },
  { key: "destDatabase", label: "Dest Database" },
  { key: "destSchema", label: "Dest Schema" },
  { key: "destTable", label: "Dest Table" },
];

const DEFAULT_COL_WIDTH = 180;
const MIN_COL_WIDTH = 80;

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
    srcDatabase: src?.databaseName || src?.serverName || "—",
    srcSchema: src?.schema || "—",
    srcTable: src?.tableName || "—",
    destDatabase: dest?.databaseName || dest?.serverName || "—",
    destSchema: dest?.schema || "—",
    destTable: dest?.tableName || "—",
  };
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
  onSwitchPanel,
  onRunFlows,
}: FlowsPanelProps) {
  const [flows, setFlows] = useState<FlowItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchText, setSearchText] = useState("");
  const [sortField, setSortField] = useState<SortField | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [colWidths, setColWidths] = useState<number[]>(COLUMNS.map(() => DEFAULT_COL_WIDTH));
  const [actionOpen, setActionOpen] = useState(false);
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);
  const [confirmRunOpen, setConfirmRunOpen] = useState(false);
  const [selectedFlowRowKey, setSelectedFlowRowKey] = useState<string | null>(null);
  const [expandedHistoryIdx, setExpandedHistoryIdx] = useState<number | null>(null);
  const actionRef = useRef<HTMLDivElement>(null);

  const resizingCol = useRef<number | null>(null);
  const resizeStartX = useRef(0);
  const resizeStartW = useRef(0);

  const fetchFlows = useCallback(async () => {
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
  }, [agentPath]);

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

  useEffect(() => {
    function onMouseMove(e: MouseEvent) {
      if (resizingCol.current === null) return;
      const idx = resizingCol.current;
      const delta = e.clientX - resizeStartX.current;
      const newW = Math.max(MIN_COL_WIDTH, resizeStartW.current + delta);
      setColWidths((prev) => {
        const next = [...prev];
        next[idx] = newW;
        return next;
      });
    }
    function onMouseUp() {
      resizingCol.current = null;
    }
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
    return () => {
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
    };
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

  function handleRowClick(rowKey: string, e: React.MouseEvent) {
    const target = e.target as HTMLElement;
    if (target.tagName === "INPUT" || target.closest(".flows-td-checkbox")) return;
    setSelectedFlowRowKey((prev) => (prev === rowKey ? null : rowKey));
  }

  const parsed = useMemo(() => flows.map(parseFlow), [flows]);

  const selectedParsedFlow = useMemo(
    () => (selectedFlowRowKey ? parsed.find((f) => f.rowKey === selectedFlowRowKey) ?? null : null),
    [parsed, selectedFlowRowKey],
  );

  const filtered = useMemo(() => {
    if (!searchText.trim()) return parsed;
    const q = searchText.toLowerCase();
    return parsed.filter((f) => {
      const raw = flows.find((r) => r.rowKey === f.rowKey);
      return (
        f.srcDatabase.toLowerCase().includes(q) ||
        f.srcSchema.toLowerCase().includes(q) ||
        f.srcTable.toLowerCase().includes(q) ||
        f.destDatabase.toLowerCase().includes(q) ||
        f.destSchema.toLowerCase().includes(q) ||
        f.destTable.toLowerCase().includes(q) ||
        f.rowKey.toLowerCase().includes(q) ||
        (raw?.sourceJson ?? "").toLowerCase().includes(q) ||
        (raw?.destJson ?? "").toLowerCase().includes(q)
      );
    });
  }, [parsed, flows, searchText]);

  const sorted = useMemo(() => {
    if (!sortField) return filtered;
    const dir = sortDir === "asc" ? 1 : -1;
    return [...filtered].sort((a, b) => {
      const va = a[sortField].toLowerCase();
      const vb = b[sortField].toLowerCase();
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
    const filteredKeys = new Set(sorted.map((f) => f.rowKey));
    const allSelected = sorted.length > 0 && sorted.every((f) => selected.has(f.rowKey));
    if (allSelected) {
      setSelected((prev) => {
        const next = new Set(prev);
        for (const k of filteredKeys) next.delete(k);
        return next;
      });
    } else {
      setSelected((prev) => new Set([...prev, ...filteredKeys]));
    }
  }

  function handleSelectRow(rowKey: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(rowKey)) next.delete(rowKey);
      else next.add(rowKey);
      return next;
    });
  }

  const allFilteredSelected = sorted.length > 0 && sorted.every((f) => selected.has(f.rowKey));
  const someFilteredSelected = sorted.some((f) => selected.has(f.rowKey));

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

  const selectedParsedForRun = useMemo(
    () => parsed.filter((f) => selected.has(f.rowKey)),
    [parsed, selected],
  );

  function handleResizeStart(e: React.MouseEvent, colIdx: number) {
    e.preventDefault();
    e.stopPropagation();
    resizingCol.current = colIdx;
    resizeStartX.current = e.clientX;
    resizeStartW.current = colWidths[colIdx];
  }

  return (
    <div className="flows-panel-full">
      <div className="flows-panel-header">
        <div className="flows-panel-header-left">
          <div className="panel-switch-icons">
            <button
              className="panel-switch-btn"
              title="Connections"
              aria-label="Connections"
              onClick={() => onSwitchPanel("connections")}
            >
              <svg width="24" height="24" viewBox="0 0 16 16" fill="none">
                <circle cx="4" cy="4" r="2" stroke="currentColor" strokeWidth="1.3" />
                <circle cx="12" cy="4" r="2" stroke="currentColor" strokeWidth="1.3" />
                <circle cx="8" cy="12" r="2" stroke="currentColor" strokeWidth="1.3" />
                <path d="M4 6V9L8 10M12 6V9L8 10" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </button>
            <button
              className="panel-switch-btn panel-switch-btn-active"
              title="Flows"
              aria-label="Flows"
              data-flows-btn
              onClick={() => onSwitchPanel("flows")}
            >
              <svg width="24" height="24" viewBox="0 0 16 16" fill="none">
                <path d="M3 4H7M3 8H10M3 12H13" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
                <path d="M12 3L14 4L12 5" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </button>
          </div>
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
            <table className="flows-table">
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
                  {COLUMNS.map((col, i) => (
                    <th
                      key={col.key}
                      className="flows-th"
                      style={{ width: colWidths[i] }}
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
                      <div
                        className="flows-col-resize"
                        onMouseDown={(e) => handleResizeStart(e, i)}
                      />
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sorted.map((flow) => (
                  <tr
                    key={flow.rowKey}
                    className={`${selected.has(flow.rowKey) ? "flows-row-selected" : ""}${selectedFlowRowKey === flow.rowKey ? " flows-row-active" : ""}`}
                    onClick={(e) => handleRowClick(flow.rowKey, e)}
                  >
                    <td className="flows-td-checkbox">
                      <input
                        type="checkbox"
                        checked={selected.has(flow.rowKey)}
                        onChange={() => handleSelectRow(flow.rowKey)}
                      />
                    </td>
                    <td style={{ width: colWidths[0] }}>{flow.srcDatabase}</td>
                    <td style={{ width: colWidths[1] }}>{flow.srcSchema}</td>
                    <td style={{ width: colWidths[2] }}>{flow.srcTable}</td>
                    <td style={{ width: colWidths[3] }}>{flow.destDatabase}</td>
                    <td style={{ width: colWidths[4] }}>{flow.destSchema}</td>
                    <td style={{ width: colWidths[5] }}>{flow.destTable}</td>
                  </tr>
                ))}
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
                Run {selectedParsedForRun.length}{" "}
                {selectedParsedForRun.length === 1 ? "flow" : "flows"}?
              </p>
              <div className="flows-confirm-list">
                <table className="flows-confirm-list-table">
                  <thead>
                    <tr>
                      <th>Source Database</th>
                      <th>Source Schema</th>
                      <th>Source Table</th>
                      <th>Dest Database</th>
                      <th>Dest Schema</th>
                      <th>Dest Table</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedParsedForRun.map((f) => (
                      <tr key={f.rowKey}>
                        <td>{f.srcDatabase}</td>
                        <td>{f.srcSchema}</td>
                        <td>{f.srcTable}</td>
                        <td>{f.destDatabase}</td>
                        <td>{f.destSchema}</td>
                        <td>{f.destTable}</td>
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
