import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import "./ConnectionsPanel.css";

export interface SavedConnection {
  rowKey: string;
  connectionType: string;
  serverName: string;
  authentication: string;
  databaseName: string;
  encrypt: string;
  trustServerCertificate: boolean;
  createdAt: string;
}

export interface TableInfo {
  schema: string;
  name: string;
  fileFormatId?: string;
}

export interface QueryInfo {
  rowKey: string;
  connectionRowKey: string;
  queryText: string;
  createdAt: string;
}

interface ContextMenuState {
  x: number;
  y: number;
  rowKey: string;
  schema: string;
  tableName: string;
  isQuery: boolean;
  isConnection: boolean;
}

interface ConnectionsPanelProps {
  connections: SavedConnection[];
  connectionTables: Record<string, TableInfo[]>;
  connectionQueries: Record<string, QueryInfo[]>;
  loadingTables: Set<string>;
  dryRunningTables: Set<string>;
  selectedTable: { rowKey: string; schema: string; tableName: string } | null;
  selectedQuery: { connectionRowKey: string; queryRowKey: string; queryText: string } | null;
  tableTabCounts: Record<string, number>;
  expanded: Set<string>;
  onExpandedChange: (next: Set<string>) => void;
  width: number;
  onExpandConnection: (rowKey: string) => void;
  onTableClick: (rowKey: string, schema: string, tableName: string) => void;
  onQueryClick: (connectionRowKey: string, queryRowKey: string, queryText: string) => void;
  onReloadPreview: () => void;
  onRefreshConnection: (rowKey: string) => void;
  onDryRun: (rowKey: string, schema: string, tableName: string) => void;
  onFullRun: (rowKey: string, schema: string, tableName: string) => void;
  onSwitchPanel: (panel: "connections" | "flows") => void;
  onWidthChange?: (width: number) => void;
  flowsBadgeCount?: number;
  connectionsBadgeCount?: number;
  newConnectionRowKeys?: Set<string>;
  onDismissNewBadge?: (rowKey: string) => void;
  checkedTables?: Set<string>;
  onCheckedTablesChange?: (next: Set<string>) => void;
  onProfileData?: (tableKeys: string[]) => void;
  onApplySanitization?: (tableKeys: string[]) => void;
}

const MIN_WIDTH = 200;
const MAX_WIDTH = 500;

export default function ConnectionsPanel({
  connections,
  connectionTables,
  connectionQueries,
  loadingTables,
  dryRunningTables,
  selectedTable,
  selectedQuery,
  tableTabCounts,
  expanded,
  onExpandedChange,
  width,
  onExpandConnection,
  onTableClick,
  onQueryClick,
  onReloadPreview,
  onRefreshConnection,
  onDryRun,
  onFullRun,
  onSwitchPanel,
  onWidthChange,
  flowsBadgeCount,
  connectionsBadgeCount,
  newConnectionRowKeys,
  onDismissNewBadge,
  checkedTables,
  onCheckedTablesChange,
  onProfileData,
  onApplySanitization,
}: ConnectionsPanelProps) {
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [searchText, setSearchText] = useState("");
  const [actionsOpen, setActionsOpen] = useState(false);
  const actionsRef = useRef<HTMLDivElement>(null);
  const isResizing = useRef(false);
  const panelRef = useRef<HTMLDivElement>(null);

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isResizing.current = true;
    const startX = e.clientX;
    const startWidth = width;

    function onMouseMove(ev: MouseEvent) {
      if (!isResizing.current) return;
      const newWidth = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, startWidth + ev.clientX - startX));
      onWidthChange?.(newWidth);
    }

    function onMouseUp() {
      isResizing.current = false;
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    }

    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
  }, [width, onWidthChange]);

  useEffect(() => {
    return () => {
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
  }, []);

  useEffect(() => {
    if (isResizing.current) return;
    const el = panelRef.current;
    if (!el) return;
    const prev = el.style.width;
    el.style.width = "max-content";
    const measured = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, el.scrollWidth));
    el.style.width = prev;
    if (measured !== width) {
      onWidthChange?.(measured);
    }
  }, [connections, connectionTables, connectionQueries, expanded]);

  useEffect(() => {
    if (!contextMenu) return;
    function dismiss() { setContextMenu(null); }
    document.addEventListener("mousedown", dismiss);
    return () => document.removeEventListener("mousedown", dismiss);
  }, [contextMenu]);

  function handleTableContextMenu(
    e: React.MouseEvent,
    rowKey: string,
    schema: string,
    tableName: string,
    isQuery = false,
    isConnection = false,
  ) {
    e.preventDefault();
    e.stopPropagation();
    setContextMenu({ x: e.clientX, y: e.clientY, rowKey, schema, tableName, isQuery, isConnection });
  }

  const grouped = useMemo(() => {
    const groups: Record<string, SavedConnection[]> = {};
    for (const conn of connections) {
      const type = conn.connectionType || "Other";
      if (!groups[type]) groups[type] = [];
      groups[type].push(conn);
    }
    for (const type of Object.keys(groups)) {
      groups[type].sort((a, b) => {
        const ta = a.createdAt ?? "";
        const tb = b.createdAt ?? "";
        return tb.localeCompare(ta);
      });
    }
    return groups;
  }, [connections]);

  function handleToggle(rowKey: string) {
    if (newConnectionRowKeys?.has(rowKey)) {
      onDismissNewBadge?.(rowKey);
    }
    const next = new Set(expanded);
    if (next.has(rowKey)) {
      next.delete(rowKey);
    } else {
      next.add(rowKey);
      onExpandConnection(rowKey);
    }
    onExpandedChange(next);
  }

  useEffect(() => {
    if (!actionsOpen) return;
    function dismiss(e: MouseEvent) {
      if (actionsRef.current && !actionsRef.current.contains(e.target as Node)) {
        setActionsOpen(false);
      }
    }
    document.addEventListener("mousedown", dismiss);
    return () => document.removeEventListener("mousedown", dismiss);
  }, [actionsOpen]);

  function handleCheckboxToggle(e: React.MouseEvent, key: string) {
    e.stopPropagation();
    const next = new Set(checkedTables);
    if (next.has(key)) {
      next.delete(key);
    } else {
      next.add(key);
    }
    onCheckedTablesChange?.(next);
  }

  const isExpanded = (rowKey: string) => expanded.has(rowKey);
  const isLoading = (rowKey: string) => loadingTables.has(rowKey);

  const searchLower = searchText.toLowerCase();

  function filteredTables(rowKey: string) {
    const t = connectionTables[rowKey];
    if (!t || !searchText) return t;
    return t.filter((item) => `${item.schema}.${item.name}`.toLowerCase().includes(searchLower));
  }

  function filteredQueries(rowKey: string) {
    const q = connectionQueries[rowKey];
    if (!q || !searchText) return q;
    return q.filter((item) => item.queryText.toLowerCase().includes(searchLower));
  }

  return (
    <>
    <div ref={panelRef} className="connections-panel" style={{ width }}>
      <div className="connections-panel-header">
        <div className="panel-switch-icons">
          <button
            className="panel-switch-btn panel-switch-btn-active"
            aria-label="Connections"
            data-connections-btn
            onClick={() => onSwitchPanel("connections")}
          >
            Connections
            {!!connectionsBadgeCount && connectionsBadgeCount > 0 && (
              <span className="connections-badge">{connectionsBadgeCount}</span>
            )}
          </button>
          <button
            className="panel-switch-btn"
            aria-label="Flows"
            data-flows-btn
            onClick={() => onSwitchPanel("flows")}
          >
            Flows
            {!!flowsBadgeCount && flowsBadgeCount > 0 && (
              <span className="flows-badge">{flowsBadgeCount}</span>
            )}
          </button>
        </div>
      </div>
      <div className="conn-toolbar">
        <div className="conn-search-wrapper">
          <svg className="conn-search-icon" width="14" height="14" viewBox="0 0 14 14" fill="none">
            <circle cx="6" cy="6" r="4.5" stroke="currentColor" strokeWidth="1.3" />
            <path d="M9.5 9.5L13 13" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
          </svg>
          <input
            className="conn-search-input"
            type="text"
            placeholder="Search tables..."
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
          />
        </div>
        <div className="conn-actions-wrapper" ref={actionsRef}>
          <button
            className="conn-actions-btn"
            onClick={() => setActionsOpen((v) => !v)}
          >
            Actions
            <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
              <path d="M2 4L5 7L8 4" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </button>
          {actionsOpen && (
            <div className="conn-actions-dropdown">
              <div
                className="conn-actions-dropdown-item"
                onClick={() => {
                  setActionsOpen(false);
                  onProfileData?.(Array.from(checkedTables ?? []));
                }}
              >
                Data Sanitize - Profile Data
              </div>
              <div
                className="conn-actions-dropdown-item"
                onClick={() => {
                  setActionsOpen(false);
                  onApplySanitization?.(Array.from(checkedTables ?? []));
                }}
              >
                Data Sanitize - Apply Sanitization
              </div>
            </div>
          )}
        </div>
      </div>
      {connections.length === 0 ? (
        <p className="connections-panel-empty">No saved connections.</p>
      ) : (
        <div className="connections-list">
          {Object.entries(grouped).map(([type, conns]) => {
            const visibleConns = searchText
              ? conns.filter((conn) => {
                  const ft = filteredTables(conn.rowKey);
                  const fq = filteredQueries(conn.rowKey);
                  return (ft && ft.length > 0) || (fq && fq.length > 0) || !isExpanded(conn.rowKey);
                })
              : conns;
            if (visibleConns.length === 0) return null;
            return (
            <div key={type} className="conn-group">
              <ul className="conn-group-list">
                {visibleConns.map((conn) => {
                  const fTables = filteredTables(conn.rowKey);
                  const fQueries = filteredQueries(conn.rowKey);
                  return (
                  <li key={conn.rowKey} className="connections-list-entry">
                    <div
                      className="connections-list-item"
                      onClick={() => handleToggle(conn.rowKey)}
                      onContextMenu={(e) => handleTableContextMenu(e, conn.rowKey, "", "", false, true)}
                    >
                      <button
                        className={`conn-expand-btn ${isExpanded(conn.rowKey) ? "expanded" : ""}`}
                        aria-label={isExpanded(conn.rowKey) ? "Collapse" : "Expand"}
                      >
                        <svg width="12" height="12" viewBox="0 0 12 12">
                          <path d="M4 2 L8 6 L4 10" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                        </svg>
                      </button>
                      <div className="conn-details">
                        <span className="conn-server">{conn.serverName}</span>
                        {conn.databaseName && (
                          <span className="conn-db">{conn.databaseName}</span>
                        )}
                      </div>
                      {newConnectionRowKeys?.has(conn.rowKey) && (
                        <span className="conn-new-badge">new</span>
                      )}
                    </div>
                    {isExpanded(conn.rowKey) && (
                      <div className="conn-tables">
                        {isLoading(conn.rowKey) ? (
                          <div className="conn-tables-loading">Loading...</div>
                        ) : (
                          <>
                            {fQueries && fQueries.length > 0 && (
                              <ul className="conn-tables-list">
                                {fQueries.map((q) => {
                                  const isSelected = selectedQuery?.connectionRowKey === conn.rowKey
                                    && selectedQuery?.queryRowKey === q.rowKey;
                                  const label = q.queryText.length > 40
                                    ? q.queryText.substring(0, 40) + "..."
                                    : q.queryText;
                                  return (
                                    <li
                                      key={q.rowKey}
                                      className={`conn-table-item conn-query-item${isSelected ? " conn-table-item-selected" : ""}`}
                                      onClick={() => onQueryClick(conn.rowKey, q.rowKey, q.queryText)}
                                      onContextMenu={(e) => handleTableContextMenu(e, conn.rowKey, "", q.rowKey, true)}
                                    >
                                      <svg className="conn-table-icon conn-query-icon" width="14" height="14" viewBox="0 0 14 14">
                                        <path d="M2 2 L12 2 L12 12 L2 12 Z" fill="none" stroke="currentColor" strokeWidth="1" rx="1" />
                                        <path d="M4 5 L10 5 M4 7 L9 7 M4 9 L7 9" stroke="currentColor" strokeWidth="0.8" strokeLinecap="round" />
                                      </svg>
                                      <span className="conn-table-name" title={q.queryText}>{label}</span>
                                    </li>
                                  );
                                })}
                              </ul>
                            )}
                            {fTables ? (
                              fTables.length === 0 && (!fQueries || fQueries.length === 0) ? (
                                <div className="conn-tables-empty">No tables found.</div>
                              ) : (
                                <ul className="conn-tables-list">
                                  {fTables.map((t) => {
                                    const tKey = `${conn.rowKey}:${t.schema}:${t.name}`;
                                    const isSelected = selectedTable?.rowKey === conn.rowKey
                                      && selectedTable?.schema === t.schema
                                      && selectedTable?.tableName === t.name;
                                    const isDryRunning = dryRunningTables.has(tKey);
                                    const isChecked = checkedTables?.has(tKey) ?? false;
                                    return (
                                      <li
                                        key={`${t.schema}.${t.name}`}
                                        className={`conn-table-item${isSelected ? " conn-table-item-selected" : ""}`}
                                        onClick={() => onTableClick(conn.rowKey, t.schema, t.name)}
                                        onContextMenu={(e) => handleTableContextMenu(e, conn.rowKey, t.schema, t.name)}
                                      >
                                        <input
                                          type="checkbox"
                                          className="conn-table-checkbox"
                                          checked={isChecked}
                                          onClick={(e) => handleCheckboxToggle(e as unknown as React.MouseEvent, tKey)}
                                          onChange={() => {}}
                                        />
                                        <span className="conn-table-icon-wrapper">
                                          {isDryRunning && (
                                            <svg className="conn-table-running-icon" width="14" height="14" viewBox="0 0 14 14">
                                              <circle cx="7" cy="7" r="5.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeDasharray="20 12" />
                                            </svg>
                                          )}
                                          <svg className="conn-table-icon" width="14" height="14" viewBox="0 0 14 14">
                                            <rect x="1" y="1" width="12" height="12" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1" />
                                            <line x1="1" y1="5" x2="13" y2="5" stroke="currentColor" strokeWidth="1" />
                                            <line x1="1" y1="9" x2="13" y2="9" stroke="currentColor" strokeWidth="1" />
                                            <line x1="5" y1="5" x2="5" y2="13" stroke="currentColor" strokeWidth="1" />
                                          </svg>
                                        </span>
                                        <span className="conn-table-name">{t.schema}.{t.name}</span>
                                        {tableTabCounts[tKey] && (
                                          <span className="conn-table-tab-badge">
                                            {tableTabCounts[tKey] === 1 ? "1 tab" : `${tableTabCounts[tKey]} tabs`}
                                          </span>
                                        )}
                                      </li>
                                    );
                                  })}
                                </ul>
                              )
                            ) : null}
                          </>
                        )}
                      </div>
                    )}
                  </li>
                  );
                })}
              </ul>
            </div>
            );
          })}
        </div>
      )}
      <div
        className="connections-panel-resize"
        onMouseDown={handleResizeStart}
      />
    </div>
    {contextMenu && createPortal(
      <div
        className="conn-context-menu"
        style={{ top: contextMenu.y, left: contextMenu.x }}
        onMouseDown={(e) => e.stopPropagation()}
      >
        {contextMenu.isConnection ? (
          <div
            className="conn-context-menu-item"
            onClick={() => {
              const { rowKey } = contextMenu;
              setContextMenu(null);
              onRefreshConnection(rowKey);
            }}
          >
            Refresh
          </div>
        ) : (
          <>
            {contextMenu.isQuery ? (
              <div
                className="conn-context-menu-item"
                onClick={() => {
                  setContextMenu(null);
                  onReloadPreview();
                }}
              >
                Refresh
              </div>
            ) : (
              <>
                <div className="conn-context-menu-parent">
                  Sample
                  <div className="conn-context-submenu">
                    <div
                      className="conn-context-menu-item"
                      onClick={() => {
                        setContextMenu(null);
                        onReloadPreview();
                      }}
                    >
                      Sample Data
                    </div>
                    <div
                      className="conn-context-menu-item conn-context-menu-item-disabled"
                      title="Coming soon"
                    >
                      Sample from Query
                    </div>
                  </div>
                </div>
                <div className="conn-context-menu-parent">
                  Data Protection
                  <div className="conn-context-submenu">
                    <div
                      className="conn-context-menu-item"
                      onClick={() => {
                        const { rowKey, schema, tableName } = contextMenu;
                        setContextMenu(null);
                        onDryRun(rowKey, schema, tableName);
                      }}
                    >
                      Preview
                      <span className="conn-context-menu-subtitle">View sample output without saving changes</span>
                    </div>
                    {(() => {
                      const t = connectionTables[contextMenu.rowKey]?.find(
                        (ti) => ti.schema === contextMenu.schema && ti.name === contextMenu.tableName
                      );
                      const hasFormat = !!t?.fileFormatId;
                      return (
                        <div
                          className={`conn-context-menu-item${hasFormat ? "" : " conn-context-menu-item-disabled"}`}
                          title={hasFormat ? undefined : "You must run Preview first."}
                          onClick={() => {
                            if (!hasFormat) return;
                            const { rowKey, schema, tableName } = contextMenu;
                            setContextMenu(null);
                            onFullRun(rowKey, schema, tableName);
                          }}
                        >
                          Run
                          <span className="conn-context-menu-subtitle">Apply to full dataset</span>
                        </div>
                      );
                    })()}
                  </div>
                </div>
              </>
            )}
          </>
        )}
      </div>,
      document.body,
    )}
    </>
  );
}
