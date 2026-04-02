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
  starredTables?: Set<string>;
  onStarredTablesChange?: (next: Set<string>) => void;
  tableColumns?: Record<string, { name: string; type: string }[]>;
  onFetchTableColumns?: (rowKey: string, schema: string, tableName: string) => void;
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
  onTableClick: _onTableClick,
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
  starredTables,
  onStarredTablesChange,
  tableColumns,
  onFetchTableColumns,
}: ConnectionsPanelProps) {
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [searchText, setSearchText] = useState("");
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [actionsOpen, setActionsOpen] = useState(false);
  const [selectMenuOpen, setSelectMenuOpen] = useState(false);
  const actionsRef = useRef<HTMLDivElement>(null);
  const selectRef = useRef<HTMLDivElement>(null);
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

  useEffect(() => {
    if (!selectMenuOpen) return;
    function dismiss(e: MouseEvent) {
      if (selectRef.current && !selectRef.current.contains(e.target as Node)) {
        setSelectMenuOpen(false);
      }
    }
    document.addEventListener("mousedown", dismiss);
    return () => document.removeEventListener("mousedown", dismiss);
  }, [selectMenuOpen]);

  const allTableKeys = useMemo(() => {
    const keys: string[] = [];
    for (const conn of connections) {
      const tables = connectionTables[conn.rowKey];
      if (!tables) continue;
      for (const t of tables) {
        keys.push(`${conn.rowKey}:${t.schema}:${t.name}`);
      }
    }
    return keys;
  }, [connections, connectionTables]);

  const visibleTableKeys = useMemo(() => {
    const keys: string[] = [];
    for (const conn of connections) {
      if (!expanded.has(conn.rowKey)) continue;
      const tables = filteredTables(conn.rowKey);
      if (!tables) continue;
      for (const t of tables) {
        keys.push(`${conn.rowKey}:${t.schema}:${t.name}`);
      }
    }
    return keys;
  }, [connections, connectionTables, expanded, searchText]);

  function handleSelectAll() {
    onCheckedTablesChange?.(new Set(allTableKeys));
    setSelectMenuOpen(false);
  }

  function handleSelectNone() {
    onCheckedTablesChange?.(new Set());
    setSelectMenuOpen(false);
  }

  function handleSelectCheckboxClick() {
    if (hasChecked) {
      onCheckedTablesChange?.(new Set());
    } else {
      onCheckedTablesChange?.(new Set(visibleTableKeys));
    }
  }

  function handleSelectStarred() {
    const next = new Set<string>();
    for (const key of allTableKeys) {
      if (starredTables?.has(key)) next.add(key);
    }
    onCheckedTablesChange?.(next);
    setSelectMenuOpen(false);
  }

  function handleSelectUnstarred() {
    const next = new Set<string>();
    for (const key of allTableKeys) {
      if (!starredTables?.has(key)) next.add(key);
    }
    onCheckedTablesChange?.(next);
    setSelectMenuOpen(false);
  }

  function handleRefreshAll() {
    for (const conn of connections) {
      onRefreshConnection(conn.rowKey);
    }
  }

  function handleStarToggle(e: React.MouseEvent, key: string) {
    e.stopPropagation();
    const next = new Set(starredTables);
    if (next.has(key)) {
      next.delete(key);
    } else {
      next.add(key);
    }
    onStarredTablesChange?.(next);
  }

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

  const hasChecked = (checkedTables?.size ?? 0) > 0;

  function handleTableExpandToggle(tKey: string, rowKey: string, schema: string, tableName: string) {
    const next = new Set(expandedTables);
    if (next.has(tKey)) {
      next.delete(tKey);
    } else {
      next.add(tKey);
      if (!tableColumns?.[tKey]) {
        onFetchTableColumns?.(rowKey, schema, tableName);
      }
    }
    setExpandedTables(next);
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
      <div className="conn-google-search-box">
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
        </div>
        <div className="conn-icon-bar">
        <div className="conn-icon-btn-wrapper conn-select-split" ref={selectRef} data-tooltip="Select">
          <button
            className="conn-icon-btn conn-select-arrow-btn"
            aria-label="Select options"
            onClick={() => setSelectMenuOpen((v) => !v)}
          >
            <span className="conn-select-arrow-checkbox-space" />
            <svg className="conn-icon-btn-caret" width="12" height="12" viewBox="0 0 12 12" fill="none">
              <path d="M1.5 3.5L6 8.5L10.5 3.5" fill="currentColor" />
            </svg>
          </button>
          <button
            className="conn-icon-btn conn-select-checkbox-btn"
            aria-label={hasChecked ? "Deselect all" : "Select all visible"}
            onClick={(e) => { e.stopPropagation(); handleSelectCheckboxClick(); }}
          >
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <rect x="2" y="3" width="10" height="10" rx="1.5" stroke="currentColor" strokeWidth="1.4" fill="none" />
              {hasChecked && (
                <line x1="4.5" y1="8" x2="9.5" y2="8" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
              )}
            </svg>
          </button>
          {selectMenuOpen && (
            <div className="conn-icon-dropdown">
              <div className="conn-icon-dropdown-item" onClick={handleSelectAll}>All</div>
              <div className="conn-icon-dropdown-item" onClick={handleSelectNone}>None</div>
              <div className="conn-icon-dropdown-item" onClick={handleSelectStarred}>Starred</div>
              <div className="conn-icon-dropdown-item" onClick={handleSelectUnstarred}>Unstarred</div>
            </div>
          )}
        </div>
        {hasChecked ? (
          <>
            <div className="conn-icon-btn-wrapper" data-tooltip="Profile Data">
              <button
                className="conn-icon-btn"
                aria-label="Profile Data"
                onClick={() => onProfileData?.(Array.from(checkedTables ?? []))}
              >
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                  <circle cx="6.5" cy="6.5" r="4.8" stroke="currentColor" strokeWidth="1.4" />
                  <circle cx="6.5" cy="6.5" r="3.2" stroke="currentColor" strokeWidth="0.8" opacity="0.45" />
                  <path d="M10.2 10.2L14.5 14.5" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round" />
                </svg>
              </button>
            </div>
            <div className="conn-icon-btn-wrapper" data-tooltip="Apply Sanitization">
              <button
                className="conn-icon-btn"
                aria-label="Apply Sanitization"
                onClick={() => onApplySanitization?.(Array.from(checkedTables ?? []))}
              >
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                  <path d="M8 1.5L2.5 4.5V7.5C2.5 11 5 13.5 8 14.5C11 13.5 13.5 11 13.5 7.5V4.5L8 1.5Z" stroke="currentColor" strokeWidth="1.3" strokeLinejoin="round" fill="none" />
                  <path d="M5.5 8L7.2 9.7L10.5 6.3" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              </button>
            </div>
          </>
        ) : (
          <div className="conn-icon-btn-wrapper" data-tooltip="Refresh">
            <button
              className="conn-icon-btn"
              aria-label="Refresh"
              onClick={handleRefreshAll}
            >
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <path d="M13.5 8A5.5 5.5 0 0 1 3.05 10" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
                <path d="M2.5 8A5.5 5.5 0 0 1 12.95 6" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
                <path d="M12.95 3V6H9.95" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
                <path d="M3.05 13V10H6.05" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </button>
          </div>
        )}
        <div className="conn-icon-btn-wrapper" ref={actionsRef} data-tooltip="Action">
          <button
            className="conn-icon-btn"
            aria-label="Action"
            onClick={() => setActionsOpen((v) => !v)}
          >
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <circle cx="8" cy="3.5" r="1.2" fill="currentColor" />
              <circle cx="8" cy="8" r="1.2" fill="currentColor" />
              <circle cx="8" cy="12.5" r="1.2" fill="currentColor" />
            </svg>
          </button>
          {actionsOpen && (
            <div className="conn-icon-dropdown">
              <div
                className="conn-icon-dropdown-item"
                onClick={() => {
                  setActionsOpen(false);
                  onProfileData?.(Array.from(checkedTables ?? []));
                }}
              >
                Data Sanitize - Profile Data
              </div>
              <div
                className="conn-icon-dropdown-item"
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
                                    const isTableExpanded = expandedTables.has(tKey);
                                    const cols = tableColumns?.[tKey];
                                    return (
                                      <li
                                        key={`${t.schema}.${t.name}`}
                                        className={`conn-table-entry${isTableExpanded ? " conn-table-entry-expanded" : ""}`}
                                      >
                                        <div
                                          className={`conn-table-item${isSelected ? " conn-table-item-selected" : ""}`}
                                          onClick={() => handleTableExpandToggle(tKey, conn.rowKey, t.schema, t.name)}
                                        >
                                          <input
                                            type="checkbox"
                                            className="conn-table-checkbox"
                                            checked={isChecked}
                                            onClick={(e) => handleCheckboxToggle(e as unknown as React.MouseEvent, tKey)}
                                            onChange={() => {}}
                                          />
                                          <svg
                                            className={`conn-table-star${starredTables?.has(tKey) ? " conn-table-star-active" : ""}`}
                                            width="14"
                                            height="14"
                                            viewBox="0 0 14 14"
                                            onClick={(e) => handleStarToggle(e, tKey)}
                                          >
                                            <path
                                              d="M7 1.5L8.76 5.1L12.7 5.64L9.85 8.42L10.52 12.34L7 10.48L3.48 12.34L4.15 8.42L1.3 5.64L5.24 5.1L7 1.5Z"
                                              fill={starredTables?.has(tKey) ? "#f5c518" : "#fff"}
                                              stroke={starredTables?.has(tKey) ? "#f5c518" : "#555"}
                                              strokeWidth="1"
                                              strokeLinejoin="round"
                                            />
                                          </svg>
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
                                        </div>
                                        {isTableExpanded && (
                                          <ul className="conn-table-columns">
                                            {cols ? (
                                              cols.map((col) => (
                                                <li key={col.name} className="conn-table-column-row">
                                                  <span className="conn-table-column-name">{col.name}</span>
                                                  <span className="conn-table-column-type">{col.type}</span>
                                                </li>
                                              ))
                                            ) : (
                                              <li className="conn-table-column-row conn-table-columns-loading">Loading columns...</li>
                                            )}
                                          </ul>
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
