import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
}

interface ConnectionsPanelProps {
  connections: SavedConnection[];
  connectionTables: Record<string, TableInfo[]>;
  connectionQueries: Record<string, QueryInfo[]>;
  loadingTables: Set<string>;
  selectedTable: { rowKey: string; schema: string; tableName: string } | null;
  selectedQuery: { connectionRowKey: string; queryRowKey: string; queryText: string } | null;
  onExpandConnection: (rowKey: string) => void;
  onTableClick: (rowKey: string, schema: string, tableName: string) => void;
  onQueryClick: (connectionRowKey: string, queryRowKey: string, queryText: string) => void;
  onReloadPreview: () => void;
  onDryRun: (rowKey: string, schema: string, tableName: string) => void;
  onClose: () => void;
  onWidthChange?: (width: number) => void;
}

const MIN_WIDTH = 200;
const MAX_WIDTH = 500;
const DEFAULT_WIDTH = 260;

export default function ConnectionsPanel({
  connections,
  connectionTables,
  connectionQueries,
  loadingTables,
  selectedTable,
  selectedQuery,
  onExpandConnection,
  onTableClick,
  onQueryClick,
  onReloadPreview,
  onDryRun,
  onClose,
  onWidthChange,
}: ConnectionsPanelProps) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [width, setWidth] = useState(DEFAULT_WIDTH);
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const isResizing = useRef(false);

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isResizing.current = true;
    const startX = e.clientX;
    const startWidth = width;

    function onMouseMove(ev: MouseEvent) {
      if (!isResizing.current) return;
      const newWidth = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, startWidth + ev.clientX - startX));
      setWidth(newWidth);
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
  ) {
    e.preventDefault();
    setContextMenu({ x: e.clientX, y: e.clientY, rowKey, schema, tableName, isQuery });
  }

  const grouped = useMemo(() => {
    const groups: Record<string, SavedConnection[]> = {};
    for (const conn of connections) {
      const type = conn.connectionType || "Other";
      if (!groups[type]) groups[type] = [];
      groups[type].push(conn);
    }
    return groups;
  }, [connections]);

  function handleToggle(rowKey: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(rowKey)) {
        next.delete(rowKey);
      } else {
        next.add(rowKey);
        onExpandConnection(rowKey);
      }
      return next;
    });
  }

  const isExpanded = (rowKey: string) => expanded.has(rowKey);
  const isLoading = (rowKey: string) => loadingTables.has(rowKey);
  const tables = (rowKey: string) => connectionTables[rowKey];
  const queries = (rowKey: string) => connectionQueries[rowKey];

  return (
    <div className="connections-panel" style={{ width }}>
      <div className="connections-panel-header">
        <h3 className="connections-panel-title">Connections</h3>
        <button
          className="connections-panel-close"
          onClick={onClose}
          aria-label="Close panel"
        >
          <svg width="14" height="14" viewBox="0 0 14 14">
            <path d="M3 3 L11 11 M11 3 L3 11" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
        </button>
      </div>
      {connections.length === 0 ? (
        <p className="connections-panel-empty">No saved connections.</p>
      ) : (
        <div className="connections-list">
          {Object.entries(grouped).map(([type, conns]) => (
            <div key={type} className="conn-group">
              <div className="conn-group-header">
                <span className="conn-type-badge">{type}</span>
              </div>
              <ul className="conn-group-list">
                {conns.map((conn) => (
                  <li key={conn.rowKey} className="connections-list-entry">
                    <div
                      className="connections-list-item"
                      onClick={() => handleToggle(conn.rowKey)}
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
                    </div>
                    {isExpanded(conn.rowKey) && (
                      <div className="conn-tables">
                        {isLoading(conn.rowKey) ? (
                          <div className="conn-tables-loading">Loading...</div>
                        ) : (
                          <>
                            {queries(conn.rowKey) && queries(conn.rowKey)!.length > 0 && (
                              <ul className="conn-tables-list">
                                {queries(conn.rowKey)!.map((q) => {
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
                            {tables(conn.rowKey) ? (
                              tables(conn.rowKey)!.length === 0 && (!queries(conn.rowKey) || queries(conn.rowKey)!.length === 0) ? (
                                <div className="conn-tables-empty">No tables found.</div>
                              ) : (
                                <ul className="conn-tables-list">
                                  {tables(conn.rowKey)!.map((t) => {
                                    const isSelected = selectedTable?.rowKey === conn.rowKey
                                      && selectedTable?.schema === t.schema
                                      && selectedTable?.tableName === t.name;
                                    return (
                                      <li
                                        key={`${t.schema}.${t.name}`}
                                        className={`conn-table-item${isSelected ? " conn-table-item-selected" : ""}`}
                                        onClick={() => onTableClick(conn.rowKey, t.schema, t.name)}
                                        onContextMenu={(e) => handleTableContextMenu(e, conn.rowKey, t.schema, t.name)}
                                      >
                                        <svg className="conn-table-icon" width="14" height="14" viewBox="0 0 14 14">
                                          <rect x="1" y="1" width="12" height="12" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1" />
                                          <line x1="1" y1="5" x2="13" y2="5" stroke="currentColor" strokeWidth="1" />
                                          <line x1="1" y1="9" x2="13" y2="9" stroke="currentColor" strokeWidth="1" />
                                          <line x1="5" y1="5" x2="5" y2="13" stroke="currentColor" strokeWidth="1" />
                                        </svg>
                                        <span className="conn-table-name">{t.schema}.{t.name}</span>
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
                ))}
              </ul>
            </div>
          ))}
        </div>
      )}
      {contextMenu && (
        <div
          className="conn-context-menu"
          style={{ top: contextMenu.y, left: contextMenu.x }}
          onMouseDown={(e) => e.stopPropagation()}
        >
          <div
            className="conn-context-menu-item"
            onClick={() => {
              setContextMenu(null);
              onReloadPreview();
            }}
          >
            Reload Data Preview
          </div>
          {!contextMenu.isQuery && (
            <div
              className="conn-context-menu-item"
              onClick={() => {
                const { rowKey, schema, tableName } = contextMenu;
                setContextMenu(null);
                onDryRun(rowKey, schema, tableName);
              }}
            >
              Data Protection: Dry Run
            </div>
          )}
        </div>
      )}
      <div
        className="connections-panel-resize"
        onMouseDown={handleResizeStart}
      />
    </div>
  );
}
