import { useState } from "react";
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

interface ConnectionsPanelProps {
  connections: SavedConnection[];
  connectionTables: Record<string, TableInfo[]>;
  loadingTables: Set<string>;
  onExpandConnection: (rowKey: string) => void;
}

export default function ConnectionsPanel({
  connections,
  connectionTables,
  loadingTables,
  onExpandConnection,
}: ConnectionsPanelProps) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  function handleToggle(rowKey: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(rowKey)) {
        next.delete(rowKey);
      } else {
        next.add(rowKey);
        if (!connectionTables[rowKey]) {
          onExpandConnection(rowKey);
        }
      }
      return next;
    });
  }

  const isExpanded = (rowKey: string) => expanded.has(rowKey);
  const isLoading = (rowKey: string) => loadingTables.has(rowKey);
  const tables = (rowKey: string) => connectionTables[rowKey];

  return (
    <div className="connections-panel">
      <h3 className="connections-panel-title">Connections</h3>
      {connections.length === 0 ? (
        <p className="connections-panel-empty">No saved connections.</p>
      ) : (
        <ul className="connections-list">
          {connections.map((conn) => (
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
                <span className="conn-type-badge">{conn.connectionType}</span>
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
                    <div className="conn-tables-loading">Loading tables...</div>
                  ) : tables(conn.rowKey) ? (
                    tables(conn.rowKey)!.length === 0 ? (
                      <div className="conn-tables-empty">No tables found.</div>
                    ) : (
                      <ul className="conn-tables-list">
                        {tables(conn.rowKey)!.map((t) => (
                          <li key={`${t.schema}.${t.name}`} className="conn-table-item">
                            <svg className="conn-table-icon" width="14" height="14" viewBox="0 0 14 14">
                              <rect x="1" y="1" width="12" height="12" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1" />
                              <line x1="1" y1="5" x2="13" y2="5" stroke="currentColor" strokeWidth="1" />
                              <line x1="1" y1="9" x2="13" y2="9" stroke="currentColor" strokeWidth="1" />
                              <line x1="5" y1="5" x2="5" y2="13" stroke="currentColor" strokeWidth="1" />
                            </svg>
                            <span className="conn-table-name">{t.schema}.{t.name}</span>
                          </li>
                        ))}
                      </ul>
                    )
                  ) : null}
                </div>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
