import { useEffect, useState } from "react";
import type { SavedConnection } from "./ConnectionsPanel";
import "./FullRunModal.css";

export interface FlowSource {
  connectionRowKey: string;
  serverName: string;
  databaseName: string;
  schema: string;
  tableName: string;
}

export interface FlowDest {
  connectionRowKey: string;
  serverName: string;
  databaseName: string;
  schema: string;
}

interface FullRunModalProps {
  connections: SavedConnection[];
  sourceConnectionRowKey: string;
  schema: string;
  tableName: string;
  agentPath: string;
  minimizing?: boolean;
  onClose: () => void;
  onSaveAndRun: (source: FlowSource, dest: FlowDest, destConnectionRowKey: string, destSchema: string) => void;
  onAddToFlow: (source: FlowSource, dest: FlowDest) => void;
  onMinimizeEnd?: () => void;
}

export default function FullRunModal({
  connections,
  sourceConnectionRowKey,
  schema,
  tableName,
  agentPath,
  minimizing,
  onClose,
  onSaveAndRun,
  onAddToFlow,
  onMinimizeEnd,
}: FullRunModalProps) {
  const [selectedConnection, setSelectedConnection] = useState(
    connections.length > 0 ? connections[0].rowKey : ""
  );
  const [schemas, setSchemas] = useState<string[]>([]);
  const [selectedSchema, setSelectedSchema] = useState("");
  const [loadingSchemas, setLoadingSchemas] = useState(false);

  const sourceConn = connections.find((c) => c.rowKey === sourceConnectionRowKey);

  useEffect(() => {
    if (!selectedConnection || !agentPath) return;

    let cancelled = false;
    setLoadingSchemas(true);
    setSchemas([]);
    setSelectedSchema("");

    fetch(`/api/agents/${agentPath}/list-schemas?rowKey=${encodeURIComponent(selectedConnection)}`)
      .then((res) => res.json())
      .then((data) => {
        if (cancelled) return;
        if (data.success && Array.isArray(data.schemas)) {
          setSchemas(data.schemas);
          if (data.schemas.length > 0) {
            setSelectedSchema(data.schemas[0]);
          }
        }
      })
      .catch(() => {})
      .finally(() => {
        if (!cancelled) setLoadingSchemas(false);
      });

    return () => { cancelled = true; };
  }, [selectedConnection, agentPath]);

  const destConn = connections.find((c) => c.rowKey === selectedConnection);
  const canSubmit = !!selectedConnection && !!selectedSchema;

  function handleAddToFlow() {
    if (!canSubmit || !sourceConn || !destConn) return;
    onAddToFlow(
      {
        connectionRowKey: sourceConnectionRowKey,
        serverName: sourceConn.serverName,
        databaseName: sourceConn.databaseName,
        schema,
        tableName,
      },
      {
        connectionRowKey: selectedConnection,
        serverName: destConn.serverName,
        databaseName: destConn.databaseName,
        schema: selectedSchema,
      },
    );
  }

  function handleSaveAndRun() {
    if (!canSubmit || !sourceConn || !destConn) return;
    onSaveAndRun(
      {
        connectionRowKey: sourceConnectionRowKey,
        serverName: sourceConn.serverName,
        databaseName: sourceConn.databaseName,
        schema,
        tableName,
      },
      {
        connectionRowKey: selectedConnection,
        serverName: destConn.serverName,
        databaseName: destConn.databaseName,
        schema: selectedSchema,
      },
      selectedConnection,
      selectedSchema,
    );
  }

  return (
    <div className={`fullrun-modal-overlay${minimizing ? " fullrun-overlay-minimizing" : ""}`}>
      <div
        className={`fullrun-modal-dialog${minimizing ? " fullrun-modal-minimizing" : ""}`}
        onAnimationEnd={minimizing ? onMinimizeEnd : undefined}
      >
        <div className="fullrun-modal-header">
          <h2>Data Protection: Full Run</h2>
        </div>

        <div className="fullrun-modal-body">
          <div className="fullrun-section">
            <h3 className="fullrun-section-title">Source</h3>
            <div className="fullrun-table-info">
              {sourceConn
                ? <><strong>{sourceConn.serverName}</strong>{sourceConn.databaseName ? ` / ${sourceConn.databaseName}` : ""}</>
                : <span>Unknown connection</span>}
              <span className="fullrun-source-table">{schema}.{tableName}</span>
            </div>
          </div>

          <div className="fullrun-section">
            <h3 className="fullrun-section-title">Destination</h3>
            <div className="fullrun-form-row">
              <label className="fullrun-form-label">Database:</label>
              <select
                className="fullrun-form-select"
                value={selectedConnection}
                onChange={(e) => setSelectedConnection(e.target.value)}
              >
                {connections.map((conn) => (
                  <option key={conn.rowKey} value={conn.rowKey}>
                    {conn.serverName}{conn.databaseName ? ` / ${conn.databaseName}` : ""}
                  </option>
                ))}
              </select>
            </div>

            <div className="fullrun-form-row">
              <label className="fullrun-form-label">Schema:</label>
              <select
                className="fullrun-form-select"
                value={selectedSchema}
                onChange={(e) => setSelectedSchema(e.target.value)}
                disabled={loadingSchemas || schemas.length === 0}
              >
                {loadingSchemas ? (
                  <option value="">Loading...</option>
                ) : schemas.length === 0 ? (
                  <option value="">No schemas available</option>
                ) : (
                  schemas.map((s) => (
                    <option key={s} value={s}>{s}</option>
                  ))
                )}
              </select>
            </div>
          </div>
        </div>

        <div className="fullrun-modal-footer">
          <button className="fullrun-btn fullrun-btn-cancel" onClick={onClose}>
            Cancel
          </button>
          <div className="fullrun-footer-actions">
            <button
              className="fullrun-btn fullrun-btn-add-flow"
              disabled={!canSubmit}
              onClick={handleAddToFlow}
            >
              Save
            </button>
            <button
              className="fullrun-btn fullrun-btn-run"
              disabled={!canSubmit}
              onClick={handleSaveAndRun}
            >
              Save &amp; Run
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
