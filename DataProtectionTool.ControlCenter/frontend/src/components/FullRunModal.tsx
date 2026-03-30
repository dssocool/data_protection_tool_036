import { useEffect, useState } from "react";
import type { SavedConnection } from "./ConnectionsPanel";
import "./FullRunModal.css";

interface FullRunModalProps {
  connections: SavedConnection[];
  schema: string;
  tableName: string;
  agentPath: string;
  onClose: () => void;
  onRun: (destConnectionRowKey: string, destSchema: string) => void;
}

export default function FullRunModal({
  connections,
  schema,
  tableName,
  agentPath,
  onClose,
  onRun,
}: FullRunModalProps) {
  const [selectedConnection, setSelectedConnection] = useState(
    connections.length > 0 ? connections[0].rowKey : ""
  );
  const [schemas, setSchemas] = useState<string[]>([]);
  const [selectedSchema, setSelectedSchema] = useState("");
  const [loadingSchemas, setLoadingSchemas] = useState(false);

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

  return (
    <div className="fullrun-modal-overlay">
      <div className="fullrun-modal-dialog">
        <div className="fullrun-modal-header">
          <h2>Data Protection: Full Run</h2>
        </div>

        <div className="fullrun-modal-body">
          <div className="fullrun-table-info">
            Source table: <strong>{schema}.{tableName}</strong>
          </div>

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

        <div className="fullrun-modal-footer">
          <button className="fullrun-btn fullrun-btn-cancel" onClick={onClose}>
            Cancel
          </button>
          <button
            className="fullrun-btn fullrun-btn-run"
            disabled={!selectedConnection || !selectedSchema}
            onClick={() => onRun(selectedConnection, selectedSchema)}
          >
            Run
          </button>
        </div>
      </div>
    </div>
  );
}
