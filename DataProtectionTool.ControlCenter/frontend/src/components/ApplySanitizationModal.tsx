import { useEffect, useMemo, useState } from "react";
import type { SavedConnection } from "./ConnectionsPanel";
import "./ApplySanitizationModal.css";

interface ApplySanitizationModalProps {
  connections: SavedConnection[];
  checkedTableKeys: string[];
  agentPath: string;
  onClose: () => void;
  onSave: (destConnectionRowKey: string, destSchema: string, tableKeys: string[]) => void;
  onSaveAndRun: (destConnectionRowKey: string, destSchema: string, tableKeys: string[]) => void;
}

export default function ApplySanitizationModal({
  connections,
  checkedTableKeys,
  agentPath,
  onClose,
  onSave,
  onSaveAndRun,
}: ApplySanitizationModalProps) {
  const [selectedConnection, setSelectedConnection] = useState(
    connections.length > 0 ? connections[0].rowKey : ""
  );
  const [schemas, setSchemas] = useState<string[]>([]);
  const [selectedSchema, setSelectedSchema] = useState("");
  const [loadingSchemas, setLoadingSchemas] = useState(false);

  const { dbCount, tableCount } = useMemo(() => {
    const uniqueRowKeys = new Set<string>();
    for (const key of checkedTableKeys) {
      const rowKey = key.split(":")[0];
      if (rowKey) uniqueRowKeys.add(rowKey);
    }
    return { dbCount: uniqueRowKeys.size, tableCount: checkedTableKeys.length };
  }, [checkedTableKeys]);

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

  const canSubmit = !!selectedConnection && !!selectedSchema;

  return (
    <div className="apply-san-overlay">
      <div className="apply-san-dialog">
        <div className="apply-san-header">
          <h2>Choose Destination</h2>
        </div>

        <div className="apply-san-body">
          <div className="apply-san-summary">
            {dbCount} {dbCount === 1 ? "database" : "databases"}, {tableCount}{" "}
            {tableCount === 1 ? "table" : "tables"} selected
          </div>

          <div className="apply-san-dropdowns">
            <div className="apply-san-dropdown-group">
              <label className="apply-san-label">Database</label>
              <select
                className="apply-san-select"
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

            <div className="apply-san-dropdown-group">
              <label className="apply-san-label">Schema</label>
              <select
                className="apply-san-select"
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

        <div className="apply-san-footer">
          <button className="apply-san-btn apply-san-btn-cancel" onClick={onClose}>
            Cancel
          </button>
          <div className="apply-san-footer-actions">
            <button
              className="apply-san-btn apply-san-btn-save"
              disabled={!canSubmit}
              onClick={() => onSave(selectedConnection, selectedSchema, checkedTableKeys)}
            >
              Save
            </button>
            <button
              className="apply-san-btn apply-san-btn-run"
              disabled={!canSubmit}
              onClick={() => onSaveAndRun(selectedConnection, selectedSchema, checkedTableKeys)}
            >
              Save and Run
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
