import { useState } from "react";
import type { SavedConnection } from "./ConnectionsPanel";
import "./QueryModal.css";

export interface QueryValidateResult {
  success: boolean;
  message: string;
}

export interface QuerySaveData {
  connectionRowKey: string;
  queryText: string;
}

interface QueryModalProps {
  connections: SavedConnection[];
  onClose: () => void;
  onSave: (data: QuerySaveData) => void;
  onValidate: (data: QuerySaveData) => Promise<QueryValidateResult>;
}

export default function QueryModal({
  connections,
  onClose,
  onSave,
  onValidate,
}: QueryModalProps) {
  const [connectionRowKey, setConnectionRowKey] = useState(
    connections.length > 0 ? connections[0].rowKey : ""
  );
  const [queryText, setQueryText] = useState("");
  const [status, setStatus] = useState("");
  const [validating, setValidating] = useState(false);
  const [validated, setValidated] = useState(false);

  function invalidate() {
    setValidated(false);
  }

  function getFormData(): QuerySaveData {
    return { connectionRowKey, queryText };
  }

  function handleSave() {
    if (!validated) return;
    onSave(getFormData());
  }

  async function handleValidate() {
    if (!connectionRowKey) {
      setStatus("Please select a database connection.");
      return;
    }
    if (!queryText.trim()) {
      setStatus("Query text is required.");
      return;
    }
    setValidating(true);
    setStatus("Validating...");
    try {
      const result = await onValidate(getFormData());
      setStatus(result.message);
      setValidated(result.success);
    } catch (err) {
      setStatus(err instanceof Error ? err.message : "Validation failed.");
      setValidated(false);
    } finally {
      setValidating(false);
    }
  }

  return (
    <div className="modal-overlay">
      <div className="modal-dialog query-modal-dialog">
        <div className="modal-header">
          <h2>New Query</h2>
        </div>

        <div className="modal-body">
          <div className="form-row">
            <label className="form-label">Database:</label>
            <select
              className="form-select"
              value={connectionRowKey}
              onChange={(e) => {
                setConnectionRowKey(e.target.value);
                invalidate();
              }}
            >
              {connections.length === 0 && (
                <option value="">No connections available</option>
              )}
              {connections.map((conn) => (
                <option key={conn.rowKey} value={conn.rowKey}>
                  {conn.serverName}
                  {conn.databaseName ? ` / ${conn.databaseName}` : ""}
                </option>
              ))}
            </select>
          </div>

          <div className="form-row query-text-row">
            <label className="form-label query-text-label">Query:</label>
            <textarea
              className="query-textarea"
              placeholder="SELECT column1, column2 FROM myTable WHERE ..."
              value={queryText}
              onChange={(e) => {
                setQueryText(e.target.value);
                invalidate();
              }}
              rows={8}
              spellCheck={false}
            />
          </div>

          <div className="form-row status-row">
            <label className="form-label status-label">Status</label>
            <textarea
              className="form-textarea"
              readOnly
              value={status}
              rows={3}
            />
          </div>
        </div>

        <div className="modal-footer">
          <button className="btn btn-cancel" onClick={onClose}>
            Cancel
          </button>
          <div className="modal-footer-right">
            <button
              className="btn btn-validate"
              onClick={handleValidate}
              disabled={validating}
            >
              {validating ? "Validating..." : "Validate"}
            </button>
            <button
              className="btn btn-save"
              onClick={handleSave}
              disabled={!validated}
            >
              Save
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
