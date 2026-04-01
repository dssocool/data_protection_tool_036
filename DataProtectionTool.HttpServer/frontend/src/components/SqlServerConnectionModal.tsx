import { useEffect, useRef, useState } from "react";
import "./SqlServerConnectionModal.css";

export interface ValidateResult {
  success: boolean;
  message: string;
}

interface SqlServerConnectionModalProps {
  onClose: () => void;
  onSave: (data: SqlServerConnectionData) => void;
  onValidate: (data: SqlServerConnectionData) => Promise<ValidateResult>;
  minimizing?: boolean;
  onMinimizeEnd?: () => void;
}

export interface SqlServerConnectionData {
  serverName: string;
  authentication: string;
  userName: string;
  password: string;
  databaseName: string;
  encrypt: string;
  trustServerCertificate: boolean;
}

const AUTH_OPTIONS = ["Microsoft Entra Integrated"];

const ENCRYPT_OPTIONS = ["Mandatory"];

export default function SqlServerConnectionModal({
  onClose,
  onSave,
  onValidate,
  minimizing,
  onMinimizeEnd,
}: SqlServerConnectionModalProps) {
  const [serverName, setServerName] = useState("");
  const [authentication, setAuthentication] = useState(AUTH_OPTIONS[0]);
  const [userName, setUserName] = useState("");
  const [password, setPassword] = useState("");
  const [databaseName, setDatabaseName] = useState("");
  const [encrypt, setEncrypt] = useState("Mandatory");
  const [trustServerCertificate, setTrustServerCertificate] = useState(true);
  const [status, setStatus] = useState("");
  const [validating, setValidating] = useState(false);
  const [validated, setValidated] = useState(false);

  const dialogRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!minimizing || !dialogRef.current) return;
    const dialog = dialogRef.current;
    const connBtn = document.querySelector<HTMLElement>("[data-connections-btn]");
    if (!connBtn) return;

    const dr = dialog.getBoundingClientRect();
    const cr = connBtn.getBoundingClientRect();

    const dx = (cr.left + cr.width / 2) - (dr.left + dr.width / 2);
    const dy = (cr.top + cr.height / 2) - (dr.top + dr.height / 2);

    dialog.style.setProperty("--minimize-tx", `${dx}px`);
    dialog.style.setProperty("--minimize-ty", `${dy}px`);
  }, [minimizing]);

  const credentialsDisabled = authentication === "Microsoft Entra Integrated";

  function invalidate() {
    setValidated(false);
  }

  function getFormData(): SqlServerConnectionData {
    return {
      serverName,
      authentication,
      userName,
      password,
      databaseName,
      encrypt,
      trustServerCertificate,
    };
  }

  function handleSave() {
    if (!validated) return;
    onSave(getFormData());
  }

  async function handleValidate() {
    if (!serverName.trim()) {
      setStatus("Server Name is required.");
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
    <div className={`modal-overlay${minimizing ? " sql-overlay-minimizing" : ""}`}>
      <div
        ref={dialogRef}
        className={`modal-dialog${minimizing ? " sql-modal-minimizing" : ""}`}
        onAnimationEnd={minimizing ? onMinimizeEnd : undefined}
      >
        <div className="modal-header">
          <h2>SQL Server Connection</h2>
        </div>

        <div className="modal-body">
          <div className="form-row">
            <label className="form-label">Server Name:</label>
            <input
              className="form-input"
              type="text"
              placeholder="e.g. localhost\SQLEXPRESS"
              value={serverName}
              onChange={(e) => { setServerName(e.target.value); invalidate(); }}
            />
          </div>

          <div className="form-row">
            <label className="form-label">Authentication:</label>
            <select
              className="form-select"
              value={authentication}
              onChange={(e) => { setAuthentication(e.target.value); invalidate(); }}
            >
              {AUTH_OPTIONS.map((opt) => (
                <option key={opt} value={opt}>
                  {opt}
                </option>
              ))}
            </select>
          </div>

          <div className="form-row">
            <label className="form-label">User Name:</label>
            <input
              className={`form-input ${credentialsDisabled ? "disabled" : ""}`}
              type="text"
              value={userName}
              onChange={(e) => { setUserName(e.target.value); invalidate(); }}
              disabled={credentialsDisabled}
            />
          </div>

          <div className="form-row">
            <label className="form-label">Password:</label>
            <input
              className={`form-input ${credentialsDisabled ? "disabled" : ""}`}
              type="password"
              value={password}
              onChange={(e) => { setPassword(e.target.value); invalidate(); }}
              disabled={credentialsDisabled}
            />
          </div>

          <div className="form-row">
            <label className="form-label">Database Name:</label>
            <input
              className="form-input"
              type="text"
              value={databaseName}
              onChange={(e) => { setDatabaseName(e.target.value); invalidate(); }}
            />
          </div>

          <div className="form-row">
            <label className="form-label">Encrypt:</label>
            <select
              className="form-select"
              value={encrypt}
              onChange={(e) => { setEncrypt(e.target.value); invalidate(); }}
            >
              {ENCRYPT_OPTIONS.map((opt) => (
                <option key={opt} value={opt}>
                  {opt}
                </option>
              ))}
            </select>
          </div>

          <div className="form-row checkbox-row">
            <label className="form-label" />
            <label className="checkbox-label">
              <input
                type="checkbox"
                checked={trustServerCertificate}
                onChange={(e) => { setTrustServerCertificate(e.target.checked); invalidate(); }}
              />
              Trust Server Certificate
            </label>
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
