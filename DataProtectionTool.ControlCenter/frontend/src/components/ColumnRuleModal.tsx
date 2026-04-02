import { useEffect, useState } from "react";
import "./ColumnRuleModal.css";

interface ColumnRuleModalProps {
  selectedRule: Record<string, unknown>;
  allAlgorithms: Record<string, unknown>[];
  allDomains: Record<string, unknown>[];
  allFrameworks: Record<string, unknown>[];
  onSave: (params: {
    fileFieldMetadataId: string;
    algorithmName: string;
    domainName: string;
  }) => Promise<void>;
  onClose: () => void;
  mismatchedColumns?: Map<string, { maskType: string; sqlType: string }>;
  onMismatchedColumnsChange?: (
    updater: (
      prev: Map<string, { maskType: string; sqlType: string }>
    ) => Map<string, { maskType: string; sqlType: string }>
  ) => void;
  selectedColumnSqlType?: string;
}

export default function ColumnRuleModal({
  selectedRule,
  allAlgorithms,
  allDomains,
  allFrameworks,
  onSave,
  onClose,
  onMismatchedColumnsChange,
  selectedColumnSqlType = "",
}: ColumnRuleModalProps) {
  const isMasked = selectedRule.isMasked !== false && !selectedRule._noRule;
  const [modalDomainName, setModalDomainName] = useState(
    isMasked && typeof selectedRule.domainName === "string" ? selectedRule.domainName : ""
  );
  const [modalAlgorithmName, setModalAlgorithmName] = useState(() => {
    const candidateAlg = isMasked && typeof selectedRule.algorithmName === "string" ? selectedRule.algorithmName : "";
    return candidateAlg;
  });
  const [modalAlgorithmType, setModalAlgorithmType] = useState(() => {
    const candidateAlg = isMasked && typeof selectedRule.algorithmName === "string" ? selectedRule.algorithmName : "";
    const matched = candidateAlg ? allAlgorithms.find(a => a.algorithmName === candidateAlg) : undefined;
    return matched ? String(matched.maskType ?? "") : "";
  });
  const [allowedAlgorithmTypes, setAllowedAlgorithmTypes] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);
  const [typeMismatchConfirm, setTypeMismatchConfirm] = useState<{ maskType: string; sqlType: string } | null>(null);

  useEffect(() => {
    if (!selectedColumnSqlType) {
      setAllowedAlgorithmTypes([]);
      return;
    }
    let cancelled = false;
    fetch(`/api/allowed-algorithm-types?sqlType=${encodeURIComponent(selectedColumnSqlType)}`)
      .then(r => r.json())
      .then(json => {
        if (!cancelled && json.success) {
          setAllowedAlgorithmTypes(json.allowedTypes ?? []);
        }
      })
      .catch(() => {
        if (!cancelled) setAllowedAlgorithmTypes([]);
      });
    return () => { cancelled = true; };
  }, [selectedColumnSqlType]);

  const matchedAlg = modalAlgorithmName
    ? allAlgorithms.find(a => a.algorithmName === modalAlgorithmName)
    : undefined;
  const fwId = matchedAlg && matchedAlg.frameworkId != null ? String(matchedAlg.frameworkId) : "";
  const matchedFw = fwId
    ? allFrameworks.find(f => String(f.frameworkId) === fwId)
    : undefined;

  const str = (val: unknown) => (val != null ? String(val) : "");

  function handleSave() {
    const alg = allAlgorithms.find(a => a.algorithmName === modalAlgorithmName);
    const mt = alg ? String(alg.maskType ?? "") : "";
    if (mt && allowedAlgorithmTypes.length > 0 && !allowedAlgorithmTypes.includes(mt)) {
      setTypeMismatchConfirm({ maskType: mt, sqlType: selectedColumnSqlType });
      return;
    }
    doSave();
  }

  function doSave() {
    const fieldName = typeof selectedRule.fieldName === "string" ? selectedRule.fieldName : "";
    if (fieldName && onMismatchedColumnsChange) {
      onMismatchedColumnsChange(prev => {
        const next = new Map(prev);
        next.delete(fieldName);
        return next;
      });
    }
    const id = selectedRule.fileFieldMetadataId;
    if (typeof id !== "string" && typeof id !== "number") return;
    setSaving(true);
    (async () => {
      try {
        await onSave({
          fileFieldMetadataId: String(id),
          algorithmName: modalAlgorithmName,
          domainName: modalDomainName,
        });
        onClose();
      } catch {
        // keep modal open on error
      } finally {
        setSaving(false);
      }
    })();
  }

  function handleMismatchConfirm() {
    if (!typeMismatchConfirm) return;
    const fieldName = typeof selectedRule.fieldName === "string" ? selectedRule.fieldName : "";
    if (fieldName && onMismatchedColumnsChange) {
      const info = { maskType: typeMismatchConfirm.maskType, sqlType: typeMismatchConfirm.sqlType };
      onMismatchedColumnsChange(prev => new Map(prev).set(fieldName, info));
    }
    setTypeMismatchConfirm(null);
    const id = selectedRule.fileFieldMetadataId;
    if (typeof id !== "string" && typeof id !== "number") return;
    setSaving(true);
    (async () => {
      try {
        await onSave({
          fileFieldMetadataId: String(id),
          algorithmName: modalAlgorithmName,
          domainName: modalDomainName,
        });
        onClose();
      } catch {
        // keep modal open on error
      } finally {
        setSaving(false);
      }
    })();
  }

  return (
    <>
      <div className="column-rule-modal-overlay" onClick={onClose}>
        <div className="column-rule-modal" onClick={(e) => e.stopPropagation()}>
          <div className="column-rule-modal-header">
            <span className="column-rule-modal-title">
              Column Rule: {String(selectedRule.fieldName ?? "")}
            </span>
            <button
              className="column-rule-modal-close"
              onClick={onClose}
              aria-label="Close"
            >
              <svg width="14" height="14" viewBox="0 0 14 14">
                <path d="M3 3 L11 11 M11 3 L3 11" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
              </svg>
            </button>
          </div>
          <div className="column-rule-modal-body">
            <div className="column-rule-row">
              <span className="column-rule-label">Domain Name</span>
              <select
                className="column-rule-select"
                value={modalDomainName}
                onChange={(e) => {
                  const newDomain = e.target.value;
                  setModalDomainName(newDomain);
                  const dom = allDomains.find(d => d.domainName === newDomain);
                  const defaultAlg = dom && typeof dom.defaultAlgorithmCode === "string"
                    ? dom.defaultAlgorithmCode : "";
                  setModalAlgorithmName(defaultAlg);
                  const matched = defaultAlg ? allAlgorithms.find(a => a.algorithmName === defaultAlg) : undefined;
                  setModalAlgorithmType(matched ? String(matched.maskType ?? "") : "");
                }}
              >
                <option value="">-- Select --</option>
                {allDomains.map((d, i) => (
                  <option key={i} value={String(d.domainName ?? "")}>
                    {String(d.domainName ?? "")}
                  </option>
                ))}
              </select>
            </div>
            <div className="column-rule-row">
              <span className="column-rule-label">Algorithm Name</span>
              <div className="column-rule-field">
                <select
                  className="column-rule-select"
                  value={modalAlgorithmName}
                  onChange={(e) => {
                    const newAlgName = e.target.value;
                    setModalAlgorithmName(newAlgName);
                    const alg = newAlgName ? allAlgorithms.find(a => a.algorithmName === newAlgName) : undefined;
                    setModalAlgorithmType(alg ? String(alg.maskType ?? "") : "");
                  }}
                >
                  <option value="">-- Select --</option>
                  {allAlgorithms
                    .filter(a => {
                      if (modalAlgorithmType && String(a.maskType ?? "") !== modalAlgorithmType) return false;
                      return true;
                    })
                    .map((a, i) => (
                      <option key={i} value={String(a.algorithmName ?? "")}>
                        {String(a.algorithmName ?? "")}
                      </option>
                    ))}
                </select>
                {modalAlgorithmType && allowedAlgorithmTypes.length > 0 && !allowedAlgorithmTypes.includes(modalAlgorithmType) && (
                  <span className="column-rule-mismatch-hint">
                    The Algorithm is for <strong>{modalAlgorithmType}</strong>, but column is <strong>{selectedColumnSqlType}</strong>
                  </span>
                )}
              </div>
            </div>
            <div className="column-rule-row">
              <span className="column-rule-label">Algorithm Type</span>
              <select
                className="column-rule-select"
                value={modalAlgorithmType}
                onChange={(e) => {
                  const newType = e.target.value;
                  setModalAlgorithmType(newType);
                  if (modalAlgorithmName) {
                    const currentAlg = allAlgorithms.find(a => a.algorithmName === modalAlgorithmName);
                    if (currentAlg && String(currentAlg.maskType ?? "") !== newType) {
                      setModalAlgorithmName("");
                    }
                  }
                }}
              >
                <option value="">-- Select --</option>
                {[...new Set(allAlgorithms.map(a => String(a.maskType ?? "")).filter(Boolean))].map((t, i) => (
                  <option key={i} value={t}>{t}</option>
                ))}
              </select>
            </div>
            <div className="column-rule-row">
              <span className="column-rule-label">Algorithm Description</span>
              <span className="column-rule-readonly">
                {matchedAlg ? str(matchedAlg.description) : ""}
              </span>
            </div>
            <div className="column-rule-row">
              <span className="column-rule-label">Framework Name</span>
              <span className="column-rule-readonly">
                {matchedFw ? str(matchedFw.frameworkName) : ""}
              </span>
            </div>
            <div className="column-rule-row">
              <span className="column-rule-label">Framework Description</span>
              <span className="column-rule-readonly">
                {matchedFw ? str(matchedFw.description) : ""}
              </span>
            </div>
          </div>
          <div className="column-rule-modal-footer">
            <button
              className="column-rule-btn-cancel"
              onClick={onClose}
              disabled={saving}
            >
              Cancel
            </button>
            <button
              className="column-rule-btn-save"
              disabled={saving || !modalAlgorithmName || !modalDomainName}
              onClick={handleSave}
            >
              {saving ? "Saving..." : "Save"}
            </button>
          </div>
        </div>
      </div>
      {typeMismatchConfirm && (
        <div className="column-rule-mismatch-overlay" onClick={() => setTypeMismatchConfirm(null)}>
          <div className="column-rule-mismatch-dialog" onClick={(e) => e.stopPropagation()}>
            <p className="column-rule-mismatch-msg">
              The selected Algorithm has Type {typeMismatchConfirm.maskType} but the column is <strong>{typeMismatchConfirm.sqlType}</strong> in database. Still proceed?
            </p>
            <div className="column-rule-mismatch-actions">
              <button
                className="column-rule-btn-cancel"
                onClick={() => setTypeMismatchConfirm(null)}
              >
                Cancel
              </button>
              <button
                className="column-rule-btn-save"
                disabled={saving}
                onClick={handleMismatchConfirm}
              >
                {saving ? "Saving..." : "Confirm"}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
