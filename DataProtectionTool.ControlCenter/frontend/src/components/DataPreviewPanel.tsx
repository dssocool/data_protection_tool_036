import { forwardRef, useCallback, useEffect, useMemo, useRef, useState } from "react";
import "./DataPreviewPanel.css";

export interface PreviewData {
  headers: string[];
  rows: string[][];
  columnTypes?: string[];
}

export interface DryRunResult {
  label: string;
  data: PreviewData | null;
  status?: string;
  inProgress?: boolean;
}

interface DiffTab {
  name: string;
  leftTab: string;
  rightTab: string;
}

interface DataPreviewPanelProps {
  loading: boolean;
  error: string | null;
  data: PreviewData | null;
  originalData: PreviewData | null;
  dryRuns: DryRunResult[];
  activeTab: string;
  diffTab: DiffTab | null;
  columnRules: Record<string, unknown>[];
  columnRuleAlgorithms: Record<string, unknown>[];
  columnRuleDomains: Record<string, unknown>[];
  columnRuleFrameworks: Record<string, unknown>[];
  columnRulesLoading: boolean;
  allDomains: Record<string, unknown>[];
  allAlgorithms: Record<string, unknown>[];
  allFrameworks: Record<string, unknown>[];
  onTabChange: (tab: string) => void;
  onTabClose: (tab: string) => void;
  onDiffSelect: (leftTab: string, rightTab: string) => void;
  onSaveColumnRule: (params: {
    fileFieldMetadataId: string;
    algorithmName: string;
    domainName: string;
  }) => Promise<void>;
  panelLeft: number;
}

function resolveTabData(
  tab: string,
  data: PreviewData | null,
  originalData: PreviewData | null,
  dryRuns: DryRunResult[],
): PreviewData | null {
  if (tab === "Original") return originalData ?? data;
  const dryRun = dryRuns.find((dr) => dr.label === tab);
  if (dryRun) return dryRun.data;
  return null;
}

const DataTable = forwardRef<HTMLTableElement, { data: PreviewData }>(
  function DataTable({ data }, ref) {
    return (
      <table className="data-preview-table" ref={ref}>
        <thead>
          <tr>
            {data.headers.map((h, i) => (
              <th key={i}>
                {h}
                {data.columnTypes?.[i] && (
                  <span className="column-type-label">{data.columnTypes[i]}</span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.rows.map((row, ri) => (
            <tr key={ri}>
              {row.map((cell, ci) => (
                <td key={ci}>{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    );
  },
);

const DiffView = forwardRef<HTMLTableElement, { left: PreviewData; right: PreviewData }>(
  function DiffView({ left, right }, ref) {
    const headers = left.headers;
    const maxRows = Math.max(left.rows.length, right.rows.length);

    return (
      <table className="data-preview-table data-preview-diff-table" ref={ref}>
        <thead>
          <tr>
            {headers.map((h, i) => (
              <th key={i}>
                {h}
                {left.columnTypes?.[i] && (
                  <span className="column-type-label">{left.columnTypes[i]}</span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: maxRows }, (_, ri) => {
            const leftRow = left.rows[ri];
            const rightRow = right.rows[ri];
            return (
              <tr key={ri}>
                {headers.map((_, ci) => {
                  const lv = leftRow?.[ci] ?? "";
                  const rv = rightRow?.[ci] ?? "";
                  const changed = lv !== rv;
                  if (!changed) {
                    return <td key={ci}>{lv}</td>;
                  }
                  return (
                    <td key={ci} className="data-preview-diff-cell-changed">
                      <span className="data-preview-diff-old">{lv}</span>
                      <span className="data-preview-diff-arrow">{"\u2192"}</span>
                      <span className="data-preview-diff-new">{rv}</span>
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    );
  },
);

export default function DataPreviewPanel({
  loading,
  error,
  data,
  originalData,
  dryRuns,
  activeTab,
  diffTab,
  columnRules,
  columnRuleAlgorithms: _columnRuleAlgorithms,
  columnRuleDomains: _columnRuleDomains,
  columnRuleFrameworks: _columnRuleFrameworks,
  columnRulesLoading,
  allDomains,
  allAlgorithms,
  allFrameworks,
  onTabChange,
  onTabClose,
  onDiffSelect,
  onSaveColumnRule,
  panelLeft,
}: DataPreviewPanelProps) {
  const dataTabs = useMemo(() => {
    const list: string[] = ["Original"];
    for (const dr of dryRuns) list.push(dr.label);
    return list;
  }, [dryRuns]);

  const tabs = useMemo(() => {
    const list: string[] = ["Original"];
    for (const dr of dryRuns) list.push(dr.label);
    if (diffTab) list.push(diffTab.name);
    return list;
  }, [dryRuns, diffTab]);

  const [leftDiffTab, setLeftDiffTab] = useState("Original");
  const [rightDiffTab, setRightDiffTab] = useState("");

  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; tab: string } | null>(null);
  const contextMenuRef = useRef<HTMLDivElement>(null);
  const tableRef = useRef<HTMLTableElement>(null);
  const columnRulesScrollRef = useRef<HTMLDivElement>(null);
  const dataBodyRef = useRef<HTMLDivElement>(null);
  const scrollingSource = useRef<"body" | "rules" | null>(null);
  const [colWidths, setColWidths] = useState<{ left: number; width: number }[]>([]);
  const [selectedRule, setSelectedRule] = useState<Record<string, unknown> | null>(null);
  const [modalDomainName, setModalDomainName] = useState("");
  const [modalAlgorithmName, setModalAlgorithmName] = useState("");
  const [modalAlgorithmType, setModalAlgorithmType] = useState("");
  const [allowedAlgorithmTypes, setAllowedAlgorithmTypes] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);
  const [typeMismatchConfirm, setTypeMismatchConfirm] = useState<{ maskType: string; sqlType: string } | null>(null);
  const [mismatchedColumns, setMismatchedColumns] = useState<Set<string>>(new Set());

  const handleDataScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
    if (scrollingSource.current === "rules") return;
    scrollingSource.current = "body";
    if (columnRulesScrollRef.current) {
      columnRulesScrollRef.current.scrollLeft = e.currentTarget.scrollLeft;
    }
    requestAnimationFrame(() => { scrollingSource.current = null; });
  }, []);

  const handleColumnRulesScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
    if (scrollingSource.current === "body") return;
    scrollingSource.current = "rules";
    if (dataBodyRef.current) {
      dataBodyRef.current.scrollLeft = e.currentTarget.scrollLeft;
    }
    requestAnimationFrame(() => { scrollingSource.current = null; });
  }, []);

  const rulesByField = useMemo(() => {
    const map = new Map<string, Record<string, unknown>>();
    for (const rule of columnRules) {
      const fieldName = rule.fieldName;
      if (typeof fieldName === "string") {
        map.set(fieldName, rule);
      }
    }
    return map;
  }, [columnRules]);

  const closeContextMenu = useCallback(() => setContextMenu(null), []);

  useEffect(() => {
    if (!contextMenu) return;
    const handleClick = (e: MouseEvent) => {
      if (contextMenuRef.current && !contextMenuRef.current.contains(e.target as Node)) {
        closeContextMenu();
      }
    };
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") closeContextMenu();
    };
    document.addEventListener("mousedown", handleClick);
    document.addEventListener("keydown", handleKey);
    return () => {
      document.removeEventListener("mousedown", handleClick);
      document.removeEventListener("keydown", handleKey);
    };
  }, [contextMenu, closeContextMenu]);

  useEffect(() => {
    if (
      diffTab
      && dataTabs.includes(diffTab.leftTab)
      && dataTabs.includes(diffTab.rightTab)
    ) {
      setLeftDiffTab(diffTab.leftTab);
      setRightDiffTab(diffTab.rightTab);
      return;
    }
    const defaultLeft = dataTabs[0] ?? "";
    const defaultRight = dataTabs.find((tab) => tab !== defaultLeft) ?? "";
    setLeftDiffTab(defaultLeft);
    setRightDiffTab(defaultRight);
  }, [dataTabs, diffTab]);

  const isDiffActive = diffTab != null && activeTab === diffTab.name;

  const activeData = isDiffActive
    ? null
    : resolveTabData(activeTab, data, originalData, dryRuns);

  const leftData = isDiffActive
    ? resolveTabData(diffTab!.leftTab, data, originalData, dryRuns)
    : null;
  const rightData = isDiffActive
    ? resolveTabData(diffTab!.rightTab, data, originalData, dryRuns)
    : null;

  const currentHeaders = isDiffActive
    ? (leftData?.headers ?? [])
    : (activeData?.headers ?? []);

  const currentColumnTypes = isDiffActive
    ? (leftData?.columnTypes ?? [])
    : (activeData?.columnTypes ?? []);

  const selectedColumnSqlType = useMemo(() => {
    if (!selectedRule) return "";
    const fieldName = selectedRule.fieldName;
    if (typeof fieldName !== "string") return "";
    const idx = currentHeaders.indexOf(fieldName);
    if (idx < 0 || idx >= currentColumnTypes.length) return "";
    return currentColumnTypes[idx] ?? "";
  }, [selectedRule, currentHeaders, currentColumnTypes]);

  useEffect(() => {
    if (!selectedRule || !selectedColumnSqlType) {
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
  }, [selectedRule, selectedColumnSqlType]);

  useEffect(() => {
    const table = tableRef.current;
    if (!table) return;
    const ths = table.querySelectorAll("thead th");
    if (!ths.length) return;
    const measure = () => {
      const tableRect = table.getBoundingClientRect();
      const widths = Array.from(ths).map((th) => {
        const rect = th.getBoundingClientRect();
        return { left: rect.left - tableRect.left, width: rect.width };
      });
      setColWidths(widths);
    };
    const ro = new ResizeObserver(measure);
    ths.forEach((th) => ro.observe(th));
    return () => ro.disconnect();
  }, [activeData, leftData, rightData]);

  return (
    <div className="data-preview-panel" style={{ left: panelLeft + 16 }}>
      <div className="data-preview-header">
        <div className="data-preview-tabs">
          {tabs.map((tab) => (
            <button
              key={tab}
              className={`data-preview-tab${activeTab === tab ? " data-preview-tab-active" : ""}`}
              onClick={() => onTabChange(tab)}
              onContextMenu={(e) => {
                e.preventDefault();
                setContextMenu({ x: e.clientX, y: e.clientY, tab });
              }}
            >
              {tab}
            </button>
          ))}
          {contextMenu && (
            <div
              ref={contextMenuRef}
              className="data-preview-tab-context-menu"
              style={{ left: contextMenu.x, top: contextMenu.y }}
            >
              <button
                className="data-preview-tab-context-menu-item"
                onClick={() => {
                  onTabClose(contextMenu.tab);
                  closeContextMenu();
                }}
              >
                Close
              </button>
            </div>
          )}
        </div>
        {dataTabs.length > 1 && (
          <div className="data-preview-diff-controls">
            <select
              className="data-preview-diff-select"
              value={leftDiffTab}
              onChange={(e) => setLeftDiffTab(e.target.value)}
            >
              {dataTabs.map((tab) => (
                <option key={tab} value={tab} disabled={tab === rightDiffTab}>
                  {tab}
                </option>
              ))}
            </select>
            <span className="data-preview-diff-select-arrow">{"\u2192"}</span>
            <select
              className="data-preview-diff-select"
              value={rightDiffTab}
              onChange={(e) => setRightDiffTab(e.target.value)}
            >
              {dataTabs.map((tab) => (
                <option key={tab} value={tab} disabled={tab === leftDiffTab}>
                  {tab}
                </option>
              ))}
            </select>
            <button
              type="button"
              className="data-preview-diff-button"
              disabled={!leftDiffTab || !rightDiffTab || leftDiffTab === rightDiffTab}
              onClick={() => onDiffSelect(leftDiffTab, rightDiffTab)}
            >
              Diff
            </button>
          </div>
        )}
      </div>
      <div className="data-preview-body" ref={dataBodyRef} onScroll={handleDataScroll}>
        {(() => {
          const activeDryRun = dryRuns.find((dr) => dr.label === activeTab);
          if (activeDryRun?.inProgress && !activeData) {
            return (
              <div className="dry-run-status-view">
                <div className="dry-run-status-spinner" />
                <div className="dry-run-status-text">{activeDryRun.status ?? "Starting dry run..."}</div>
              </div>
            );
          }
          if (loading) {
            return <div className="data-preview-loading">Loading preview...</div>;
          }
          if (error) {
            return <div className="data-preview-error">{error}</div>;
          }
          if (isDiffActive && leftData && rightData) {
            return <DiffView left={leftData} right={rightData} ref={tableRef} />;
          }
          if (activeData) {
            return <DataTable data={activeData} ref={tableRef} />;
          }
          return null;
        })()}
      </div>
      {currentHeaders.length > 0 && (
        <div className="data-preview-column-rules">
          <div className="data-preview-column-rules-header">
            <span className="data-preview-column-rules-tab">Column Rules</span>
          </div>
          <div className="data-preview-column-rules-scroll" ref={columnRulesScrollRef} onScroll={handleColumnRulesScroll}>
            <div
              className="data-preview-column-rules-row"
              style={colWidths.length > 0 ? {
                width: colWidths[colWidths.length - 1].left + colWidths[colWidths.length - 1].width,
              } : undefined}
            >
              {currentHeaders.map((header, i) => {
                const rule = rulesByField.get(header);
                const pos = colWidths[i];
                return (
                  <div
                    key={i}
                    className="data-preview-column-rules-cell"
                    style={pos ? { left: pos.left, width: pos.width } : undefined}
                  >
                    {columnRulesLoading ? (
                      <span className="column-rule-loading">...</span>
                    ) : rule ? (
                      <button
                        className={`column-rule-btn${rule.isMasked === false ? " column-rule-btn-na" : ""}`}
                        onClick={() => {
                          setSelectedRule(rule);
                          const isMasked = rule.isMasked !== false;
                          setModalDomainName(isMasked && typeof rule.domainName === "string" ? rule.domainName : "");
                          const candidateAlg = isMasked && typeof rule.algorithmName === "string" ? rule.algorithmName : "";
                          setModalAlgorithmName(candidateAlg);
                          const matched = candidateAlg ? allAlgorithms.find(a => a.algorithmName === candidateAlg) : undefined;
                          setModalAlgorithmType(matched ? String(matched.maskType ?? "") : "");
                        }}
                        title={rule.isMasked === false ? `No masking for ${header}` : `View rule for ${header}`}
                      >
                        {rule.isMasked === false
                          ? "N/A"
                          : typeof rule.algorithmName === "string"
                            ? rule.algorithmName
                            : header}
                        {mismatchedColumns.has(header) && (
                          <svg className="column-rule-warning-icon" width="14" height="14" viewBox="0 0 24 24" fill="none">
                            <path d="M12 2L1 21h22L12 2z" fill="#e8a012" stroke="#b37a00" strokeWidth="1"/>
                            <text x="12" y="18" textAnchor="middle" fontSize="13" fontWeight="bold" fill="#fff">!</text>
                          </svg>
                        )}
                      </button>
                    ) : (
                      <button
                        className="column-rule-btn column-rule-btn-na"
                        onClick={() => {
                          setSelectedRule({ fieldName: header, _noRule: true });
                          setModalDomainName("");
                          setModalAlgorithmName("");
                          setModalAlgorithmType("");
                        }}
                        title={`No rule for ${header}`}
                      >
                        N/A
                      </button>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}
      {selectedRule && (() => {
        const matchedAlg = modalAlgorithmName
          ? allAlgorithms.find(a => a.algorithmName === modalAlgorithmName)
          : undefined;
        const fwId = matchedAlg && matchedAlg.frameworkId != null ? String(matchedAlg.frameworkId) : "";
        const matchedFw = fwId
          ? allFrameworks.find(f => String(f.frameworkId) === fwId)
          : undefined;

        const str = (val: unknown) => (val != null ? String(val) : "");

        return (
          <div className="column-rule-modal-overlay" onClick={() => setSelectedRule(null)}>
            <div className="column-rule-modal" onClick={(e) => e.stopPropagation()}>
              <div className="column-rule-modal-header">
                <span className="column-rule-modal-title">
                  Column Rule: {String(selectedRule.fieldName ?? "")}
                </span>
                <button
                  className="column-rule-modal-close"
                  onClick={() => setSelectedRule(null)}
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
                  onClick={() => setSelectedRule(null)}
                  disabled={saving}
                >
                  Cancel
                </button>
                <button
                  className="column-rule-btn-save"
                  disabled={saving || !modalAlgorithmName || !modalDomainName}
                  onClick={() => {
                    const alg = allAlgorithms.find(a => a.algorithmName === modalAlgorithmName);
                    const mt = alg ? String(alg.maskType ?? "") : "";
                    if (mt && allowedAlgorithmTypes.length > 0 && !allowedAlgorithmTypes.includes(mt)) {
                      setTypeMismatchConfirm({ maskType: mt, sqlType: selectedColumnSqlType });
                      return;
                    }
                    const fieldName = typeof selectedRule.fieldName === "string" ? selectedRule.fieldName : "";
                    if (fieldName) {
                      setMismatchedColumns(prev => {
                        const next = new Set(prev);
                        next.delete(fieldName);
                        return next;
                      });
                    }
                    const id = selectedRule.fileFieldMetadataId;
                    if (typeof id !== "string" && typeof id !== "number") return;
                    setSaving(true);
                    (async () => {
                      try {
                        await onSaveColumnRule({
                          fileFieldMetadataId: String(id),
                          algorithmName: modalAlgorithmName,
                          domainName: modalDomainName,
                        });
                        setSelectedRule(null);
                      } catch {
                        // keep modal open on error
                      } finally {
                        setSaving(false);
                      }
                    })();
                  }}
                >
                  {saving ? "Saving..." : "Save"}
                </button>
              </div>
            </div>
          </div>
        );
      })()}
      {typeMismatchConfirm && selectedRule && (
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
                onClick={() => {
                  const fieldName = typeof selectedRule.fieldName === "string" ? selectedRule.fieldName : "";
                  if (fieldName) {
                    setMismatchedColumns(prev => new Set(prev).add(fieldName));
                  }
                  setTypeMismatchConfirm(null);
                  const id = selectedRule.fileFieldMetadataId;
                  if (typeof id !== "string" && typeof id !== "number") return;
                  setSaving(true);
                  (async () => {
                    try {
                      await onSaveColumnRule({
                        fileFieldMetadataId: String(id),
                        algorithmName: modalAlgorithmName,
                        domainName: modalDomainName,
                      });
                      setSelectedRule(null);
                    } catch {
                      // keep modal open on error
                    } finally {
                      setSaving(false);
                    }
                  })();
                }}
              >
                {saving ? "Saving..." : "Confirm"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
