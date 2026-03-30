import { forwardRef, useCallback, useEffect, useMemo, useRef, useState } from "react";
import "./DataPreviewPanel.css";

export interface PreviewData {
  headers: string[];
  rows: string[][];
}

export interface DryRunResult {
  label: string;
  data: PreviewData;
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
  columnRulesLoading: boolean;
  onTabChange: (tab: string) => void;
  onTabClose: (tab: string) => void;
  onDiffSelect: (leftTab: string, rightTab: string) => void;
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
              <th key={i}>{h}</th>
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
              <th key={i}>{h}</th>
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
  columnRulesLoading,
  onTabChange,
  onTabClose,
  onDiffSelect,
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
  const [colWidths, setColWidths] = useState<number[]>([]);
  const [selectedRule, setSelectedRule] = useState<Record<string, unknown> | null>(null);

  const handleDataScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
    if (columnRulesScrollRef.current) {
      columnRulesScrollRef.current.scrollLeft = e.currentTarget.scrollLeft;
    }
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

  useEffect(() => {
    const table = tableRef.current;
    if (!table) return;
    const ths = table.querySelectorAll("thead th");
    if (!ths.length) return;
    const measure = () => {
      const widths = Array.from(ths).map((th) => th.getBoundingClientRect().width);
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
      <div className="data-preview-body" onScroll={handleDataScroll}>
        {loading ? (
          <div className="data-preview-loading">Loading preview...</div>
        ) : error ? (
          <div className="data-preview-error">{error}</div>
        ) : isDiffActive && leftData && rightData ? (
          <DiffView left={leftData} right={rightData} ref={tableRef} />
        ) : activeData ? (
          <DataTable data={activeData} ref={tableRef} />
        ) : null}
      </div>
      {currentHeaders.length > 0 && (
        <div className="data-preview-column-rules">
          <div className="data-preview-column-rules-header">
            <span className="data-preview-column-rules-tab">Column Rules</span>
          </div>
          <div className="data-preview-column-rules-scroll" ref={columnRulesScrollRef}>
            <table className="data-preview-table data-preview-column-rules-table">
              {colWidths.length > 0 && (
                <colgroup>
                  {colWidths.map((w, i) => (
                    <col key={i} style={{ width: w, minWidth: w }} />
                  ))}
                </colgroup>
              )}
              <tbody>
                <tr>
                  {currentHeaders.map((header, i) => {
                    const rule = rulesByField.get(header);
                    return (
                      <td key={i}>
                        {columnRulesLoading ? (
                          <span className="column-rule-loading">...</span>
                        ) : rule ? (
                          <button
                            className="column-rule-btn"
                            onClick={() => setSelectedRule(rule)}
                            title={`View rule for ${header}`}
                          >
                            {header}
                          </button>
                        ) : null}
                      </td>
                    );
                  })}
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      )}
      {selectedRule && (
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
              <pre className="column-rule-json">
                {JSON.stringify(selectedRule, null, 2)}
              </pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
