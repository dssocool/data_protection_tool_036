import { useEffect, useMemo, useState } from "react";
import "./DataPreviewPanel.css";

export interface PreviewData {
  headers: string[];
  rows: string[][];
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
  maskedData: PreviewData | null;
  activeTab: string;
  diffTab: DiffTab | null;
  onTabChange: (tab: string) => void;
  onDiffSelect: (leftTab: string, rightTab: string) => void;
  onDiffClose: () => void;
  panelLeft: number;
  onClose: () => void;
}

function resolveTabData(
  tab: string,
  data: PreviewData | null,
  originalData: PreviewData | null,
  maskedData: PreviewData | null,
): PreviewData | null {
  if (tab === "Masked") return maskedData;
  if (tab === "Original") return originalData ?? data;
  return null;
}

function DataTable({ data }: { data: PreviewData }) {
  return (
    <table className="data-preview-table">
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
}

function DiffView({ left, right }: { left: PreviewData; right: PreviewData }) {
  const headers = left.headers;
  const maxRows = Math.max(left.rows.length, right.rows.length);

  return (
    <table className="data-preview-table data-preview-diff-table">
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
}

export default function DataPreviewPanel({
  loading,
  error,
  data,
  originalData,
  maskedData,
  activeTab,
  diffTab,
  onTabChange,
  onDiffSelect,
  onDiffClose,
  panelLeft,
  onClose,
}: DataPreviewPanelProps) {
  const dataTabs = useMemo(() => {
    const list: string[] = ["Original"];
    if (maskedData) list.push("Masked");
    return list;
  }, [maskedData]);

  const tabs = useMemo(() => {
    const list: string[] = ["Original"];
    if (maskedData) list.push("Masked");
    if (diffTab) list.push(diffTab.name);
    return list;
  }, [maskedData, diffTab]);

  const [leftDiffTab, setLeftDiffTab] = useState("Original");
  const [rightDiffTab, setRightDiffTab] = useState("");

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
    : resolveTabData(activeTab, data, originalData, maskedData);

  const leftData = isDiffActive
    ? resolveTabData(diffTab!.leftTab, data, originalData, maskedData)
    : null;
  const rightData = isDiffActive
    ? resolveTabData(diffTab!.rightTab, data, originalData, maskedData)
    : null;

  return (
    <div className="data-preview-panel" style={{ left: panelLeft + 16 }}>
      <div className="data-preview-header">
        <div style={{ display: "flex", alignItems: "center", gap: 12, minWidth: 0, flex: 1 }}>
          <div className="data-preview-tabs">
            {tabs.map((tab) => (
              <button
                key={tab}
                className={`data-preview-tab${activeTab === tab ? " data-preview-tab-active" : ""}`}
                onClick={() => onTabChange(tab)}
              >
                {tab}
                {diffTab && tab === diffTab.name && (
                  <span
                    className="data-preview-tab-close"
                    onClick={(e) => {
                      e.stopPropagation();
                      onDiffClose();
                    }}
                  >
                    ×
                  </span>
                )}
              </button>
            ))}
          </div>
          {dataTabs.length > 1 && (
            <div className="data-preview-diff-controls">
              <label className="data-preview-diff-select-label">
                Left
                <select
                  className="data-preview-diff-select"
                  value={leftDiffTab}
                  onChange={(e) => {
                    const nextLeft = e.target.value;
                    setLeftDiffTab(nextLeft);
                    if (nextLeft && rightDiffTab && nextLeft !== rightDiffTab) {
                      onDiffSelect(nextLeft, rightDiffTab);
                    }
                  }}
                >
                  {dataTabs.map((tab) => (
                    <option key={tab} value={tab} disabled={tab === rightDiffTab}>
                      {tab}
                    </option>
                  ))}
                </select>
              </label>
              <label className="data-preview-diff-select-label">
                Right
                <select
                  className="data-preview-diff-select"
                  value={rightDiffTab}
                  onChange={(e) => {
                    const nextRight = e.target.value;
                    setRightDiffTab(nextRight);
                    if (leftDiffTab && nextRight && leftDiffTab !== nextRight) {
                      onDiffSelect(leftDiffTab, nextRight);
                    }
                  }}
                >
                  {dataTabs.map((tab) => (
                    <option key={tab} value={tab} disabled={tab === leftDiffTab}>
                      {tab}
                    </option>
                  ))}
                </select>
              </label>
            </div>
          )}
        </div>
        <button
          className="data-preview-close"
          onClick={onClose}
          aria-label="Close preview"
        >
          <svg width="14" height="14" viewBox="0 0 14 14">
            <path d="M3 3 L11 11 M11 3 L3 11" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
        </button>
      </div>
      <div className="data-preview-body">
        {loading ? (
          <div className="data-preview-loading">Loading preview...</div>
        ) : error ? (
          <div className="data-preview-error">{error}</div>
        ) : isDiffActive && leftData && rightData ? (
          <DiffView left={leftData} right={rightData} />
        ) : activeData ? (
          <DataTable data={activeData} />
        ) : null}
      </div>
    </div>
  );
}
