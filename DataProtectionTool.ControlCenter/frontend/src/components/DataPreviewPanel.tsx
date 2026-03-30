import { useMemo } from "react";
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
  tableName: string;
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
  tableName,
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
  const tabs = useMemo(() => {
    const list: string[] = ["Original"];
    if (maskedData) list.push("Masked");
    if (diffTab) list.push(diffTab.name);
    return list;
  }, [maskedData, diffTab]);

  const diffPairs = useMemo(() => {
    const dataTabs = ["Original"];
    if (maskedData) dataTabs.push("Masked");
    if (dataTabs.length < 2) return [];
    const pairs: { label: string; left: string; right: string }[] = [];
    for (let i = 0; i < dataTabs.length; i++) {
      for (let j = i + 1; j < dataTabs.length; j++) {
        pairs.push({
          label: `${dataTabs[i]} vs ${dataTabs[j]}`,
          left: dataTabs[i],
          right: dataTabs[j],
        });
      }
    }
    return pairs;
  }, [maskedData]);

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
        <div style={{ display: "flex", alignItems: "baseline", gap: 12, minWidth: 0, flex: 1 }}>
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
          <span className="data-preview-table-name">{tableName}</span>
          {diffPairs.length > 0 && (
            <select
              className="data-preview-diff-select"
              value=""
              onChange={(e) => {
                const idx = parseInt(e.target.value, 10);
                if (!isNaN(idx)) {
                  const pair = diffPairs[idx];
                  onDiffSelect(pair.left, pair.right);
                }
              }}
            >
              <option value="" disabled>Diff...</option>
              {diffPairs.map((pair, i) => (
                <option key={i} value={i}>{pair.label}</option>
              ))}
            </select>
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
