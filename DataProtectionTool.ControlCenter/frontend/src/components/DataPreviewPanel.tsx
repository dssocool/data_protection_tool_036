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
  panelLeft: number;
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
  panelLeft,
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
        <div className="data-preview-tabs">
          {tabs.map((tab) => (
            <button
              key={tab}
              className={`data-preview-tab${activeTab === tab ? " data-preview-tab-active" : ""}`}
              onClick={() => onTabChange(tab)}
            >
              {tab}
            </button>
          ))}
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
