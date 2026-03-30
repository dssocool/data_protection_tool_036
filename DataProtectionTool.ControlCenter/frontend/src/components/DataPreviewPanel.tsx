import { useCallback, useEffect, useMemo, useRef, useState, type UIEvent } from "react";
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
  dryRuns,
  activeTab,
  diffTab,
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
  const bodyRef = useRef<HTMLDivElement>(null);
  const columnRulesRef = useRef<HTMLDivElement>(null);
  const isSyncing = useRef(false);

  const handleBodyScroll = useCallback((e: UIEvent<HTMLDivElement>) => {
    if (isSyncing.current) return;
    isSyncing.current = true;
    if (columnRulesRef.current) {
      columnRulesRef.current.scrollLeft = (e.target as HTMLDivElement).scrollLeft;
    }
    isSyncing.current = false;
  }, []);

  const handleColumnRulesScroll = useCallback((e: UIEvent<HTMLDivElement>) => {
    if (isSyncing.current) return;
    isSyncing.current = true;
    if (bodyRef.current) {
      bodyRef.current.scrollLeft = (e.target as HTMLDivElement).scrollLeft;
    }
    isSyncing.current = false;
  }, []);

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
      <div className="data-preview-body" ref={bodyRef} onScroll={handleBodyScroll}>
        <div className="data-preview-scroll-content">
          <div className="data-preview-data-area">
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
      </div>
      {currentHeaders.length > 0 && (
        <div className="data-preview-column-rules">
          <div className="data-preview-column-rules-header">
            <span className="data-preview-column-rules-tab">Column Rules</span>
          </div>
          <div className="data-preview-column-rules-scroll" ref={columnRulesRef} onScroll={handleColumnRulesScroll}>
            <table className="data-preview-table data-preview-column-rules-table">
              <tbody>
                <tr>
                  {currentHeaders.map((_, i) => (
                    <td key={i}></td>
                  ))}
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
