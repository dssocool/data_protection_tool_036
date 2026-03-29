import "./DataPreviewPanel.css";

export interface PreviewData {
  headers: string[];
  rows: string[][];
}

interface DataPreviewPanelProps {
  tableName: string;
  loading: boolean;
  error: string | null;
  data: PreviewData | null;
  panelLeft: number;
  onClose: () => void;
}

export default function DataPreviewPanel({
  tableName,
  loading,
  error,
  data,
  panelLeft,
  onClose,
}: DataPreviewPanelProps) {
  return (
    <div className="data-preview-panel" style={{ left: panelLeft + 16 }}>
      <div className="data-preview-header">
        <div style={{ display: "flex", alignItems: "baseline", gap: 12, minWidth: 0, flex: 1 }}>
          <div className="data-preview-tabs">
            <button className="data-preview-tab data-preview-tab-active">Original</button>
          </div>
          <span className="data-preview-table-name">{tableName}</span>
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
        ) : data ? (
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
        ) : null}
      </div>
    </div>
  );
}
