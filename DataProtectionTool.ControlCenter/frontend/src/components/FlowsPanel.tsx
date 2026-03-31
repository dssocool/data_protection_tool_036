import { useCallback, useEffect, useRef, useState } from "react";
import type { FlowSource, FlowDest } from "./FullRunModal";
import "./FlowsPanel.css";

export interface FlowItem {
  rowKey: string;
  sourceJson: string;
  destJson: string;
  createdAt: string;
}

interface FlowsPanelProps {
  agentPath: string;
  onClose: () => void;
  onSwitchPanel: (panel: "connections" | "flows") => void;
  onWidthChange?: (width: number) => void;
}

const MIN_WIDTH = 200;
const MAX_WIDTH = 500;
const DEFAULT_WIDTH = 260;

export default function FlowsPanel({
  agentPath,
  onClose,
  onSwitchPanel,
  onWidthChange,
}: FlowsPanelProps) {
  const [flows, setFlows] = useState<FlowItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [width, setWidth] = useState(DEFAULT_WIDTH);
  const resizing = useRef(false);
  const startX = useRef(0);
  const startW = useRef(DEFAULT_WIDTH);

  const fetchFlows = useCallback(async () => {
    if (!agentPath) return;
    setLoading(true);
    try {
      const res = await fetch(`/api/agents/${agentPath}/flows`);
      if (res.ok) {
        const data = await res.json();
        setFlows(Array.isArray(data) ? data : []);
      }
    } catch {
      // best-effort
    } finally {
      setLoading(false);
    }
  }, [agentPath]);

  useEffect(() => {
    fetchFlows();
  }, [fetchFlows]);

  useEffect(() => {
    function onMouseMove(e: MouseEvent) {
      if (!resizing.current) return;
      const newW = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, startW.current + (e.clientX - startX.current)));
      setWidth(newW);
      onWidthChange?.(newW);
    }
    function onMouseUp() {
      resizing.current = false;
    }
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
    return () => {
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
    };
  }, [onWidthChange]);

  function parseJson<T>(json: string): T | null {
    try { return JSON.parse(json); } catch { return null; }
  }

  function formatSource(src: FlowSource | null): string {
    if (!src) return "—";
    const db = src.databaseName ? `${src.serverName}/${src.databaseName}` : src.serverName;
    return `${db} > ${src.schema}.${src.tableName}`;
  }

  function formatDest(dest: FlowDest | null): string {
    if (!dest) return "—";
    const db = dest.databaseName ? `${dest.serverName}/${dest.databaseName}` : dest.serverName;
    return `${db} > ${dest.schema}`;
  }

  return (
    <div className="flows-panel" style={{ width }}>
      <div className="flows-panel-header">
        <select
          className="flows-panel-title"
          value="flows"
          onChange={(e) => onSwitchPanel(e.target.value as "connections" | "flows")}
        >
          <option value="connections">Connections</option>
          <option value="flows">Flows</option>
        </select>
        <button className="flows-panel-close" onClick={onClose} title="Close">
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
            <path d="M3 3L11 11M11 3L3 11" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
          </svg>
        </button>
      </div>

      <div className="flows-list">
        {loading ? (
          <p className="flows-panel-empty">Loading...</p>
        ) : flows.length === 0 ? (
          <p className="flows-panel-empty">No flows saved yet.</p>
        ) : (
          flows.map((flow) => {
            const src = parseJson<FlowSource>(flow.sourceJson);
            const dest = parseJson<FlowDest>(flow.destJson);
            return (
              <div key={flow.rowKey} className="flow-item">
                <div className="flow-item-line flow-item-source" title={formatSource(src)}>
                  {formatSource(src)}
                </div>
                <div className="flow-item-line flow-item-dest" title={formatDest(dest)}>
                  {formatDest(dest)}
                </div>
              </div>
            );
          })
        )}
      </div>

      <div
        className="flows-panel-resize"
        onMouseDown={(e) => {
          resizing.current = true;
          startX.current = e.clientX;
          startW.current = width;
          e.preventDefault();
        }}
      />
    </div>
  );
}
