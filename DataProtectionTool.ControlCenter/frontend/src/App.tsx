import { useCallback, useEffect, useState } from "react";
import MenuBar from "./components/MenuBar";
import SqlServerConnectionModal from "./components/SqlServerConnectionModal";
import type { SqlServerConnectionData, ValidateResult } from "./components/SqlServerConnectionModal";
import ConnectionsPanel from "./components/ConnectionsPanel";
import type { SavedConnection, TableInfo } from "./components/ConnectionsPanel";
import DataPreviewPanel from "./components/DataPreviewPanel";
import type { PreviewData } from "./components/DataPreviewPanel";
import "./App.css";

function getAgentPath(): string | null {
  const segments = window.location.pathname.split("/");
  const agentsIdx = segments.indexOf("agents");
  if (agentsIdx === -1 || agentsIdx + 1 >= segments.length) return null;
  return segments[agentsIdx + 1];
}

export default function App() {
  const [showSqlModal, setShowSqlModal] = useState(false);
  const [showConnections, setShowConnections] = useState(true);
  const [connections, setConnections] = useState<SavedConnection[]>([]);
  const [connectionTables, setConnectionTables] = useState<Record<string, TableInfo[]>>({});
  const [loadingTables, setLoadingTables] = useState<Set<string>>(new Set());
  const [selectedTable, setSelectedTable] = useState<{ rowKey: string; schema: string; tableName: string } | null>(null);
  const [previewData, setPreviewData] = useState<PreviewData | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [connectionsPanelWidth, setConnectionsPanelWidth] = useState(260);

  const fetchConnections = useCallback(async () => {
    const agentPath = getAgentPath();
    if (!agentPath) return;
    try {
      const res = await fetch(`/api/agents/${agentPath}/connections`);
      if (res.ok) {
        const data = await res.json();
        setConnections(data);
      }
    } catch {
      // silently ignore fetch errors
    }
  }, []);

  useEffect(() => {
    fetchConnections();
  }, [fetchConnections]);

  function handleSqlServerConnection() {
    setShowSqlModal(true);
  }

  function handleViewConnections() {
    setShowConnections(true);
  }

  function handleViewFlows() {
    console.log("View -> Flows");
  }

  async function handleSave(data: SqlServerConnectionData) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    await fetch(`/api/agents/${agentPath}/save-connection`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });

    setShowSqlModal(false);
    fetchConnections();
  }

  async function handleExpandConnection(rowKey: string) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    setLoadingTables((prev) => new Set(prev).add(rowKey));

    try {
      const res = await fetch(`/api/agents/${agentPath}/list-tables`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rowKey }),
      });

      if (res.ok) {
        const result = await res.json();
        if (result.success && result.tables) {
          setConnectionTables((prev) => ({ ...prev, [rowKey]: result.tables }));
        } else {
          setConnectionTables((prev) => ({ ...prev, [rowKey]: [] }));
        }
      }
    } catch {
      setConnectionTables((prev) => ({ ...prev, [rowKey]: [] }));
    } finally {
      setLoadingTables((prev) => {
        const next = new Set(prev);
        next.delete(rowKey);
        return next;
      });
    }
  }

  function parseCsv(text: string): PreviewData {
    const rows: string[][] = [];
    let i = 0;
    while (i < text.length) {
      const row: string[] = [];
      while (i < text.length) {
        if (text[i] === '"') {
          i++;
          let val = "";
          while (i < text.length) {
            if (text[i] === '"') {
              if (i + 1 < text.length && text[i + 1] === '"') {
                val += '"';
                i += 2;
              } else {
                i++;
                break;
              }
            } else {
              val += text[i];
              i++;
            }
          }
          row.push(val);
        } else {
          let val = "";
          while (i < text.length && text[i] !== ',' && text[i] !== '\n' && text[i] !== '\r') {
            val += text[i];
            i++;
          }
          row.push(val);
        }
        if (i < text.length && text[i] === ',') {
          i++;
        } else {
          break;
        }
      }
      if (i < text.length && text[i] === '\r') i++;
      if (i < text.length && text[i] === '\n') i++;
      if (row.length > 0 && !(row.length === 1 && row[0] === "")) {
        rows.push(row);
      }
    }
    return { headers: rows[0] ?? [], rows: rows.slice(1) };
  }

  async function handleTableClick(rowKey: string, schema: string, tableName: string) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    setSelectedTable({ rowKey, schema, tableName });
    setPreviewLoading(true);
    setPreviewError(null);
    setPreviewData(null);

    try {
      const res = await fetch(`/api/agents/${agentPath}/preview-table`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rowKey, schema, tableName }),
      });

      if (!res.ok) {
        setPreviewError(`Server error: ${res.status}`);
        return;
      }

      const result = await res.json();
      if (!result.success) {
        setPreviewError(result.message ?? "Preview failed.");
        return;
      }

      const blobRes = await fetch(`/api/blob/${result.filename}`);
      if (!blobRes.ok) {
        setPreviewError(`Failed to fetch CSV: ${blobRes.status}`);
        return;
      }

      const csvText = await blobRes.text();
      setPreviewData(parseCsv(csvText));
    } catch (e) {
      setPreviewError(`Preview failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setPreviewLoading(false);
    }
  }

  async function handleValidate(data: SqlServerConnectionData): Promise<ValidateResult> {
    const agentPath = getAgentPath();
    if (!agentPath) {
      return { success: false, message: "No agent path found in URL. Open this page via an agent URL." };
    }

    const res = await fetch(`/api/agents/${agentPath}/validate-sql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });

    if (!res.ok) {
      const text = await res.text();
      return { success: false, message: `Error: ${text}` };
    }

    const result = await res.json();
    return {
      success: result.success ?? false,
      message: result.message ?? result.status ?? "Unknown result",
    };
  }

  return (
    <div className="app">
      <MenuBar
        onSqlServerConnection={handleSqlServerConnection}
        onViewConnections={handleViewConnections}
        onViewFlows={handleViewFlows}
      />
      <main className="app-content">
        {showConnections && (
          <ConnectionsPanel
            connections={connections}
            connectionTables={connectionTables}
            loadingTables={loadingTables}
            selectedTable={selectedTable}
            onExpandConnection={handleExpandConnection}
            onTableClick={handleTableClick}
            onReloadPreview={handleTableClick}
            onClose={() => setShowConnections(false)}
            onWidthChange={setConnectionsPanelWidth}
          />
        )}
        {selectedTable && (
          <DataPreviewPanel
            tableName={`${selectedTable.schema}.${selectedTable.tableName}`}
            loading={previewLoading}
            error={previewError}
            data={previewData}
            panelLeft={showConnections ? connectionsPanelWidth + 16 : 0}
            onClose={() => {
              setSelectedTable(null);
              setPreviewData(null);
              setPreviewError(null);
            }}
          />
        )}
      </main>
      {showSqlModal && (
        <SqlServerConnectionModal
          onClose={() => setShowSqlModal(false)}
          onSave={handleSave}
          onValidate={handleValidate}
        />
      )}
    </div>
  );
}
