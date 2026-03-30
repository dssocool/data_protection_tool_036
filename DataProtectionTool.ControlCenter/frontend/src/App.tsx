import { useCallback, useEffect, useRef, useState } from "react";
import MenuBar from "./components/MenuBar";
import SqlServerConnectionModal from "./components/SqlServerConnectionModal";
import type { SqlServerConnectionData, ValidateResult } from "./components/SqlServerConnectionModal";
import QueryModal from "./components/QueryModal";
import type { QuerySaveData, QueryValidateResult } from "./components/QueryModal";
import ConnectionsPanel from "./components/ConnectionsPanel";
import type { SavedConnection, TableInfo, QueryInfo } from "./components/ConnectionsPanel";
import DataPreviewPanel from "./components/DataPreviewPanel";
import type { PreviewData } from "./components/DataPreviewPanel";
import StatusBar from "./components/StatusBar";
import type { StatusEvent } from "./components/StatusBar";
import EventDialog from "./components/EventDialog";
import "./App.css";

function getAgentPath(): string | null {
  const segments = window.location.pathname.split("/");
  const agentsIdx = segments.indexOf("agents");
  if (agentsIdx === -1 || agentsIdx + 1 >= segments.length) return null;
  return segments[agentsIdx + 1];
}

export default function App() {
  const [showSqlModal, setShowSqlModal] = useState(false);
  const [showQueryModal, setShowQueryModal] = useState(false);
  const [showConnections, setShowConnections] = useState(true);
  const [connections, setConnections] = useState<SavedConnection[]>([]);
  const [connectionTables, setConnectionTables] = useState<Record<string, TableInfo[]>>({});
  const [connectionQueries, setConnectionQueries] = useState<Record<string, QueryInfo[]>>({});
  const [loadingTables, setLoadingTables] = useState<Set<string>>(new Set());
  const [selectedTable, setSelectedTable] = useState<{ rowKey: string; schema: string; tableName: string } | null>(null);
  const [selectedQuery, setSelectedQuery] = useState<{ connectionRowKey: string; queryRowKey: string; queryText: string } | null>(null);
  const [previewData, setPreviewData] = useState<PreviewData | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [previewBlobFilenames, setPreviewBlobFilenames] = useState<string[]>([]);
  const [originalData, setOriginalData] = useState<PreviewData | null>(null);
  const [maskedData, setMaskedData] = useState<PreviewData | null>(null);
  const [activePreviewTab, setActivePreviewTab] = useState("Original");
  const [diffTab, setDiffTab] = useState<{ name: string; leftTab: string; rightTab: string } | null>(null);
  const [connectionsPanelWidth, setConnectionsPanelWidth] = useState(260);
  const [statusEvents, setStatusEvents] = useState<StatusEvent[]>([]);
  const [showEventDialog, setShowEventDialog] = useState(false);
  const [agentOid, setAgentOid] = useState("");
  const [agentTid, setAgentTid] = useState("");
  const [userUniqueId, setUserUniqueId] = useState<string | null>(null);
  const eventsTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchEvents = useCallback(async () => {
    const agentPath = getAgentPath();
    if (!agentPath) return;
    try {
      const res = await fetch(`/api/agents/${agentPath}/events`);
      if (res.ok) {
        const data = await res.json();
        setStatusEvents(data);
      }
    } catch {
      // silently ignore
    }
  }, []);

  useEffect(() => {
    fetchEvents();
    eventsTimerRef.current = setInterval(fetchEvents, 10000);
    return () => {
      if (eventsTimerRef.current) clearInterval(eventsTimerRef.current);
    };
  }, [fetchEvents]);

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

  useEffect(() => {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    fetch(`/api/agents/${agentPath}`)
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (data) {
          setAgentOid(data.oid ?? "");
          setAgentTid(data.tid ?? "");
        }
      })
      .catch(() => {});

    fetch(`/api/agents/${agentPath}/user-id`)
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (data) setUserUniqueId(data.uniqueId ?? null);
      })
      .catch(() => {});
  }, []);

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
      const [tablesRes, queriesRes] = await Promise.all([
        fetch(`/api/agents/${agentPath}/list-tables`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ rowKey }),
        }),
        fetch(`/api/agents/${agentPath}/queries?connectionRowKey=${encodeURIComponent(rowKey)}`),
      ]);

      if (tablesRes.ok) {
        const result = await tablesRes.json();
        if (result.success && result.tables) {
          setConnectionTables((prev) => ({ ...prev, [rowKey]: result.tables }));
        } else {
          setConnectionTables((prev) => ({ ...prev, [rowKey]: [] }));
        }
      }

      if (queriesRes.ok) {
        const queries = await queriesRes.json();
        setConnectionQueries((prev) => ({ ...prev, [rowKey]: queries }));
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

  async function fetchPreviewFromFilenames(filenames: string[]) {
    setPreviewBlobFilenames(filenames);

    const mergeRes = await fetch("/api/blob/preview-merge", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filenames }),
    });
    if (!mergeRes.ok) {
      setPreviewError(`Failed to fetch preview data: ${mergeRes.status}`);
      return;
    }

    const data = await mergeRes.json();
    setPreviewData(data as PreviewData);
  }

  async function deletePreviewBlobs(filenames: string[]) {
    if (filenames.length === 0) return;
    try {
      await fetch("/api/blob/delete-preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ filenames }),
      });
    } catch {
      // best-effort cleanup
    }
  }

  async function handleTableClick(rowKey: string, schema: string, tableName: string) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    setSelectedTable({ rowKey, schema, tableName });
    setSelectedQuery(null);
    setPreviewLoading(true);
    setPreviewError(null);
    setPreviewData(null);
    setOriginalData(null);
    setMaskedData(null);
    setActivePreviewTab("Original");
    setDiffTab(null);

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

      const filenames: string[] = result.filenames ?? (result.filename ? [result.filename] : []);
      await fetchPreviewFromFilenames(filenames);
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

  function handleNewQuery() {
    setShowQueryModal(true);
  }

  async function handleValidateQuery(data: QuerySaveData): Promise<QueryValidateResult> {
    const agentPath = getAgentPath();
    if (!agentPath) {
      return { success: false, message: "No agent path found in URL." };
    }

    const res = await fetch(`/api/agents/${agentPath}/validate-query`, {
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
      message: result.message ?? "Unknown result",
    };
  }

  async function handleSaveQuery(data: QuerySaveData) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    const res = await fetch(`/api/agents/${agentPath}/save-query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });

    if (res.ok) {
      const result = await res.json();
      if (result.success) {
        setShowQueryModal(false);
        const queriesRes = await fetch(
          `/api/agents/${agentPath}/queries?connectionRowKey=${encodeURIComponent(data.connectionRowKey)}`
        );
        if (queriesRes.ok) {
          const queries = await queriesRes.json();
          setConnectionQueries((prev) => ({ ...prev, [data.connectionRowKey]: queries }));
        }
      }
    }
  }

  async function handleQueryClick(connectionRowKey: string, queryRowKey: string, queryText: string) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    setSelectedQuery({ connectionRowKey, queryRowKey, queryText });
    setSelectedTable(null);
    setPreviewLoading(true);
    setPreviewError(null);
    setPreviewData(null);
    setOriginalData(null);
    setMaskedData(null);
    setActivePreviewTab("Original");
    setDiffTab(null);

    try {
      const res = await fetch(`/api/agents/${agentPath}/preview-query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ connectionRowKey, queryText }),
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

      const filenames: string[] = result.filenames ?? (result.filename ? [result.filename] : []);
      await fetchPreviewFromFilenames(filenames);
    } catch (e) {
      setPreviewError(`Preview failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setPreviewLoading(false);
    }
  }

  async function handleReloadPreview() {
    await deletePreviewBlobs(previewBlobFilenames);
    setPreviewBlobFilenames([]);

    if (selectedTable) {
      handleTableClick(selectedTable.rowKey, selectedTable.schema, selectedTable.tableName);
    } else if (selectedQuery) {
      handleQueryClick(selectedQuery.connectionRowKey, selectedQuery.queryRowKey, selectedQuery.queryText);
    }
  }

  async function handleDryRun(rowKey: string, schema: string, tableName: string) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    if (previewData) {
      setOriginalData({ ...previewData });
    }

    try {
      const res = await fetch(`/api/agents/${agentPath}/dry-run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rowKey, schema, tableName, previewBlobFilenames }),
      });

      if (!res.ok) {
        console.error("Dry run request failed:", res.status);
        return;
      }

      const result = await res.json();
      if (result.success) {
        const mergeRes = await fetch("/api/blob/preview-merge", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ filenames: previewBlobFilenames }),
        });
        if (mergeRes.ok) {
          const masked = await mergeRes.json();
          setMaskedData(masked as PreviewData);
          setActivePreviewTab("Masked");
        }
      } else {
        console.error("Dry run failed:", result.message);
      }
    } catch (e) {
      console.error("Dry run error:", e instanceof Error ? e.message : String(e));
    }
  }

  return (
    <div className="app">
      <MenuBar
        onSqlServerConnection={handleSqlServerConnection}
        onNewQuery={handleNewQuery}
        onViewConnections={handleViewConnections}
        onViewFlows={handleViewFlows}
        oid={agentOid}
        tid={agentTid}
        uniqueId={userUniqueId}
      />
      <main className="app-content">
        {showConnections && (
          <ConnectionsPanel
            connections={connections}
            connectionTables={connectionTables}
            connectionQueries={connectionQueries}
            loadingTables={loadingTables}
            selectedTable={selectedTable}
            selectedQuery={selectedQuery}
            onExpandConnection={handleExpandConnection}
            onTableClick={handleTableClick}
            onQueryClick={handleQueryClick}
            onReloadPreview={handleReloadPreview}
            onDryRun={handleDryRun}
            onClose={() => setShowConnections(false)}
            onWidthChange={setConnectionsPanelWidth}
          />
        )}
        {(selectedTable || selectedQuery) && (
          <DataPreviewPanel
            tableName={
              selectedTable
                ? `${selectedTable.schema}.${selectedTable.tableName}`
                : `Query Preview`
            }
            loading={previewLoading}
            error={previewError}
            data={previewData}
            originalData={originalData}
            maskedData={maskedData}
            activeTab={activePreviewTab}
            diffTab={diffTab}
            onTabChange={setActivePreviewTab}
            onDiffSelect={(leftTab, rightTab) => {
              const name = `${leftTab} vs ${rightTab}`;
              setDiffTab({ name, leftTab, rightTab });
              setActivePreviewTab(name);
            }}
            onDiffClose={() => {
              setDiffTab(null);
              setActivePreviewTab("Original");
            }}
            panelLeft={showConnections ? connectionsPanelWidth + 16 : 0}
            onClose={() => {
              setSelectedTable(null);
              setSelectedQuery(null);
              setPreviewData(null);
              setPreviewError(null);
              setPreviewBlobFilenames([]);
              setOriginalData(null);
              setMaskedData(null);
              setActivePreviewTab("Original");
              setDiffTab(null);
            }}
          />
        )}
      </main>
      <StatusBar
        events={statusEvents}
        onIconClick={() => setShowEventDialog((v) => !v)}
      />
      {showEventDialog && (
        <EventDialog
          events={statusEvents}
          onClose={() => setShowEventDialog(false)}
        />
      )}
      {showSqlModal && (
        <SqlServerConnectionModal
          onClose={() => setShowSqlModal(false)}
          onSave={handleSave}
          onValidate={handleValidate}
        />
      )}
      {showQueryModal && (
        <QueryModal
          connections={connections}
          onClose={() => setShowQueryModal(false)}
          onSave={handleSaveQuery}
          onValidate={handleValidateQuery}
        />
      )}
    </div>
  );
}
