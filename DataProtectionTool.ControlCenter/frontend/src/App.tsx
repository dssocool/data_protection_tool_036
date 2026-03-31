import { useCallback, useEffect, useRef, useState } from "react";
import MenuBar from "./components/MenuBar";
import SqlServerConnectionModal from "./components/SqlServerConnectionModal";
import type { SqlServerConnectionData, ValidateResult } from "./components/SqlServerConnectionModal";
import QueryModal from "./components/QueryModal";
import type { QuerySaveData, QueryValidateResult } from "./components/QueryModal";
import ConnectionsPanel from "./components/ConnectionsPanel";
import type { SavedConnection, TableInfo, QueryInfo } from "./components/ConnectionsPanel";
import DataPreviewPanel from "./components/DataPreviewPanel";
import type { PreviewData, DryRunResult } from "./components/DataPreviewPanel";
import StatusBar from "./components/StatusBar";
import type { StatusEvent } from "./components/StatusBar";
import EventDialog from "./components/EventDialog";
import FullRunModal from "./components/FullRunModal";
import type { FlowSource, FlowDest } from "./components/FullRunModal";
import FlowsPanel from "./components/FlowsPanel";
import "./App.css";

interface TablePreviewCache {
  previewData: PreviewData | null;
  originalData: PreviewData | null;
  dryRuns: DryRunResult[];
  activePreviewTab: string;
  diffTab: { name: string; leftTab: string; rightTab: string } | null;
  previewBlobFilenames: string[];
  previewError: string | null;
  dryRunInProgress: boolean;
  columnRules: Record<string, unknown>[];
  columnRuleAlgorithms: Record<string, unknown>[];
  columnRuleDomains: Record<string, unknown>[];
  columnRuleFrameworks: Record<string, unknown>[];
}

function tableKey(rowKey: string, schema: string, tableName: string) {
  return `${rowKey}:${schema}:${tableName}`;
}

function getAgentPath(): string | null {
  const segments = window.location.pathname.split("/");
  const agentsIdx = segments.indexOf("agents");
  if (agentsIdx === -1 || agentsIdx + 1 >= segments.length) return null;
  return segments[agentsIdx + 1];
}

export default function App() {
  const [showSqlModal, setShowSqlModal] = useState(false);
  const [showQueryModal, setShowQueryModal] = useState(false);
  const [leftPanel, setLeftPanel] = useState<"connections" | "flows" | null>("connections");
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
  const [dryRuns, setDryRuns] = useState<DryRunResult[]>([]);
  const [activePreviewTab, setActivePreviewTab] = useState("Original");
  const [diffTab, setDiffTab] = useState<{ name: string; leftTab: string; rightTab: string } | null>(null);
  const [connectionsPanelWidth, setConnectionsPanelWidth] = useState(260);
  const [statusEvents, setStatusEvents] = useState<StatusEvent[]>([]);
  const [showEventDialog, setShowEventDialog] = useState(false);
  const [agentOid, setAgentOid] = useState("");
  const [agentTid, setAgentTid] = useState("");
  const [userUniqueId, setUserUniqueId] = useState<string | null>(null);
  const [fullRunTarget, setFullRunTarget] = useState<{ rowKey: string; schema: string; tableName: string } | null>(null);
  const [columnRules, setColumnRules] = useState<Record<string, unknown>[]>([]);
  const [columnRuleAlgorithms, setColumnRuleAlgorithms] = useState<Record<string, unknown>[]>([]);
  const [columnRuleDomains, setColumnRuleDomains] = useState<Record<string, unknown>[]>([]);
  const [columnRuleFrameworks, setColumnRuleFrameworks] = useState<Record<string, unknown>[]>([]);
  const [columnRulesLoading, setColumnRulesLoading] = useState(false);
  const [allDomains, setAllDomains] = useState<Record<string, unknown>[]>([]);
  const [allAlgorithms, setAllAlgorithms] = useState<Record<string, unknown>[]>([]);
  const [allFrameworks, setAllFrameworks] = useState<Record<string, unknown>[]>([]);
  const [dryRunningTables, setDryRunningTables] = useState<Set<string>>(new Set());
  const [mismatchedColumns, setMismatchedColumns] = useState<Set<string>>(new Set());
  const eventsTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const previewCacheRef = useRef<Map<string, string[]>>(new Map());
  const tableCacheRef = useRef<Map<string, TablePreviewCache>>(new Map());
  const selectedTableRef = useRef(selectedTable);
  selectedTableRef.current = selectedTable;

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

    fetchEngineMetadata(agentPath);
  }, []);

  function handleSqlServerConnection() {
    setShowSqlModal(true);
  }

  function handleViewConnections() {
    setLeftPanel("connections");
  }

  function handleViewFlows() {
    setLeftPanel("flows");
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

    const alreadyCached = connectionTables[rowKey]?.length > 0;

    if (alreadyCached) {
      try {
        const queriesRes = await fetch(
          `/api/agents/${agentPath}/queries?connectionRowKey=${encodeURIComponent(rowKey)}`
        );
        if (queriesRes.ok) {
          const queries = await queriesRes.json();
          setConnectionQueries((prev) => ({ ...prev, [rowKey]: queries }));
        }
      } catch {
        // queries fetch is best-effort when tables are cached
      }
      return;
    }

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

  async function handleRefreshConnection(rowKey: string) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    setLoadingTables((prev) => new Set(prev).add(rowKey));

    try {
      const tablesRes = await fetch(`/api/agents/${agentPath}/list-tables`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rowKey, refresh: true }),
      });

      if (tablesRes.ok) {
        const result = await tablesRes.json();
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

  async function fetchPreviewFromFilenames(filenames: string[]): Promise<PreviewData | null> {
    setPreviewBlobFilenames(filenames);

    const mergeRes = await fetch("/api/blob/preview-merge", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filenames }),
    });
    if (!mergeRes.ok) {
      setPreviewError(`Failed to fetch preview data: ${mergeRes.status}`);
      return null;
    }

    const data = (await mergeRes.json()) as PreviewData;
    setPreviewData(data);
    return data;
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

  function saveCurrentTableToCache() {
    if (!selectedTable) return;
    const key = tableKey(selectedTable.rowKey, selectedTable.schema, selectedTable.tableName);
    const existing = tableCacheRef.current.get(key);
    tableCacheRef.current.set(key, {
      previewData,
      originalData,
      dryRuns,
      activePreviewTab,
      diffTab,
      previewBlobFilenames,
      previewError,
      dryRunInProgress: existing?.dryRunInProgress ?? false,
      columnRules,
      columnRuleAlgorithms,
      columnRuleDomains,
      columnRuleFrameworks,
    });
  }

  function restoreTableFromCache(cached: TablePreviewCache) {
    setPreviewData(cached.previewData);
    setOriginalData(cached.originalData);
    setDryRuns(cached.dryRuns);
    setActivePreviewTab(cached.activePreviewTab);
    setDiffTab(cached.diffTab);
    setPreviewBlobFilenames(cached.previewBlobFilenames);
    setPreviewError(cached.previewError);
    setPreviewLoading(cached.dryRunInProgress);
    setColumnRules(cached.columnRules);
    setColumnRuleAlgorithms(cached.columnRuleAlgorithms);
    setColumnRuleDomains(cached.columnRuleDomains);
    setColumnRuleFrameworks(cached.columnRuleFrameworks);
  }

  async function fetchColumnRules(
    agentPath: string,
    fileFormatId: string,
    previewHeaders?: string[],
    previewColumnTypes?: string[],
  ) {
    setColumnRulesLoading(true);
    try {
      let url = `/api/agents/${agentPath}/column-rules?fileFormatId=${encodeURIComponent(fileFormatId)}`;
      if (previewHeaders?.length && previewColumnTypes?.length) {
        url += `&headers=${encodeURIComponent(JSON.stringify(previewHeaders))}`;
        url += `&columnTypes=${encodeURIComponent(JSON.stringify(previewColumnTypes))}`;
      }
      const res = await fetch(url);
      if (res.ok) {
        const result = await res.json();
        if (result.success && Array.isArray(result.responseList)) {
          const parseArray = (arr: unknown): Record<string, unknown>[] => {
            if (!Array.isArray(arr)) return [];
            return arr.map((item: unknown) => {
              if (typeof item === "string") {
                try { return JSON.parse(item); } catch { return { raw: item }; }
              }
              return item as Record<string, unknown>;
            });
          };
          setColumnRules(parseArray(result.responseList));
          setColumnRuleAlgorithms(parseArray(result.algorithms));
          setColumnRuleDomains(parseArray(result.domains));
          setColumnRuleFrameworks(parseArray(result.frameworks));
          return parseArray(result.responseList);
        }
      }
    } catch {
      // best-effort
    } finally {
      setColumnRulesLoading(false);
    }
    setColumnRules([]);
    setColumnRuleAlgorithms([]);
    setColumnRuleDomains([]);
    setColumnRuleFrameworks([]);
    return [];
  }

  async function fetchEngineMetadata(agentPath: string) {
    try {
      const res = await fetch(`/api/agents/${agentPath}/engine-metadata`);
      if (res.ok) {
        const result = await res.json();
        if (result.success) {
          const parseArray = (arr: unknown): Record<string, unknown>[] => {
            if (!Array.isArray(arr)) return [];
            return arr.map((item: unknown) => {
              if (typeof item === "string") {
                try { return JSON.parse(item); } catch { return { raw: item }; }
              }
              return item as Record<string, unknown>;
            });
          };
          setAllDomains(parseArray(result.domains));
          setAllAlgorithms(parseArray(result.algorithms));
          setAllFrameworks(parseArray(result.frameworks));
          return;
        }
      }
    } catch {
      // best-effort
    }
    setAllDomains([]);
    setAllAlgorithms([]);
    setAllFrameworks([]);
  }

  async function handleSaveColumnRule(params: {
    fileFieldMetadataId: string;
    algorithmName: string;
    domainName: string;
  }) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    const res = await fetch(
      `/api/agents/${agentPath}/column-rule/${encodeURIComponent(params.fileFieldMetadataId)}`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          algorithmName: params.algorithmName,
          domainName: params.domainName,
        }),
      }
    );

    if (!res.ok) {
      throw new Error(`Server error: ${res.status}`);
    }

    const result = await res.json();
    if (result.success === false) {
      throw new Error(result.message || "Failed to save column rule.");
    }

    if (selectedTable) {
      const tableInfo = connectionTables[selectedTable.rowKey]?.find(
        (t) => t.schema === selectedTable.schema && t.name === selectedTable.tableName
      );
      if (tableInfo?.fileFormatId) {
        const origPreview = originalData ?? previewData;
        await fetchColumnRules(
          agentPath, tableInfo.fileFormatId,
          origPreview?.headers, origPreview?.columnTypes,
        );
      }
    }
  }

  async function handleTableClick(rowKey: string, schema: string, tableName: string) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    saveCurrentTableToCache();

    const key = tableKey(rowKey, schema, tableName);
    const cached = tableCacheRef.current.get(key);

    setSelectedTable({ rowKey, schema, tableName });
    setSelectedQuery(null);

    if (cached) {
      restoreTableFromCache(cached);
      return;
    }

    setPreviewLoading(true);
    setPreviewError(null);
    setPreviewData(null);
    setOriginalData(null);
    setDryRuns([]);
    setActivePreviewTab("Original");
    setDiffTab(null);
    setColumnRules([]);
    setColumnRuleAlgorithms([]);
    setColumnRuleDomains([]);
    setColumnRuleFrameworks([]);
    setMismatchedColumns(new Set());

    const tableInfo = connectionTables[rowKey]?.find(
      (t) => t.schema === schema && t.name === tableName
    );
    if (tableInfo?.fileFormatId) {
      fetchColumnRules(agentPath, tableInfo.fileFormatId);
    }

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
      const preview = await fetchPreviewFromFilenames(filenames);

      if (tableInfo?.fileFormatId && preview?.headers?.length && preview?.columnTypes?.length) {
        fetchColumnRules(agentPath, tableInfo.fileFormatId, preview.headers, preview.columnTypes);
      }
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

    saveCurrentTableToCache();

    setSelectedQuery({ connectionRowKey, queryRowKey, queryText });
    setSelectedTable(null);
    setPreviewLoading(true);
    setPreviewError(null);
    setPreviewData(null);
    setOriginalData(null);
    setDryRuns([]);
    setActivePreviewTab("Original");
    setDiffTab(null);
    setColumnRules([]);
    setColumnRuleAlgorithms([]);
    setColumnRuleDomains([]);
    setColumnRuleFrameworks([]);

    const cacheKey = `query:${connectionRowKey}:${queryRowKey}`;
    const cached = previewCacheRef.current.get(cacheKey);

    try {
      if (cached) {
        await fetchPreviewFromFilenames(cached);
      } else {
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
        previewCacheRef.current.set(cacheKey, filenames);
        await fetchPreviewFromFilenames(filenames);
      }
    } catch (e) {
      setPreviewError(`Preview failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setPreviewLoading(false);
    }
  }

  async function handleReloadPreview() {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    if (selectedTable) {
      const key = tableKey(selectedTable.rowKey, selectedTable.schema, selectedTable.tableName);
      tableCacheRef.current.delete(key);

      setPreviewLoading(true);
      setPreviewError(null);
      setPreviewData(null);
      setOriginalData(null);
      setDryRuns([]);
      setActivePreviewTab("Original");
      setDiffTab(null);

      try {
        const res = await fetch(`/api/agents/${agentPath}/reload-preview-table`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            rowKey: selectedTable.rowKey,
            schema: selectedTable.schema,
            tableName: selectedTable.tableName,
          }),
        });

        if (!res.ok) {
          setPreviewError(`Server error: ${res.status}`);
          return;
        }

        const result = await res.json();
        if (!result.success) {
          setPreviewError(result.message ?? "Reload preview failed.");
          return;
        }

        const filenames: string[] = result.filenames ?? (result.filename ? [result.filename] : []);
        await fetchPreviewFromFilenames(filenames);
      } catch (e) {
        setPreviewError(`Reload preview failed: ${e instanceof Error ? e.message : String(e)}`);
      } finally {
        setPreviewLoading(false);
      }
    } else if (selectedQuery) {
      const cacheKey = `query:${selectedQuery.connectionRowKey}:${selectedQuery.queryRowKey}`;
      previewCacheRef.current.delete(cacheKey);

      await deletePreviewBlobs(previewBlobFilenames);
      setPreviewBlobFilenames([]);

      handleQueryClick(selectedQuery.connectionRowKey, selectedQuery.queryRowKey, selectedQuery.queryText);
    }
  }

  function isViewingTable(rowKey: string, schema: string, tName: string): boolean {
    const cur = selectedTableRef.current;
    return cur?.rowKey === rowKey && cur?.schema === schema && cur?.tableName === tName;
  }

  async function handleDryRun(rowKey: string, schema: string, tableName: string) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    const key = tableKey(rowKey, schema, tableName);
    const isSameTable = selectedTable?.rowKey === rowKey
      && selectedTable?.schema === schema
      && selectedTable?.tableName === tableName;

    if (!isSameTable) {
      saveCurrentTableToCache();
    }

    setSelectedTable({ rowKey, schema, tableName });
    setSelectedQuery(null);
    setPreviewLoading(true);
    setPreviewError(null);

    let filenames = isSameTable ? previewBlobFilenames : [];
    const cachedEntry = tableCacheRef.current.get(key);
    if (!isSameTable && cachedEntry && cachedEntry.previewBlobFilenames.length > 0) {
      filenames = cachedEntry.previewBlobFilenames;
      restoreTableFromCache(cachedEntry);
      setPreviewLoading(true);
    }

    try {
      if (filenames.length === 0) {
        setPreviewData(null);
        setOriginalData(null);
        setDryRuns([]);
        setActivePreviewTab("Original");
        setDiffTab(null);

        const previewRes = await fetch(`/api/agents/${agentPath}/preview-table`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ rowKey, schema, tableName }),
        });

        if (!previewRes.ok) {
          setPreviewError(`Preview failed: server error ${previewRes.status}`);
          setPreviewLoading(false);
          return;
        }

        const previewResult = await previewRes.json();
        if (!previewResult.success) {
          setPreviewError(previewResult.message ?? "Preview failed.");
          setPreviewLoading(false);
          return;
        }

        filenames = previewResult.filenames ?? (previewResult.filename ? [previewResult.filename] : []);
        await fetchPreviewFromFilenames(filenames);
      }

      if (previewData) {
        setOriginalData({ ...previewData });
      }

      const currentCached = tableCacheRef.current.get(key);
      const prevDryRuns = currentCached?.dryRuns ?? dryRuns;
      const newLabel = `Dry Run ${prevDryRuns.length + 1}`;
      const pendingDryRun: DryRunResult = { label: newLabel, data: null, status: "Starting dry run...", inProgress: true };
      const updatedDryRunsWithPending = [...prevDryRuns, pendingDryRun];

      setDryRuns(updatedDryRunsWithPending);
      setActivePreviewTab(newLabel);
      setPreviewLoading(false);

      tableCacheRef.current.set(key, {
        ...(currentCached ?? {
          previewData, originalData, dryRuns: prevDryRuns, activePreviewTab,
          diffTab, previewBlobFilenames: filenames, previewError: null,
          columnRules, columnRuleAlgorithms, columnRuleDomains, columnRuleFrameworks,
        }),
        previewBlobFilenames: filenames,
        dryRuns: updatedDryRunsWithPending,
        activePreviewTab: newLabel,
        dryRunInProgress: true,
      } as TablePreviewCache);
      setDryRunningTables((prev) => new Set(prev).add(key));

      const updateDryRunStatus = (status: string) => {
        setDryRuns((prev) =>
          prev.map((dr) => dr.label === newLabel ? { ...dr, status } : dr),
        );
        const cached = tableCacheRef.current.get(key);
        if (cached) {
          tableCacheRef.current.set(key, {
            ...cached,
            dryRuns: cached.dryRuns.map((dr) => dr.label === newLabel ? { ...dr, status } : dr),
          });
        }
      };

      const finalizeDryRunError = (errMsg: string) => {
        setDryRuns((prev) => prev.filter((dr) => dr.label !== newLabel));
        setActivePreviewTab("Original");
        setPreviewError(errMsg);
        const cached = tableCacheRef.current.get(key);
        if (cached) {
          tableCacheRef.current.set(key, {
            ...cached,
            dryRuns: cached.dryRuns.filter((dr) => dr.label !== newLabel),
            activePreviewTab: "Original",
            dryRunInProgress: false,
            previewError: errMsg,
          });
        }
        setDryRunningTables((prev) => { const next = new Set(prev); next.delete(key); return next; });
      };

      try {
        const response = await fetch(`/api/agents/${agentPath}/dry-run`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            rowKey, schema, tableName, previewBlobFilenames: filenames,
            previewHeaders: previewData?.headers ?? [],
            previewColumnTypes: previewData?.columnTypes ?? [],
          }),
        });

        if (!response.ok) {
          finalizeDryRunError(`Dry run request failed: server error ${response.status}`);
          return;
        }

        const sseReader = response.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        let completed = false;

        while (true) {
          const { done, value } = await sseReader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const parts = buffer.split("\n\n");
          buffer = parts.pop() ?? "";

          for (const part of parts) {
            const lines = part.split("\n");
            let eventType = "";
            let eventData = "";
            for (const line of lines) {
              if (line.startsWith("event: ")) eventType = line.slice(7);
              else if (line.startsWith("data: ")) eventData = line.slice(6);
            }

            if (eventType === "status") {
              if (isViewingTable(rowKey, schema, tableName)) {
                updateDryRunStatus(eventData);
              } else {
                const cached = tableCacheRef.current.get(key);
                if (cached) {
                  tableCacheRef.current.set(key, {
                    ...cached,
                    dryRuns: cached.dryRuns.map((dr) => dr.label === newLabel ? { ...dr, status: eventData } : dr),
                  });
                }
              }
            } else if (eventType === "complete") {
              completed = true;
              let maskedFilenames = filenames;
              let completedFileFormatId = "";
              let completeSqlColumnTypes: string[] | undefined;
              try {
                const completeData = JSON.parse(eventData);
                if (Array.isArray(completeData.maskedFilenames) && completeData.maskedFilenames.length > 0) {
                  maskedFilenames = completeData.maskedFilenames;
                }
                if (typeof completeData.fileFormatId === "string") {
                  completedFileFormatId = completeData.fileFormatId;
                }
                if (Array.isArray(completeData.sqlColumnTypes) && completeData.sqlColumnTypes.length > 0) {
                  completeSqlColumnTypes = completeData.sqlColumnTypes;
                }
              } catch { /* use original filenames as fallback */ }
              const mergeRes = await fetch("/api/blob/preview-merge", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ filenames: maskedFilenames }),
              });
              if (mergeRes.ok) {
                const masked = await mergeRes.json();
                const maskedPreview = masked as PreviewData;
                if (completeSqlColumnTypes) {
                  maskedPreview.columnTypes = completeSqlColumnTypes;
                }

                const finishDryRun = (prev: DryRunResult[]) =>
                  prev.map((dr) =>
                    dr.label === newLabel
                      ? { label: newLabel, data: maskedPreview, inProgress: false }
                      : dr,
                  );

                const latestCached = tableCacheRef.current.get(key);
                if (latestCached) {
                  tableCacheRef.current.set(key, {
                    ...latestCached,
                    dryRuns: finishDryRun(latestCached.dryRuns),
                    activePreviewTab: newLabel,
                    originalData: latestCached.originalData ?? latestCached.previewData,
                    dryRunInProgress: false,
                  });
                }
                setDryRunningTables((prev) => { const next = new Set(prev); next.delete(key); return next; });

                if (isViewingTable(rowKey, schema, tableName)) {
                  setDryRuns(finishDryRun);
                  setActivePreviewTab(newLabel);
                  const finalCached = tableCacheRef.current.get(key);
                  if (finalCached?.originalData) {
                    setOriginalData(finalCached.originalData);
                  }
                }

                if (completedFileFormatId) {
                  const cachedPreview = tableCacheRef.current.get(key);
                  const origPreview = cachedPreview?.originalData ?? cachedPreview?.previewData;
                  fetchColumnRules(
                    agentPath, completedFileFormatId,
                    origPreview?.headers, origPreview?.columnTypes,
                  );
                }
              } else {
                const latestCached = tableCacheRef.current.get(key);
                if (latestCached) {
                  tableCacheRef.current.set(key, { ...latestCached, dryRunInProgress: false });
                }
                setDryRunningTables((prev) => { const next = new Set(prev); next.delete(key); return next; });
              }
            } else if (eventType === "error") {
              let errMsg = "Dry run failed.";
              try {
                const parsed = JSON.parse(eventData);
                errMsg = parsed.message ?? errMsg;
              } catch { /* use default */ }
              if (isViewingTable(rowKey, schema, tableName)) {
                finalizeDryRunError(errMsg);
              } else {
                const cached = tableCacheRef.current.get(key);
                if (cached) {
                  const wasActive = cached.activePreviewTab === newLabel;
                  tableCacheRef.current.set(key, {
                    ...cached,
                    dryRuns: cached.dryRuns.filter((dr) => dr.label !== newLabel),
                    activePreviewTab: wasActive ? "Original" : cached.activePreviewTab,
                    dryRunInProgress: false,
                    previewError: errMsg,
                  });
                }
                setDryRunningTables((prev) => { const next = new Set(prev); next.delete(key); return next; });
              }
            }
          }
        }

        if (!completed) {
          finalizeDryRunError("Dry run stream ended unexpectedly.");
        }
      } catch (e) {
        const errMsg = `Dry run failed: ${e instanceof Error ? e.message : String(e)}`;
        if (isViewingTable(rowKey, schema, tableName)) {
          finalizeDryRunError(errMsg);
        } else {
          const cached = tableCacheRef.current.get(key);
          if (cached) {
            const wasActive = cached.activePreviewTab === newLabel;
            tableCacheRef.current.set(key, {
              ...cached,
              dryRuns: cached.dryRuns.filter((dr) => dr.label !== newLabel),
              activePreviewTab: wasActive ? "Original" : cached.activePreviewTab,
              dryRunInProgress: false,
              previewError: errMsg,
            });
          }
          setDryRunningTables((prev) => { const next = new Set(prev); next.delete(key); return next; });
        }
      }
    } catch (e) {
      setPreviewError(`Dry run failed: ${e instanceof Error ? e.message : String(e)}`);
      setPreviewLoading(false);
    }
  }

  function handleFullRunOpen(rowKey: string, schema: string, tableName: string) {
    setFullRunTarget({ rowKey, schema, tableName });
  }

  async function handleFullRunExecute(destConnectionRowKey: string, destSchema: string) {
    const agentPath = getAgentPath();
    if (!agentPath || !fullRunTarget) return;

    const { rowKey, schema, tableName } = fullRunTarget;
    setFullRunTarget(null);
    setPreviewLoading(true);
    setPreviewError(null);

    try {
      const response = await fetch(`/api/agents/${agentPath}/full-run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          rowKey,
          schema,
          tableName,
          destConnectionRowKey,
          destSchema,
        }),
      });

      if (!response.ok) {
        setPreviewError(`Full run request failed: server error ${response.status}`);
        setPreviewLoading(false);
        fetchEvents();
        return;
      }

      const sseReader = response.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let completed = false;

      while (true) {
        const { done, value } = await sseReader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split("\n\n");
        buffer = parts.pop() ?? "";

        for (const part of parts) {
          const lines = part.split("\n");
          let eventType = "";
          let eventData = "";
          for (const line of lines) {
            if (line.startsWith("event: ")) eventType = line.slice(7);
            else if (line.startsWith("data: ")) eventData = line.slice(6);
          }

          if (eventType === "status") {
            setPreviewError(eventData);
          } else if (eventType === "complete") {
            completed = true;
            setPreviewError(null);
            setPreviewLoading(false);
          } else if (eventType === "error") {
            let errMsg = "Full run failed.";
            try {
              const parsed = JSON.parse(eventData);
              errMsg = parsed.message ?? errMsg;
            } catch { /* use default */ }
            setPreviewError(errMsg);
            setPreviewLoading(false);
          }
        }
      }

      if (!completed) {
        setPreviewError("Full run stream ended unexpectedly.");
        setPreviewLoading(false);
      }
    } catch (e) {
      setPreviewError(`Full run failed: ${e instanceof Error ? e.message : String(e)}`);
      setPreviewLoading(false);
    } finally {
      fetchEvents();
    }
  }

  async function handleAddToFlow(source: FlowSource, dest: FlowDest) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    try {
      const res = await fetch(`/api/agents/${agentPath}/save-flow`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sourceJson: JSON.stringify(source),
          destJson: JSON.stringify(dest),
        }),
      });

      if (res.ok) {
        const result = await res.json();
        if (result.success) {
          setFullRunTarget(null);
        }
      }
    } catch {
      // best-effort
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
        {leftPanel === "connections" && (
          <ConnectionsPanel
            connections={connections}
            connectionTables={connectionTables}
            connectionQueries={connectionQueries}
            loadingTables={loadingTables}
            dryRunningTables={dryRunningTables}
            selectedTable={selectedTable}
            selectedQuery={selectedQuery}
            onExpandConnection={handleExpandConnection}
            onTableClick={handleTableClick}
            onQueryClick={handleQueryClick}
            onReloadPreview={handleReloadPreview}
            onRefreshConnection={handleRefreshConnection}
            onDryRun={handleDryRun}
            onFullRun={handleFullRunOpen}
            onClose={() => setLeftPanel(null)}
            onSwitchPanel={setLeftPanel}
            onWidthChange={setConnectionsPanelWidth}
          />
        )}
        {leftPanel === "flows" && (
          <FlowsPanel
            agentPath={getAgentPath() ?? ""}
            onClose={() => setLeftPanel(null)}
            onSwitchPanel={setLeftPanel}
            onWidthChange={setConnectionsPanelWidth}
          />
        )}
        {(selectedTable || selectedQuery) && (
          <DataPreviewPanel
            loading={previewLoading}
            error={previewError}
            data={previewData}
            originalData={originalData}
            dryRuns={dryRuns}
            activeTab={activePreviewTab}
            diffTab={diffTab}
            columnRules={columnRules}
            columnRuleAlgorithms={columnRuleAlgorithms}
            columnRuleDomains={columnRuleDomains}
            columnRuleFrameworks={columnRuleFrameworks}
            columnRulesLoading={columnRulesLoading}
            allDomains={allDomains}
            allAlgorithms={allAlgorithms}
            allFrameworks={allFrameworks}
            onTabChange={setActivePreviewTab}
            onTabClose={(tab) => {
              if (diffTab && tab === diffTab.name) {
                setDiffTab(null);
                setActivePreviewTab("Original");
              } else if (tab !== "Original") {
                setDryRuns((prev) => prev.filter((dr) => dr.label !== tab));
                if (diffTab && (diffTab.leftTab === tab || diffTab.rightTab === tab)) {
                  setDiffTab(null);
                }
                if (activePreviewTab === tab) {
                  setActivePreviewTab("Original");
                }
              }
              if (selectedTable) {
                const key = tableKey(selectedTable.rowKey, selectedTable.schema, selectedTable.tableName);
                const cached = tableCacheRef.current.get(key);
                if (cached) {
                  const updatedDryRuns = cached.dryRuns.filter((dr) => dr.label !== tab);
                  const updatedDiffTab = (cached.diffTab && (tab === cached.diffTab.name || tab === cached.diffTab.leftTab || tab === cached.diffTab.rightTab))
                    ? null : cached.diffTab;
                  const updatedActiveTab = cached.activePreviewTab === tab ? "Original" : cached.activePreviewTab;
                  tableCacheRef.current.set(key, {
                    ...cached,
                    dryRuns: updatedDryRuns,
                    diffTab: updatedDiffTab,
                    activePreviewTab: updatedActiveTab,
                  });
                }
              }
            }}
            onDiffSelect={(leftTab, rightTab) => {
              const name = `${leftTab} vs ${rightTab}`;
              setDiffTab({ name, leftTab, rightTab });
              setActivePreviewTab(name);
            }}
            onSaveColumnRule={handleSaveColumnRule}
            mismatchedColumns={mismatchedColumns}
            onMismatchedColumnsChange={setMismatchedColumns}
            panelLeft={leftPanel ? connectionsPanelWidth + 16 : 0}
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
      {fullRunTarget && (
        <FullRunModal
          connections={connections}
          sourceConnectionRowKey={fullRunTarget.rowKey}
          schema={fullRunTarget.schema}
          tableName={fullRunTarget.tableName}
          agentPath={getAgentPath() ?? ""}
          onClose={() => setFullRunTarget(null)}
          onRun={handleFullRunExecute}
          onAddToFlow={handleAddToFlow}
        />
      )}
    </div>
  );
}
