import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import MenuBar from "./components/MenuBar";
import SqlServerConnectionModal from "./components/SqlServerConnectionModal";
import type { SqlServerConnectionData, ValidateResult } from "./components/SqlServerConnectionModal";
import QueryModal from "./components/QueryModal";
import type { QuerySaveData, QueryValidateResult } from "./components/QueryModal";
import ConnectionsPanel from "./components/ConnectionsPanel";
import type { SavedConnection, TableInfo, QueryInfo } from "./components/ConnectionsPanel";
import DataPreviewPanel from "./components/DataPreviewPanel";
import type { PreviewData, DryRunResult, SampleResult } from "./components/DataPreviewPanel";
import StatusBar from "./components/StatusBar";
import type { StatusEvent } from "./components/StatusBar";
import EventDialog from "./components/EventDialog";
import FullRunModal from "./components/FullRunModal";
import type { FlowSource, FlowDest } from "./components/FullRunModal";
import FlowsPanel from "./components/FlowsPanel";
import type { FlowItem } from "./components/FlowsPanel";
import "./App.css";

interface TablePreviewCache {
  samples: SampleResult[];
  dryRuns: DryRunResult[];
  activePreviewTab: string;
  diffTab: { name: string; leftTab: string; rightTab: string } | null;
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

function safeJsonParse<T>(json: string): T | null {
  try { return JSON.parse(json); } catch { return null; }
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
  const [samples, setSamples] = useState<SampleResult[]>([]);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [dryRuns, setDryRuns] = useState<DryRunResult[]>([]);
  const [activePreviewTab, setActivePreviewTab] = useState("Sample 1");
  const [diffTab, setDiffTab] = useState<{ name: string; leftTab: string; rightTab: string } | null>(null);
  const [connectionsPanelWidth, setConnectionsPanelWidth] = useState(260);
  const [expandedConnections, setExpandedConnections] = useState<Set<string>>(new Set());
  const [statusEvents, setStatusEvents] = useState<StatusEvent[]>([]);
  const [showEventDialog, setShowEventDialog] = useState(false);
  const [agentOid, setAgentOid] = useState("");
  const [agentTid, setAgentTid] = useState("");
  const [agentUserName, setAgentUserName] = useState("");
  const [userUniqueId, setUserUniqueId] = useState<string | null>(null);
  const [fullRunTarget, setFullRunTarget] = useState<{ rowKey: string; schema: string; tableName: string } | null>(null);
  const [fullRunMinimizing, setFullRunMinimizing] = useState(false);
  const [unseenFlowCount, setUnseenFlowCount] = useState(0);
  const [columnRules, setColumnRules] = useState<Record<string, unknown>[]>([]);
  const [columnRuleAlgorithms, setColumnRuleAlgorithms] = useState<Record<string, unknown>[]>([]);
  const [columnRuleDomains, setColumnRuleDomains] = useState<Record<string, unknown>[]>([]);
  const [columnRuleFrameworks, setColumnRuleFrameworks] = useState<Record<string, unknown>[]>([]);
  const [columnRulesLoading, setColumnRulesLoading] = useState(false);
  const [allDomains, setAllDomains] = useState<Record<string, unknown>[]>([]);
  const [allAlgorithms, setAllAlgorithms] = useState<Record<string, unknown>[]>([]);
  const [allFrameworks, setAllFrameworks] = useState<Record<string, unknown>[]>([]);
  const [dryRunningTables, setDryRunningTables] = useState<Set<string>>(new Set());
  const [mismatchedColumns, setMismatchedColumns] = useState<Map<string, { maskType: string; sqlType: string }>>(new Map());
  const eventsTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const previewCacheRef = useRef<Map<string, string[]>>(new Map());
  const tableCacheRef = useRef<Map<string, TablePreviewCache>>(new Map());
  const selectedTableRef = useRef(selectedTable);
  selectedTableRef.current = selectedTable;
  const pendingSaveAndRunRef = useRef<{ destConnectionRowKey: string; destSchema: string; rowKey: string; schema: string; tableName: string; flowRowKey: string } | null>(null);

  const fetchEvents = useCallback(async () => {
    const agentPath = getAgentPath();
    if (!agentPath) return;
    try {
      const res = await fetch(`/api/agents/${agentPath}/events`);
      if (res.ok) {
        const data: StatusEvent[] = await res.json();
        setStatusEvents(prev => {
          const tracked = prev.filter(e => Array.isArray(e.steps) && e.steps.length > 0);
          if (tracked.length === 0) return data;

          const inProgress = tracked.filter(e => e.steps!.some(s => s.status === "running"));
          const completed = tracked.filter(e => !e.steps!.some(s => s.status === "running"));

          const merged = data.map(serverEvt => {
            const match = completed.find(
              t => t.type === serverEvt.type && t.summary === serverEvt.summary,
            );
            return match ?? serverEvt;
          });

          const unmatched = completed.filter(
            t => !data.some(s => s.type === t.type && s.summary === t.summary),
          );

          return [...merged, ...unmatched, ...inProgress];
        });
      }
    } catch {
      // silently ignore
    }
  }, []);

  const addLocalEvent = useCallback((evt: StatusEvent) => {
    setStatusEvents(prev => [...prev, evt]);
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
          setAgentUserName(data.userName ?? "");
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
    setUnseenFlowCount(0);
  }

  async function handleSave(data: SqlServerConnectionData) {
    const agentPath = getAgentPath();
    if (!agentPath) return;

    const res = await fetch(`/api/agents/${agentPath}/save-connection`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });

    if (res.ok) {
      const result = await res.json();
      if (result.event) addLocalEvent(result.event);
    }

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
        if (result.event) addLocalEvent(result.event);
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
        if (result.event) addLocalEvent(result.event);
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
    const mergeRes = await fetch("/api/blob/preview-merge", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filenames }),
    });
    if (!mergeRes.ok) {
      setPreviewError(`Failed to fetch preview data: ${mergeRes.status}`);
      return null;
    }

    return (await mergeRes.json()) as PreviewData;
  }

  function saveCurrentTableToCache() {
    if (!selectedTable) return;
    const key = tableKey(selectedTable.rowKey, selectedTable.schema, selectedTable.tableName);
    const existing = tableCacheRef.current.get(key);
    tableCacheRef.current.set(key, {
      samples,
      dryRuns,
      activePreviewTab,
      diffTab,
      previewError,
      dryRunInProgress: existing?.dryRunInProgress ?? false,
      columnRules,
      columnRuleAlgorithms,
      columnRuleDomains,
      columnRuleFrameworks,
    });
  }

  function restoreTableFromCache(cached: TablePreviewCache) {
    setSamples(cached.samples);
    setDryRuns(cached.dryRuns);
    setActivePreviewTab(cached.activePreviewTab);
    setDiffTab(cached.diffTab);
    setPreviewError(cached.previewError);
    setPreviewLoading(cached.dryRunInProgress);
    setColumnRules(cached.columnRules);
    setColumnRuleAlgorithms(cached.columnRuleAlgorithms);
    setColumnRuleDomains(cached.columnRuleDomains);
    setColumnRuleFrameworks(cached.columnRuleFrameworks);
  }

  function getAllowedAlgorithmTypes(sqlType: string): string[] {
    const numericTypes = new Set([
      "int", "bigint", "smallint", "tinyint", "float", "real",
      "decimal", "numeric", "money", "smallmoney", "bit",
    ]);
    if (numericTypes.has(sqlType.toLowerCase())) return ["BIG_DECIMAL"];
    return ["BIG_DECIMAL", "LOCAL_DATE_TIME", "STRING", "BYTE_BUFFER", "GENERIC_DATA_ROW"];
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
          const rules = parseArray(result.responseList);
          const algorithms = parseArray(result.algorithms);
          setColumnRules(rules);
          setColumnRuleAlgorithms(algorithms);
          setColumnRuleDomains(parseArray(result.domains));
          setColumnRuleFrameworks(parseArray(result.frameworks));

          if (previewHeaders?.length && previewColumnTypes?.length) {
            const algMaskTypes = new Map<string, string>();
            for (const alg of algorithms) {
              const name = typeof alg.algorithmName === "string" ? alg.algorithmName : "";
              const mt = typeof alg.maskType === "string" ? alg.maskType : String(alg.maskType ?? "");
              if (name) algMaskTypes.set(name, mt);
            }
            const detected = new Map<string, { maskType: string; sqlType: string }>();
            for (const rule of rules) {
              const fieldName = typeof rule.fieldName === "string" ? rule.fieldName : "";
              const algName = typeof rule.algorithmName === "string" ? rule.algorithmName : "";
              const isMasked = rule.isMasked !== false;
              if (!fieldName || !algName || !isMasked) continue;
              const idx = previewHeaders.indexOf(fieldName);
              if (idx < 0 || idx >= previewColumnTypes.length) continue;
              const sqlType = previewColumnTypes[idx];
              const maskType = algMaskTypes.get(algName);
              if (!maskType) continue;
              const allowed = getAllowedAlgorithmTypes(sqlType);
              if (!allowed.includes(maskType)) {
                detected.set(fieldName, { maskType, sqlType });
              }
            }
            setMismatchedColumns(prev => {
              const merged = new Map(prev);
              for (const key of [...merged.keys()]) {
                if (!detected.has(key)) merged.delete(key);
              }
              for (const [key, val] of detected) {
                merged.set(key, val);
              }
              return merged;
            });
          }

          return rules;
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
        const origPreview = samples[0]?.data ?? null;
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
    setSamples([]);
    setDryRuns([]);
    setActivePreviewTab("Sample 1");
    setDiffTab(null);
    setColumnRules([]);
    setColumnRuleAlgorithms([]);
    setColumnRuleDomains([]);
    setColumnRuleFrameworks([]);
    setMismatchedColumns(new Map());

    const tableInfo = connectionTables[rowKey]?.find(
      (t) => t.schema === schema && t.name === tableName
    );
    if (tableInfo?.fileFormatId) {
      fetchColumnRules(agentPath, tableInfo.fileFormatId);
    }

    try {
      const res = await fetch(`/api/agents/${agentPath}/sample-table`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rowKey, schema, tableName }),
      });

      if (!res.ok) {
        setPreviewError(`Server error: ${res.status}`);
        return;
      }

      const result = await res.json();
      if (result.event) addLocalEvent(result.event);
      if (!result.success) {
        setPreviewError(result.message ?? "Preview failed.");
        return;
      }

      const filenames: string[] = result.filenames ?? (result.filename ? [result.filename] : []);
      const preview = await fetchPreviewFromFilenames(filenames);

      if (preview) {
        const newSample: SampleResult = { label: "Sample 1", data: preview, blobFilenames: filenames };
        setSamples([newSample]);
      }

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
    if (result.event) addLocalEvent(result.event);
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
    if (result.event) addLocalEvent(result.event);
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
      if (result.event) addLocalEvent(result.event);
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
    setSamples([]);
    setDryRuns([]);
    setActivePreviewTab("Sample 1");
    setDiffTab(null);
    setColumnRules([]);
    setColumnRuleAlgorithms([]);
    setColumnRuleDomains([]);
    setColumnRuleFrameworks([]);

    const cacheKey = `query:${connectionRowKey}:${queryRowKey}`;
    const cached = previewCacheRef.current.get(cacheKey);

    try {
      let filenames: string[];
      if (cached) {
        filenames = cached;
      } else {
        const res = await fetch(`/api/agents/${agentPath}/sample-query`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ connectionRowKey, queryText }),
        });

        if (!res.ok) {
          setPreviewError(`Server error: ${res.status}`);
          return;
        }

        const result = await res.json();
        if (result.event) addLocalEvent(result.event);
        if (!result.success) {
          setPreviewError(result.message ?? "Preview failed.");
          return;
        }

        filenames = result.filenames ?? (result.filename ? [result.filename] : []);
        previewCacheRef.current.set(cacheKey, filenames);
      }
      const preview = await fetchPreviewFromFilenames(filenames);
      if (preview) {
        setSamples([{ label: "Sample 1", data: preview, blobFilenames: filenames }]);
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
      setPreviewLoading(true);
      setPreviewError(null);

      try {
        const res = await fetch(`/api/agents/${agentPath}/sample-table`, {
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
        if (result.event) addLocalEvent(result.event);
        if (!result.success) {
          setPreviewError(result.message ?? "Sample data failed.");
          return;
        }

        const filenames: string[] = result.filenames ?? (result.filename ? [result.filename] : []);
        const preview = await fetchPreviewFromFilenames(filenames);

        if (preview) {
          const newLabel = `Sample ${samples.length + 1}`;
          const newSample: SampleResult = { label: newLabel, data: preview, blobFilenames: filenames };
          setSamples((prev) => [...prev, newSample]);
          setActivePreviewTab(newLabel);
        }
      } catch (e) {
        setPreviewError(`Sample data failed: ${e instanceof Error ? e.message : String(e)}`);
      } finally {
        setPreviewLoading(false);
      }
    } else if (selectedQuery) {
      const cacheKey = `query:${selectedQuery.connectionRowKey}:${selectedQuery.queryRowKey}`;
      previewCacheRef.current.delete(cacheKey);

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

    let currentSamples = isSameTable ? samples : [];
    const cachedEntry = tableCacheRef.current.get(key);
    if (!isSameTable && cachedEntry && cachedEntry.samples.length > 0) {
      currentSamples = cachedEntry.samples;
      restoreTableFromCache(cachedEntry);
      setPreviewLoading(true);
    }

    let filenames = currentSamples.length > 0 ? currentSamples[0].blobFilenames : [];

    try {
      if (filenames.length === 0) {
        setSamples([]);
        setDryRuns([]);
        setActivePreviewTab("Sample 1");
        setDiffTab(null);

        const previewRes = await fetch(`/api/agents/${agentPath}/sample-table`, {
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
        if (previewResult.event) addLocalEvent(previewResult.event);
        if (!previewResult.success) {
          setPreviewError(previewResult.message ?? "Preview failed.");
          setPreviewLoading(false);
          return;
        }

        filenames = previewResult.filenames ?? (previewResult.filename ? [previewResult.filename] : []);
        const preview = await fetchPreviewFromFilenames(filenames);
        if (preview) {
          const newSample: SampleResult = { label: "Sample 1", data: preview, blobFilenames: filenames };
          currentSamples = [newSample];
          setSamples([newSample]);
        }
      }

      const sampleData = currentSamples[0]?.data ?? null;

      const currentCached = tableCacheRef.current.get(key);
      const prevDryRuns = currentCached?.dryRuns ?? dryRuns;
      const newLabel = `DP Preview ${prevDryRuns.length + 1}`;
      const pendingDryRun: DryRunResult = { label: newLabel, data: null, status: "Starting DP preview...", inProgress: true };
      const updatedDryRunsWithPending = [...prevDryRuns, pendingDryRun];

      setDryRuns(updatedDryRunsWithPending);
      setActivePreviewTab(newLabel);
      setPreviewLoading(false);

      tableCacheRef.current.set(key, {
        ...(currentCached ?? {
          samples: currentSamples, dryRuns: prevDryRuns, activePreviewTab,
          diffTab, previewError: null,
          columnRules, columnRuleAlgorithms, columnRuleDomains, columnRuleFrameworks,
        }),
        samples: currentSamples,
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
        const fallbackTab = currentSamples[0]?.label ?? "Sample 1";
        setActivePreviewTab(fallbackTab);
        setPreviewError(errMsg);
        const cached = tableCacheRef.current.get(key);
        if (cached) {
          tableCacheRef.current.set(key, {
            ...cached,
            dryRuns: cached.dryRuns.filter((dr) => dr.label !== newLabel),
            activePreviewTab: fallbackTab,
            dryRunInProgress: false,
            previewError: errMsg,
          });
        }
        setDryRunningTables((prev) => { const next = new Set(prev); next.delete(key); return next; });
      };

      const dryRunTrackedTs = new Date().toISOString();
      const dryRunTrackedEvent: StatusEvent = {
        timestamp: dryRunTrackedTs,
        type: "dp_preview",
        summary: `DP preview started: ${schema}.${tableName}`,
        detail: "",
        steps: [],
      };
      addLocalEvent(dryRunTrackedEvent);

      const updateDryRunTrackedSteps = (stepMsg: string, stepStatus: "running" | "done" | "error") => {
        setStatusEvents(prev => prev.map(evt => {
          if (evt.timestamp !== dryRunTrackedTs || evt.type !== "dp_preview" || !evt.steps) return evt;
          const steps = evt.steps.map(s => {
            if (s.status !== "running") return s;
            const closedStatus = s.message.includes("(skipped") ? "skipped" as const : "done" as const;
            return { ...s, status: closedStatus };
          });
          if (stepStatus !== "done" || stepMsg) {
            steps.push({ timestamp: new Date().toISOString(), message: stepMsg, status: stepStatus });
          }
          return { ...evt, steps };
        }));
      };

      const finalizeDryRunTrackedEvent = (summary: string, lastStepStatus: "done" | "error") => {
        setStatusEvents(prev => prev.map(evt => {
          if (evt.timestamp !== dryRunTrackedTs || evt.type !== "dp_preview" || !evt.steps) return evt;
          const steps = evt.steps.map(s => {
            if (s.status !== "running") return s;
            if (s.message.includes("(skipped")) return { ...s, status: "skipped" as const };
            return { ...s, status: lastStepStatus };
          });
          return { ...evt, summary, steps };
        }));
      };

      try {
        const response = await fetch(`/api/agents/${agentPath}/dp-preview`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            rowKey, schema, tableName, previewBlobFilenames: filenames,
            previewHeaders: sampleData?.headers ?? [],
            previewColumnTypes: sampleData?.columnTypes ?? [],
          }),
        });

        if (!response.ok) {
          finalizeDryRunTrackedEvent(`DP preview failed: server error ${response.status}`, "error");
          finalizeDryRunError(`DP preview request failed: server error ${response.status}`);
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

            if (eventType === "event") {
              try {
                const parsed = JSON.parse(eventData);
                finalizeDryRunTrackedEvent(parsed.summary ?? `DP preview completed: ${schema}.${tableName}`, "done");
              } catch { /* ignore parse errors */ }
            } else if (eventType === "status") {
              updateDryRunTrackedSteps(eventData, "running");
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
                    dryRunInProgress: false,
                  });
                }
                setDryRunningTables((prev) => { const next = new Set(prev); next.delete(key); return next; });

                if (isViewingTable(rowKey, schema, tableName)) {
                  setDryRuns(finishDryRun);
                  setActivePreviewTab(newLabel);
                }

                if (completedFileFormatId) {
                  const cachedPreview = tableCacheRef.current.get(key);
                  const origPreview = cachedPreview?.samples[0]?.data ?? null;
                  fetchColumnRules(
                    agentPath, completedFileFormatId,
                    origPreview?.headers, origPreview?.columnTypes,
                  );
                  setConnectionTables((prev) => {
                    const tables = prev[rowKey];
                    if (!tables) return prev;
                    return {
                      ...prev,
                      [rowKey]: tables.map((t) =>
                        t.schema === schema && t.name === tableName
                          ? { ...t, fileFormatId: completedFileFormatId }
                          : t,
                      ),
                    };
                  });
                }
              } else {
                const latestCached = tableCacheRef.current.get(key);
                if (latestCached) {
                  tableCacheRef.current.set(key, { ...latestCached, dryRunInProgress: false });
                }
                setDryRunningTables((prev) => { const next = new Set(prev); next.delete(key); return next; });
              }
            } else if (eventType === "error") {
              let errMsg = "DP preview failed.";
              try {
                const parsed = JSON.parse(eventData);
                errMsg = parsed.message ?? errMsg;
              } catch { /* use default */ }
              finalizeDryRunTrackedEvent(errMsg, "error");
              if (isViewingTable(rowKey, schema, tableName)) {
                finalizeDryRunError(errMsg);
              } else {
                const cached = tableCacheRef.current.get(key);
                if (cached) {
                  const wasActive = cached.activePreviewTab === newLabel;
                  const cachedFallback = cached.samples[0]?.label ?? "Sample 1";
                  tableCacheRef.current.set(key, {
                    ...cached,
                    dryRuns: cached.dryRuns.filter((dr) => dr.label !== newLabel),
                    activePreviewTab: wasActive ? cachedFallback : cached.activePreviewTab,
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
          finalizeDryRunTrackedEvent("DP preview stream ended unexpectedly.", "error");
          finalizeDryRunError("DP preview stream ended unexpectedly.");
        }
      } catch (e) {
        const errMsg = `DP preview failed: ${e instanceof Error ? e.message : String(e)}`;
        finalizeDryRunTrackedEvent(errMsg, "error");
        if (isViewingTable(rowKey, schema, tableName)) {
          finalizeDryRunError(errMsg);
        } else {
          const cached = tableCacheRef.current.get(key);
          if (cached) {
            const wasActive = cached.activePreviewTab === newLabel;
            const cachedFallback = cached.samples[0]?.label ?? "Sample 1";
            tableCacheRef.current.set(key, {
              ...cached,
              dryRuns: cached.dryRuns.filter((dr) => dr.label !== newLabel),
              activePreviewTab: wasActive ? cachedFallback : cached.activePreviewTab,
              dryRunInProgress: false,
              previewError: errMsg,
            });
          }
          setDryRunningTables((prev) => { const next = new Set(prev); next.delete(key); return next; });
        }
      }
    } catch (e) {
      setPreviewError(`DP preview failed: ${e instanceof Error ? e.message : String(e)}`);
      setPreviewLoading(false);
    }
  }

  function handleFullRunOpen(rowKey: string, schema: string, tableName: string) {
    setFullRunTarget({ rowKey, schema, tableName });
  }

  async function handleFullRunExecute(
    destConnectionRowKey: string,
    destSchema: string,
    sourceRowKey?: string,
    sourceSchema?: string,
    sourceTableName?: string,
    flowRowKey?: string,
  ) {
    const agentPath = getAgentPath();
    const target = sourceRowKey && sourceSchema && sourceTableName
      ? { rowKey: sourceRowKey, schema: sourceSchema, tableName: sourceTableName }
      : fullRunTarget;
    if (!agentPath || !target) return;

    const { rowKey, schema, tableName } = target;
    setFullRunTarget(null);

    const key = tableKey(rowKey, schema, tableName);
    setDryRunningTables((prev) => new Set(prev).add(key));

    const trackedEvent: StatusEvent = {
      timestamp: new Date().toISOString(),
      type: "dp_run",
      summary: `DP run started: ${schema}.${tableName}`,
      detail: "",
      steps: [],
    };
    addLocalEvent(trackedEvent);

    const updateTrackedSteps = (stepMsg: string, stepStatus: "running" | "done" | "error") => {
      setStatusEvents(prev => prev.map(evt => {
        if (evt.timestamp !== trackedEvent.timestamp || evt.type !== "dp_run" || !evt.steps) return evt;
        const steps = evt.steps.map(s => s.status === "running" ? { ...s, status: "done" as const } : s);
        if (stepStatus !== "done" || stepMsg) {
          steps.push({ timestamp: new Date().toISOString(), message: stepMsg, status: stepStatus });
        }
        return { ...evt, steps };
      }));
    };

    const finalizeTrackedEvent = (summary: string, lastStepStatus: "done" | "error") => {
      setStatusEvents(prev => prev.map(evt => {
        if (evt.timestamp !== trackedEvent.timestamp || evt.type !== "dp_run" || !evt.steps) return evt;
        const steps = evt.steps.map(s => s.status === "running" ? { ...s, status: lastStepStatus } : s);
        return { ...evt, summary, steps };
      }));
    };

    try {
      const response = await fetch(`/api/agents/${agentPath}/dp-run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          rowKey,
          schema,
          tableName,
          destConnectionRowKey,
          destSchema,
          flowRowKey: flowRowKey ?? "",
        }),
      });

      if (!response.ok) {
        finalizeTrackedEvent(`DP run failed: server error ${response.status}`, "error");
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

          if (eventType === "event") {
            try {
              const parsed = JSON.parse(eventData);
              finalizeTrackedEvent(parsed.summary ?? `DP run completed: ${schema}.${tableName}`, "done");
            } catch { /* ignore parse errors */ }
          } else if (eventType === "status") {
            updateTrackedSteps(eventData, "running");
          } else if (eventType === "complete") {
            completed = true;
          } else if (eventType === "error") {
            let errMsg = "DP run failed.";
            try {
              const parsed = JSON.parse(eventData);
              errMsg = parsed.message ?? errMsg;
            } catch { /* use default */ }
            finalizeTrackedEvent(errMsg, "error");
          }
        }
      }

      if (!completed) {
        finalizeTrackedEvent("DP run stream ended unexpectedly.", "error");
      }
    } catch (e) {
      finalizeTrackedEvent(`DP run failed: ${e instanceof Error ? e.message : String(e)}`, "error");
    } finally {
      setDryRunningTables((prev) => {
        const next = new Set(prev);
        next.delete(key);
        return next;
      });
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
          setFullRunMinimizing(true);
        }
      }
    } catch {
      // best-effort
    }
  }

  async function handleSaveAndRun(
    source: FlowSource,
    dest: FlowDest,
    destConnectionRowKey: string,
    destSchema: string,
  ) {
    const agentPath = getAgentPath();
    if (!agentPath || !fullRunTarget) return;

    const { rowKey, schema, tableName } = fullRunTarget;

    let flowRowKey = "";
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
          setUnseenFlowCount((c) => c + 1);
          flowRowKey = result.rowKey ?? "";
        }
      }
    } catch { /* best-effort */ }

    pendingSaveAndRunRef.current = { destConnectionRowKey, destSchema, rowKey, schema, tableName, flowRowKey };
    setFullRunMinimizing(true);
  }

  function handleMinimizeEnd() {
    setFullRunMinimizing(false);
    setFullRunTarget(null);

    const pending = pendingSaveAndRunRef.current;
    if (pending) {
      pendingSaveAndRunRef.current = null;
      handleFullRunExecute(pending.destConnectionRowKey, pending.destSchema, pending.rowKey, pending.schema, pending.tableName, pending.flowRowKey);
    } else {
      setUnseenFlowCount((c) => c + 1);
    }
  }

  function handleRunFlows(flowItems: FlowItem[]) {
    for (const flow of flowItems) {
      const src = safeJsonParse<FlowSource>(flow.sourceJson);
      const dest = safeJsonParse<FlowDest>(flow.destJson);
      if (!src || !dest) continue;
      handleFullRunExecute(
        dest.connectionRowKey,
        dest.schema,
        src.connectionRowKey,
        src.schema,
        src.tableName,
        flow.rowKey,
      );
    }
  }

  const tableTabCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const [key, cached] of tableCacheRef.current.entries()) {
      const count = cached.samples.length + cached.dryRuns.length + (cached.diffTab ? 1 : 0);
      if (count >= 1) counts[key] = count;
    }
    if (selectedTable) {
      const key = tableKey(selectedTable.rowKey, selectedTable.schema, selectedTable.tableName);
      const count = samples.length + dryRuns.length + (diffTab ? 1 : 0);
      if (count >= 1) counts[key] = count;
      else delete counts[key];
    }
    return counts;
  }, [samples, dryRuns, diffTab, selectedTable]);

  return (
    <div className="app">
      <MenuBar
        onSqlServerConnection={handleSqlServerConnection}
        onNewQuery={handleNewQuery}
        onViewConnections={handleViewConnections}
        onViewFlows={handleViewFlows}
        oid={agentOid}
        tid={agentTid}
        userName={agentUserName}
        uniqueId={userUniqueId}
      />
      <main className="app-content">
        {leftPanel === "flows" ? (
          <FlowsPanel
            agentPath={getAgentPath() ?? ""}
            onSwitchPanel={setLeftPanel}
            onRunFlows={handleRunFlows}
          />
        ) : (
          <>
            {leftPanel === "connections" && (
              <ConnectionsPanel
                connections={connections}
                connectionTables={connectionTables}
                connectionQueries={connectionQueries}
                loadingTables={loadingTables}
                dryRunningTables={dryRunningTables}
                selectedTable={selectedTable}
                selectedQuery={selectedQuery}
                tableTabCounts={tableTabCounts}
                expanded={expandedConnections}
                onExpandedChange={setExpandedConnections}
                width={connectionsPanelWidth}
                flowsBadgeCount={unseenFlowCount}
                onExpandConnection={handleExpandConnection}
                onTableClick={handleTableClick}
                onQueryClick={handleQueryClick}
                onReloadPreview={handleReloadPreview}
                onRefreshConnection={handleRefreshConnection}
                onDryRun={handleDryRun}
                onFullRun={handleFullRunOpen}
                onSwitchPanel={(p) => { setLeftPanel(p); if (p === "flows") setUnseenFlowCount(0); }}
                onWidthChange={setConnectionsPanelWidth}
              />
            )}
            {(selectedTable || selectedQuery) && (
              <DataPreviewPanel
                loading={previewLoading}
                error={previewError}
                samples={samples}
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
                  const isSampleTab = samples.some((s) => s.label === tab);
                  const isDryRunTab = dryRuns.some((dr) => dr.label === tab);
                  const isDiffCloseTab = diffTab && tab === diffTab.name;

                  if (isDiffCloseTab) {
                    setDiffTab(null);
                    const fallback = samples[0]?.label ?? dryRuns[0]?.label ?? "Sample 1";
                    setActivePreviewTab(fallback);
                  } else if (isDryRunTab) {
                    setDryRuns((prev) => prev.filter((dr) => dr.label !== tab));
                    if (diffTab && (diffTab.leftTab === tab || diffTab.rightTab === tab)) {
                      setDiffTab(null);
                    }
                    if (activePreviewTab === tab) {
                      const fallback = samples[0]?.label ?? "Sample 1";
                      setActivePreviewTab(fallback);
                    }
                  } else if (isSampleTab) {
                    const remaining = samples.filter((s) => s.label !== tab);
                    setSamples(remaining);
                    if (diffTab && (diffTab.leftTab === tab || diffTab.rightTab === tab)) {
                      setDiffTab(null);
                    }
                    if (activePreviewTab === tab) {
                      const fallback = remaining[0]?.label ?? dryRuns[0]?.label ?? null;
                      if (fallback) {
                        setActivePreviewTab(fallback);
                      } else {
                        setSelectedTable(null);
                        setSelectedQuery(null);
                        setSamples([]);
                        setPreviewError(null);
                        setDiffTab(null);
                        setActivePreviewTab("Sample 1");
                      }
                    }
                    if (remaining.length === 0 && dryRuns.length === 0) {
                      setSelectedTable(null);
                      setSelectedQuery(null);
                      setPreviewError(null);
                      setDiffTab(null);
                      setActivePreviewTab("Sample 1");
                    }
                  }
                  if (selectedTable) {
                    const cacheKey = tableKey(selectedTable.rowKey, selectedTable.schema, selectedTable.tableName);
                    const cached = tableCacheRef.current.get(cacheKey);
                    if (cached) {
                      const updatedSamples = cached.samples.filter((s) => s.label !== tab);
                      const updatedDryRuns = cached.dryRuns.filter((dr) => dr.label !== tab);
                      const updatedDiffTab = (cached.diffTab && (tab === cached.diffTab.name || tab === cached.diffTab.leftTab || tab === cached.diffTab.rightTab))
                        ? null : cached.diffTab;
                      const fallbackTab = updatedSamples[0]?.label ?? updatedDryRuns[0]?.label ?? "Sample 1";
                      const updatedActiveTab = cached.activePreviewTab === tab ? fallbackTab : cached.activePreviewTab;
                      if (updatedSamples.length === 0 && updatedDryRuns.length === 0) {
                        tableCacheRef.current.delete(cacheKey);
                      } else {
                        tableCacheRef.current.set(cacheKey, {
                          ...cached,
                          samples: updatedSamples,
                          dryRuns: updatedDryRuns,
                          diffTab: updatedDiffTab,
                          activePreviewTab: updatedActiveTab,
                        });
                      }
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
          </>
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
          minimizing={fullRunMinimizing}
          onClose={() => setFullRunTarget(null)}
          onSaveAndRun={handleSaveAndRun}
          onAddToFlow={handleAddToFlow}
          onMinimizeEnd={handleMinimizeEnd}
        />
      )}
    </div>
  );
}
