import { useState } from "react";
import MenuBar from "./components/MenuBar";
import SqlServerConnectionModal from "./components/SqlServerConnectionModal";
import type { SqlServerConnectionData } from "./components/SqlServerConnectionModal";
import "./App.css";

export default function App() {
  const [showSqlModal, setShowSqlModal] = useState(false);

  function handleSqlServerConnection() {
    setShowSqlModal(true);
  }

  function handleViewConnections() {
    console.log("View -> Connections");
  }

  function handleViewFlows() {
    console.log("View -> Flows");
  }

  function handleSave(data: SqlServerConnectionData) {
    console.log("Save connection:", data);
    setShowSqlModal(false);
  }

  async function handleValidate(data: SqlServerConnectionData): Promise<string> {
    const segments = window.location.pathname.split("/");
    const agentsIdx = segments.indexOf("agents");
    if (agentsIdx === -1 || agentsIdx + 1 >= segments.length) {
      return "No agent path found in URL. Open this page via an agent URL.";
    }
    const agentPath = segments[agentsIdx + 1];

    const res = await fetch(`/api/agents/${agentPath}/validate-sql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });

    if (!res.ok) {
      const text = await res.text();
      return `Error: ${text}`;
    }

    const result = await res.json();
    return result.message ?? result.status ?? "Unknown result";
  }

  return (
    <div className="app">
      <MenuBar
        onSqlServerConnection={handleSqlServerConnection}
        onViewConnections={handleViewConnections}
        onViewFlows={handleViewFlows}
      />
      <main className="app-content">
        <h1>DataProtectionTool Control Center</h1>
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
