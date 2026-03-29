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
        />
      )}
    </div>
  );
}
