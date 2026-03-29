import "./ConnectionsPanel.css";

export interface SavedConnection {
  rowKey: string;
  connectionType: string;
  serverName: string;
  authentication: string;
  databaseName: string;
  encrypt: string;
  trustServerCertificate: boolean;
  createdAt: string;
}

interface ConnectionsPanelProps {
  connections: SavedConnection[];
}

export default function ConnectionsPanel({ connections }: ConnectionsPanelProps) {
  return (
    <div className="connections-panel">
      <h3 className="connections-panel-title">Connections</h3>
      {connections.length === 0 ? (
        <p className="connections-panel-empty">No saved connections.</p>
      ) : (
        <ul className="connections-list">
          {connections.map((conn) => (
            <li key={conn.rowKey} className="connections-list-item">
              <span className="conn-type-badge">{conn.connectionType}</span>
              <div className="conn-details">
                <span className="conn-server">{conn.serverName}</span>
                {conn.databaseName && (
                  <span className="conn-db">{conn.databaseName}</span>
                )}
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
