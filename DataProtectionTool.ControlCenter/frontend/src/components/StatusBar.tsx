import "./StatusBar.css";

export interface StatusEvent {
  timestamp: string;
  type: string;
  summary: string;
  detail: string;
}

interface StatusBarProps {
  events: StatusEvent[];
  onIconClick: () => void;
}

function getStatusColor(events: StatusEvent[]): string {
  if (events.length === 0) return "#888";
  const last = events[events.length - 1];
  if (last.summary.toLowerCase().includes("error") || last.summary.toLowerCase().includes("failed"))
    return "#d32f2f";
  if (last.summary.toLowerCase().includes("timeout"))
    return "#ed6c02";
  return "#2e7d32";
}

export default function StatusBar({ events, onIconClick }: StatusBarProps) {
  const latest = events.length > 0 ? events[events.length - 1] : null;
  const color = getStatusColor(events);

  return (
    <div className="status-bar">
      <div className="status-bar-icon" onClick={onIconClick} title="Show event history">
        <svg width="14" height="14" viewBox="0 0 14 14">
          <circle cx="7" cy="7" r="5" fill={color} opacity="0.9" />
          <circle cx="7" cy="7" r="2.5" fill="#fff" opacity="0.5" />
        </svg>
      </div>
      <span className="status-bar-summary">
        {latest ? latest.summary : "No recent activity"}
      </span>
    </div>
  );
}
