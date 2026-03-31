import "./StatusBar.css";

export interface StatusEventStep {
  timestamp: string;
  message: string;
  status: "running" | "done" | "error" | "skipped";
}

export interface StatusEvent {
  timestamp: string;
  type: string;
  summary: string;
  detail: string;
  steps?: StatusEventStep[];
}

interface StatusBarProps {
  events: StatusEvent[];
  onIconClick: () => void;
}

export default function StatusBar({ events, onIconClick }: StatusBarProps) {
  const latest = events.length > 0 ? events[events.length - 1] : null;

  return (
    <div className="status-bar">
      <span className="status-bar-summary" onClick={onIconClick} title="Show event history">
        {latest ? latest.summary : "No recent activity"}
      </span>
    </div>
  );
}
