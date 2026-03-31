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

function isInProgress(evt: StatusEvent): boolean {
  if (!evt.steps || evt.steps.length === 0) return false;
  return evt.steps[evt.steps.length - 1].status === "running";
}

function getLatestStatus(evt: StatusEvent): "running" | "error" | "warn" | "info" | "success" {
  if (isInProgress(evt)) return "running";
  const s = evt.summary.toLowerCase();
  if (s.includes("error") || s.includes("failed")) return "error";
  if (s.includes("timeout")) return "warn";
  if (s.includes("connected") || s.includes("disconnected")) return "info";
  return "success";
}

function StatusIcon({ status }: { status: "running" | "error" | "warn" | "info" | "success" }) {
  if (status === "running") {
    return <span className="status-bar-spinner" />;
  }
  if (status === "error") {
    return (
      <svg className="status-bar-icon status-bar-icon-error" width="12" height="12" viewBox="0 0 10 10">
        <path d="M3 3 L7 7 M7 3 L3 7" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      </svg>
    );
  }
  if (status === "warn") {
    return (
      <svg className="status-bar-icon status-bar-icon-warn" width="12" height="12" viewBox="0 0 10 10">
        <path d="M5 2 L5 6" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
        <circle cx="5" cy="8" r="0.8" fill="currentColor" />
      </svg>
    );
  }
  if (status === "info") {
    return (
      <svg className="status-bar-icon status-bar-icon-info" width="12" height="12" viewBox="0 0 10 10">
        <circle cx="5" cy="5" r="4" fill="none" stroke="currentColor" strokeWidth="1.2" />
        <circle cx="5" cy="3.2" r="0.8" fill="currentColor" />
        <path d="M5 5 L5 7.5" fill="none" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
      </svg>
    );
  }
  return (
    <svg className="status-bar-icon status-bar-icon-success" width="12" height="12" viewBox="0 0 10 10">
      <path d="M2 5 L4.5 7.5 L8 2.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

export default function StatusBar({ events, onIconClick }: StatusBarProps) {
  const latest = events.length > 0 ? events[events.length - 1] : null;
  const status = latest ? getLatestStatus(latest) : null;

  return (
    <div className="status-bar">
      {status && <StatusIcon status={status} />}
      <span className="status-bar-summary" onClick={onIconClick} title="Show event history">
        {latest ? latest.summary : "No recent activity"}
      </span>
    </div>
  );
}
