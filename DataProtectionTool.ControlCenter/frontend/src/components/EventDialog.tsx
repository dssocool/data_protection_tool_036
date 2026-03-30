import { useEffect, useRef, useState } from "react";
import type { StatusEvent } from "./StatusBar";
import "./EventDialog.css";

interface EventDialogProps {
  events: StatusEvent[];
  onClose: () => void;
}

function getBadgeClass(event: StatusEvent): string {
  const s = event.summary.toLowerCase();
  if (s.includes("error") || s.includes("failed")) return "event-badge-error";
  if (s.includes("timeout")) return "event-badge-warn";
  if (s.includes("connected") || s.includes("disconnected")) return "event-badge-info";
  return "event-badge-success";
}

function formatBadgeLabel(type: string): string {
  return type.replace(/_/g, " ");
}

function formatTime(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch {
    return "";
  }
}

export default function EventDialog({ events, onClose }: EventDialogProps) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (dialogRef.current && !dialogRef.current.contains(e.target as Node)) {
        onClose();
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [onClose]);

  const reversed = [...events].reverse();

  return (
    <>
      <div className="event-dialog-overlay" />
      <div className="event-dialog" ref={dialogRef}>
        <div className="event-dialog-header">
          <span className="event-dialog-title">Event History</span>
          <button className="event-dialog-close" onClick={onClose} aria-label="Close">
            <svg width="14" height="14" viewBox="0 0 14 14">
              <path d="M3 3 L11 11 M11 3 L3 11" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
          </button>
        </div>
        <div className="event-dialog-body">
          {reversed.length === 0 ? (
            <div className="event-dialog-empty">No events recorded yet.</div>
          ) : (
            reversed.map((evt, idx) => {
              const originalIdx = events.length - 1 - idx;
              const isExpanded = expandedIdx === originalIdx;
              return (
                <div key={originalIdx}>
                  <div
                    className="event-item"
                    onClick={() => setExpandedIdx(isExpanded ? null : originalIdx)}
                  >
                    <span className="event-item-time">{formatTime(evt.timestamp)}</span>
                    <span className={`event-item-badge ${getBadgeClass(evt)}`}>
                      {formatBadgeLabel(evt.type)}
                    </span>
                    <span className="event-item-summary">{evt.summary}</span>
                  </div>
                  {isExpanded && evt.detail && (
                    <div className="event-item-detail">{evt.detail}</div>
                  )}
                </div>
              );
            })
          )}
        </div>
      </div>
    </>
  );
}
