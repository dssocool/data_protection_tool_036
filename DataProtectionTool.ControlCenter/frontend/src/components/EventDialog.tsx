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

function hasSteps(evt: StatusEvent): boolean {
  return Array.isArray(evt.steps) && evt.steps.length > 0;
}

function isInProgress(evt: StatusEvent): boolean {
  if (!evt.steps || evt.steps.length === 0) return false;
  return evt.steps[evt.steps.length - 1].status === "running";
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
              const stepsPresent = hasSteps(evt);
              const inProgress = isInProgress(evt);

              return (
                <div key={originalIdx}>
                  <div
                    className={`event-item${stepsPresent ? " event-item-expandable" : ""}`}
                    onClick={() => setExpandedIdx(isExpanded ? null : originalIdx)}
                  >
                    {stepsPresent && (
                      <span className={`event-item-chevron${isExpanded ? " event-item-chevron-open" : ""}`}>
                        <svg width="10" height="10" viewBox="0 0 10 10">
                          <path d="M3 2 L7 5 L3 8" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                        </svg>
                      </span>
                    )}
                    <span className="event-item-time">{formatTime(evt.timestamp)}</span>
                    <span className={`event-item-badge ${getBadgeClass(evt)}`}>
                      {formatBadgeLabel(evt.type)}
                    </span>
                    <span className="event-item-summary">
                      {evt.summary}
                      {inProgress && <span className="event-item-spinner" />}
                    </span>
                  </div>
                  {isExpanded && stepsPresent && (
                    <div className="event-steps">
                      {evt.steps!.map((step, si) => (
                        <div className="event-step" key={si}>
                          <span className={`event-step-icon event-step-icon-${step.status}`}>
                            {step.status === "done" && (
                              <svg width="10" height="10" viewBox="0 0 10 10">
                                <path d="M2 5 L4.5 7.5 L8 2.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                              </svg>
                            )}
                            {step.status === "running" && <span className="event-step-spinner" />}
                            {step.status === "error" && (
                              <svg width="10" height="10" viewBox="0 0 10 10">
                                <path d="M3 3 L7 7 M7 3 L3 7" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                              </svg>
                            )}
                          </span>
                          <span className="event-step-message">{step.message}</span>
                        </div>
                      ))}
                    </div>
                  )}
                  {isExpanded && !stepsPresent && evt.detail && (
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
