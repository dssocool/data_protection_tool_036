import { useEffect, useRef, useState } from "react";
import type { StatusEvent, StatusEventStep } from "./StatusBar";
import "./EventDialog.css";

interface EventDialogProps {
  events: StatusEvent[];
  onClose: () => void;
}

interface ConsolidatedStep extends StatusEventStep {
  pollCount?: number;
}

function consolidateSteps(steps: StatusEventStep[]): ConsolidatedStep[] {
  const result: ConsolidatedStep[] = [];

  for (const step of steps) {
    if (!step.message.startsWith("Polling ")) {
      result.push({ ...step });
      continue;
    }

    const prefix = step.message.replace(/:.*$/, "");
    const prev = result[result.length - 1];
    if (prev && prev.message.startsWith("Polling ") && prev.message.replace(/:.*$/, "") === prefix) {
      prev.message = step.message;
      prev.status = step.status;
      prev.timestamp = step.timestamp;
      prev.pollCount = (prev.pollCount ?? 1) + 1;
    } else {
      result.push({ ...step, pollCount: 1 });
    }
  }

  return result;
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

function eventMatchesQuery(evt: StatusEvent, query: string): boolean {
  const q = query.toLowerCase();
  if (evt.summary.toLowerCase().includes(q)) return true;
  if (evt.detail && evt.detail.toLowerCase().includes(q)) return true;
  if (evt.type.toLowerCase().includes(q)) return true;
  if (evt.steps) {
    for (const step of evt.steps) {
      if (step.message.toLowerCase().includes(q)) return true;
    }
  }
  return false;
}

export default function EventDialog({ events, onClose }: EventDialogProps) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null);
  const [searchQuery, setSearchQuery] = useState("");

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
  const filtered = searchQuery
    ? reversed.filter((evt) => eventMatchesQuery(evt, searchQuery))
    : reversed;

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
        <div className="event-dialog-search">
          <input
            type="text"
            placeholder="Search events..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>
        <div className="event-dialog-body">
          {reversed.length === 0 ? (
            <div className="event-dialog-empty">No events recorded yet.</div>
          ) : filtered.length === 0 ? (
            <div className="event-dialog-empty">No matching events.</div>
          ) : (
            filtered.map((evt, idx) => {
              const originalIdx = events.indexOf(evt);
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
                      {consolidateSteps(evt.steps!).map((step, si) => (
                        <div className="event-step" key={si}>
                          <span className={`event-step-icon event-step-icon-${step.status}`}>
                            {step.status === "done" && (
                              <svg width="10" height="10" viewBox="0 0 10 10">
                                <path d="M2 5 L4.5 7.5 L8 2.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                              </svg>
                            )}
                            {step.status === "skipped" && (
                              <svg width="10" height="10" viewBox="0 0 10 10">
                                <path d="M1.5 2 L4.5 5 L1.5 8" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                                <path d="M5.5 2 L8.5 5 L5.5 8" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
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
                          {step.pollCount != null && step.pollCount > 1 && (
                            <span className="event-step-poll-count">x{step.pollCount}</span>
                          )}
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
