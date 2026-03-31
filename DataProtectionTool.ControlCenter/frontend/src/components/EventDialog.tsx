import { useEffect, useMemo, useRef, useState } from "react";
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

function formatBadgeLabel(type: string): string {
  return type.replace(/_/g, " ");
}

function getEventStatus(evt: StatusEvent): "running" | "error" | "done" {
  const s = evt.summary.toLowerCase();
  if (s.includes("error") || s.includes("failed")) return "error";
  if (evt.steps && evt.steps.length > 0) {
    const last = evt.steps[evt.steps.length - 1];
    if (last.status === "running") return "running";
    if (last.status === "error") return "error";
  }
  return "done";
}

function formatTime(iso: string): string {
  try {
    const d = new Date(iso);
    const yyyy = d.getFullYear();
    const MM = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    const HH = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const ss = String(d.getSeconds()).padStart(2, "0");
    const fff = String(d.getMilliseconds()).padStart(3, "0");
    return `${yyyy}/${MM}/${dd}:${HH}${mm}${ss}.${fff}`;
  } catch {
    return "";
  }
}

function hasSteps(evt: StatusEvent): boolean {
  return Array.isArray(evt.steps) && evt.steps.length > 0;
}

function eventMatchesSearch(evt: StatusEvent, query: string): boolean {
  const q = query.toLowerCase();
  if (evt.summary.toLowerCase().includes(q)) return true;
  if (evt.detail && evt.detail.toLowerCase().includes(q)) return true;
  if (evt.type.toLowerCase().includes(q)) return true;
  if (formatTime(evt.timestamp).toLowerCase().includes(q)) return true;
  if (evt.steps?.some((s) => s.message.toLowerCase().includes(q))) return true;
  return false;
}

function stepsMatchSearch(evt: StatusEvent, query: string): boolean {
  if (!query || !evt.steps) return false;
  const q = query.toLowerCase();
  return evt.steps.some((s) => s.message.toLowerCase().includes(q));
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

  const sorted = useMemo(
    () => [...events].sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()),
    [events]
  );

  const filtered = useMemo(() => {
    if (!searchQuery.trim()) return sorted;
    return sorted.filter((evt) => eventMatchesSearch(evt, searchQuery.trim()));
  }, [sorted, searchQuery]);

  const autoExpandedIndices = useMemo(() => {
    if (!searchQuery.trim()) return new Set<number>();
    const set = new Set<number>();
    for (const evt of filtered) {
      const originalIdx = events.indexOf(evt);
      if (hasSteps(evt) && stepsMatchSearch(evt, searchQuery.trim())) {
        set.add(originalIdx);
      }
    }
    return set;
  }, [filtered, events, searchQuery]);

  return (
    <>
      <div className="event-dialog-overlay" />
      <div className="event-dialog" ref={dialogRef}>
        <div className="event-dialog-header">
          <span className="event-dialog-title">Event History</span>
          <div className="event-dialog-search">
            <svg className="event-dialog-search-icon" width="12" height="12" viewBox="0 0 12 12">
              <circle cx="5" cy="5" r="3.5" fill="none" stroke="currentColor" strokeWidth="1.2" />
              <path d="M7.5 7.5 L10.5 10.5" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
            </svg>
            <input
              type="text"
              className="event-dialog-search-input"
              placeholder="Search..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>
          <button className="event-dialog-close" onClick={onClose} aria-label="Close">
            <svg width="14" height="14" viewBox="0 0 14 14">
              <path d="M3 3 L11 11 M11 3 L3 11" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
          </button>
        </div>
        <div className="event-dialog-body">
          {filtered.length === 0 ? (
            <div className="event-dialog-empty">
              {searchQuery.trim() ? "No matching events." : "No events recorded yet."}
            </div>
          ) : (
            filtered.map((evt, _idx) => {
              const originalIdx = events.indexOf(evt);
              const isExpanded = expandedIdx === originalIdx || autoExpandedIndices.has(originalIdx);
              const stepsPresent = hasSteps(evt);

              return (
                <div key={originalIdx}>
                  <div
                    className={`event-item${stepsPresent ? " event-item-expandable" : ""}`}
                    onClick={() => setExpandedIdx(isExpanded ? null : originalIdx)}
                  >
                    <div className="event-item-row1">
                      {stepsPresent && (
                        <span className={`event-item-chevron${isExpanded ? " event-item-chevron-open" : ""}`}>
                          <svg width="10" height="10" viewBox="0 0 10 10">
                            <path d="M3 2 L7 5 L3 8" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                          </svg>
                        </span>
                      )}
                      <span className={`event-item-status event-item-status-${getEventStatus(evt)}`}>
                        {getEventStatus(evt) === "done" && (
                          <svg width="10" height="10" viewBox="0 0 10 10">
                            <path d="M2 5 L4.5 7.5 L8 2.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                          </svg>
                        )}
                        {getEventStatus(evt) === "error" && (
                          <svg width="10" height="10" viewBox="0 0 10 10">
                            <path d="M3 3 L7 7 M7 3 L3 7" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                          </svg>
                        )}
                        {getEventStatus(evt) === "running" && <span className="event-item-spinner" />}
                      </span>
                      <span className="event-item-time">{formatTime(evt.timestamp)}</span>
                      <span className="event-item-type">{formatBadgeLabel(evt.type)}</span>
                    </div>
                    <div className="event-item-row2">
                      <span className="event-item-summary">{evt.summary}</span>
                    </div>
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
