import { useCallback, useEffect, useRef, useState } from "react";
import type { PreviewData } from "./DataPreviewPanel";
import "./DiffTableView.css";

interface DiffTableViewProps {
  left: PreviewData;
  right: PreviewData;
  sortColumnIndex: number | null;
  sortDirection: "asc" | "desc" | null;
  onHeaderClick: (columnIndex: number) => void;
  hoveredColumn?: string | null;
  clickedColumn?: string | null;
  onHoveredColumnChange?: (col: string | null) => void;
  onClickedColumnChange?: (col: string | null) => void;
  tableRef?: React.RefObject<HTMLTableElement | null>;
  onScroll?: (e: React.UIEvent<HTMLDivElement>) => void;
  scrollRef?: React.RefObject<HTMLDivElement | null>;
}

const EDGE_THRESHOLD = 150;

export default function DiffTableView({
  left,
  right,
  sortColumnIndex,
  sortDirection,
  onHeaderClick,
  hoveredColumn,
  clickedColumn,
  onHoveredColumnChange,
  onClickedColumnChange,
  tableRef,
  onScroll,
  scrollRef,
}: DiffTableViewProps) {
  const headers = left.headers;
  const maxRows = Math.max(left.rows.length, right.rows.length);

  const containerRef = useRef<HTMLDivElement>(null);
  const innerTableRef = useRef<HTMLTableElement>(null);
  const isMouseOverRef = useRef(false);
  const thRefs = useRef<Map<string, HTMLTableCellElement>>(new Map());
  const [userWidths, setUserWidths] = useState<number[]>([]);
  const userWidthsRef = useRef(userWidths);
  userWidthsRef.current = userWidths;

  const setContainerRef = useCallback(
    (el: HTMLDivElement | null) => {
      (containerRef as React.MutableRefObject<HTMLDivElement | null>).current = el;
      if (scrollRef) {
        (scrollRef as React.MutableRefObject<HTMLDivElement | null>).current = el;
      }
    },
    [scrollRef],
  );

  const setTableRefCb = useCallback(
    (el: HTMLTableElement | null) => {
      (innerTableRef as React.MutableRefObject<HTMLTableElement | null>).current = el;
      if (tableRef) {
        (tableRef as React.MutableRefObject<HTMLTableElement | null>).current = el;
      }
    },
    [tableRef],
  );

  const setThRef = useCallback((colName: string, el: HTMLTableCellElement | null) => {
    if (el) {
      thRefs.current.set(colName, el);
    } else {
      thRefs.current.delete(colName);
    }
  }, []);

  // Auto-scroll when highlighted column changes from outside (ConnectionsPanel)
  const activeCol = clickedColumn ?? hoveredColumn;
  useEffect(() => {
    if (!activeCol || isMouseOverRef.current) return;

    const container = containerRef.current;
    const th = thRefs.current.get(activeCol);
    if (!container || !th) return;

    const containerRect = container.getBoundingClientRect();
    const thRect = th.getBoundingClientRect();

    const colLeftInView = thRect.left - containerRect.left;
    const colRightInView = thRect.right - containerRect.left;
    const viewWidth = containerRect.width;

    const nearLeftEdge = colLeftInView < EDGE_THRESHOLD;
    const nearRightEdge = colRightInView > viewWidth - EDGE_THRESHOLD;

    if (nearLeftEdge) {
      const targetLeft = container.scrollLeft + colLeftInView - viewWidth / 3;
      container.scrollTo({ left: Math.max(0, targetLeft), behavior: "smooth" });
    } else if (nearRightEdge) {
      const targetLeft = container.scrollLeft + colRightInView - (viewWidth * 2) / 3;
      container.scrollTo({ left: targetLeft, behavior: "smooth" });
    }
  }, [activeCol]);

  // Column resize via drag
  const resizing = useRef<{ colIndex: number; startX: number; startWidth: number } | null>(null);

  const onResizeMouseDown = useCallback((e: React.MouseEvent, colIndex: number) => {
    e.stopPropagation();
    e.preventDefault();

    const th = innerTableRef.current?.querySelectorAll("thead th")[colIndex];
    const startWidth = userWidthsRef.current[colIndex] ?? th?.getBoundingClientRect().width ?? 150;
    const startX = e.clientX;
    resizing.current = { colIndex, startX, startWidth };
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";

    const onMouseMove = (ev: MouseEvent) => {
      if (!resizing.current) return;
      const newWidth = Math.max(50, resizing.current.startWidth + (ev.clientX - resizing.current.startX));
      setUserWidths((prev) => {
        const next = [...prev];
        next[colIndex] = newWidth;
        return next;
      });
    };
    const onMouseUp = () => {
      resizing.current = null;
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
    };
    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
  }, []);

  // Reset user widths when data changes
  useEffect(() => {
    setUserWidths([]);
  }, [left, right]);

  const hasUserWidths = userWidths.length > 0;

  return (
    <div
      className="diff-table-scroll"
      ref={setContainerRef}
      onScroll={onScroll}
      onMouseEnter={() => { isMouseOverRef.current = true; }}
      onMouseLeave={() => { isMouseOverRef.current = false; }}
    >
      <table className="diff-table" ref={setTableRefCb}>
        {hasUserWidths && (
          <colgroup>
            {headers.map((_, i) => (
              <col key={i} style={userWidths[i] != null ? { minWidth: userWidths[i] } : undefined} />
            ))}
          </colgroup>
        )}
        <thead>
          <tr>
            {headers.map((h, i) => {
              const isClicked = clickedColumn === h;
              const isHovered = !isClicked && hoveredColumn === h;
              return (
                <th
                  key={i}
                  ref={(el) => setThRef(h, el)}
                  className={isClicked ? "column-highlight-click" : isHovered ? "column-highlight-hover" : ""}
                  style={userWidths[i] != null ? { minWidth: userWidths[i] } : undefined}
                  onClick={() => {
                    onClickedColumnChange?.(clickedColumn === h ? null : h);
                    onHeaderClick(i);
                  }}
                  onMouseEnter={() => onHoveredColumnChange?.(h)}
                  onMouseLeave={() => onHoveredColumnChange?.(null)}
                >
                  <span className="column-header-content">
                    {h}
                    {sortColumnIndex === i && sortDirection && (
                      <span className="column-sort-indicator">
                        {sortDirection === "asc" ? "\u25B2" : "\u25BC"}
                      </span>
                    )}
                  </span>
                  {left.columnTypes?.[i] && (
                    <span className="column-type-label">{left.columnTypes[i]}</span>
                  )}
                  <div
                    className="column-resize-handle"
                    onMouseDown={(e) => onResizeMouseDown(e, i)}
                  />
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: maxRows }, (_, ri) => {
            const leftRow = left.rows[ri];
            const rightRow = right.rows[ri];
            return (
              <tr key={ri}>
                {headers.map((h, ci) => {
                  const lv = leftRow?.[ci] ?? "";
                  const rv = rightRow?.[ci] ?? "";
                  const changed = lv !== rv;
                  const isClicked = clickedColumn === h;
                  const isHovered = !isClicked && hoveredColumn === h;
                  const hlClass = isClicked ? " column-highlight-click" : isHovered ? " column-highlight-hover" : "";
                  const cellHandlers = {
                    onMouseEnter: () => onHoveredColumnChange?.(h),
                    onMouseLeave: () => onHoveredColumnChange?.(null),
                    onClick: () => onClickedColumnChange?.(clickedColumn === h ? null : h),
                  };
                  if (!changed) {
                    return (
                      <td key={ci} className={hlClass.trim() || undefined} {...cellHandlers}>
                        {lv}
                      </td>
                    );
                  }
                  return (
                    <td key={ci} className={`diff-cell-changed${hlClass}`} {...cellHandlers}>
                      <span className="diff-old">{lv}</span>
                      <span className="diff-arrow">{"\u2192"}</span>
                      <span className="diff-new">{rv}</span>
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
