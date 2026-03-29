import { useEffect, useRef, useState } from "react";
import "./MenuBar.css";

interface MenuBarProps {
  onSqlServerConnection: () => void;
  onViewConnections: () => void;
  onViewFlows: () => void;
}

export default function MenuBar({
  onSqlServerConnection,
  onViewConnections,
  onViewFlows,
}: MenuBarProps) {
  const [openMenu, setOpenMenu] = useState<string | null>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setOpenMenu(null);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  function handleTopLevelClick(name: string) {
    setOpenMenu((prev) => (prev === name ? null : name));
  }

  function handleAction(action: () => void) {
    setOpenMenu(null);
    action();
  }

  return (
    <div className="menu-bar" ref={menuRef}>
      {/* File menu */}
      <div className="menu-top-item">
        <button
          className={`menu-top-button ${openMenu === "file" ? "active" : ""}`}
          onClick={() => handleTopLevelClick("file")}
        >
          File
        </button>
        {openMenu === "file" && (
          <ul className="menu-dropdown">
            <li className="menu-item has-submenu">
              <span>New</span>
              <ul className="menu-submenu">
                <li className="menu-item has-submenu">
                  <span>Connections</span>
                  <ul className="menu-submenu">
                    <li className="menu-item">
                      <button
                        onClick={() => handleAction(onSqlServerConnection)}
                      >
                        SQL Server
                      </button>
                    </li>
                  </ul>
                </li>
              </ul>
            </li>
          </ul>
        )}
      </div>

      {/* View menu */}
      <div className="menu-top-item">
        <button
          className={`menu-top-button ${openMenu === "view" ? "active" : ""}`}
          onClick={() => handleTopLevelClick("view")}
        >
          View
        </button>
        {openMenu === "view" && (
          <ul className="menu-dropdown">
            <li className="menu-item">
              <button onClick={() => handleAction(onViewConnections)}>
                Connections
              </button>
            </li>
            <li className="menu-item">
              <button onClick={() => handleAction(onViewFlows)}>Flows</button>
            </li>
          </ul>
        )}
      </div>
    </div>
  );
}
