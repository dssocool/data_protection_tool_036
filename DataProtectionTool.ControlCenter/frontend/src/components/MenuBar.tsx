import { useEffect, useRef, useState } from "react";
import "./MenuBar.css";

interface MenuBarProps {
  onSqlServerConnection: () => void;
  onNewQuery: () => void;
  onViewConnections: () => void;
  onViewFlows: () => void;
  oid: string;
  tid: string;
  uniqueId: string | null;
}

export default function MenuBar({
  onSqlServerConnection,
  onNewQuery,
  onViewConnections,
  onViewFlows,
  oid,
  tid,
  uniqueId,
}: MenuBarProps) {
  const [openMenu, setOpenMenu] = useState<string | null>(null);
  const [showProfile, setShowProfile] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const profileRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setOpenMenu(null);
      }
      if (profileRef.current && !profileRef.current.contains(e.target as Node)) {
        setShowProfile(false);
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
                <li className="menu-item">
                  <button onClick={() => handleAction(onNewQuery)}>
                    Query
                  </button>
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

      {oid && (
        <div className="menu-user-profile" ref={profileRef}>
          <button
            className={`menu-user-button ${showProfile ? "active" : ""}`}
            onClick={() => setShowProfile((v) => !v)}
            title={`${oid} | ${tid}`}
          >
            {oid.length > 8 ? oid.slice(0, 8) + "…" : oid}
            {tid ? ` | ${tid.length > 8 ? tid.slice(0, 8) + "…" : tid}` : ""}
          </button>
          {showProfile && (
            <div className="user-profile-popout">
              <div className="user-profile-row">
                <span className="user-profile-label">OID</span>
                <span className="user-profile-value">{oid}</span>
              </div>
              <div className="user-profile-row">
                <span className="user-profile-label">TID</span>
                <span className="user-profile-value">{tid}</span>
              </div>
              <div className="user-profile-divider" />
              <div className="user-profile-row">
                <span className="user-profile-label">Unique ID</span>
                <span className="user-profile-value user-profile-uid">
                  {uniqueId ?? "—"}
                </span>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
