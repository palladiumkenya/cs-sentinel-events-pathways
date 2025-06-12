import { useState } from "react";
import "./InfoBar.css";

export default function InfoBar() {
  const [isHidden, setIsHidden] = useState(false);

  return (
    !isHidden && (
      <div className="info-banner" id="myBanner">
        <button className="close-btn" onClick={() => setIsHidden(true)}>
          ×
        </button>
        <p>Refresh done after every month.</p>
      </div>
    )
  );
}
