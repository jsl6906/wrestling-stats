# ${observable.params.gov_body.toUpperCase()} Individual Wrestler Statistics

```js
const elo_history_parquet = FileAttachment(`data/elo_history_${observable.params.gov_body}.parquet`).parquet();
const wrestlers_parquet = FileAttachment(`data/wrestlers_${observable.params.gov_body}.parquet`).parquet();
```

```js
const elo_history = Array.from(elo_history_parquet);
const wrestlers = Array.from(wrestlers_parquet);
```

```js
import {select} from "npm:@observablehq/inputs";
import * as Plot from "npm:@observablehq/plot";
import {FileAttachment, view} from "observablehq:stdlib";
```

```js
// URL parameter utilities
function getUrlParam(name) {
  const url = new URL(window.location);
  return url.searchParams.get(name);
}

function setUrlParam(name, value) {
  const url = new URL(window.location);
  if (value) {
    url.searchParams.set(name, value);
  } else {
    url.searchParams.delete(name);
  }
  window.history.replaceState({}, '', url);
}

// Get initial wrestler from URL
const initialWrestlerFromUrl = getUrlParam('wrestler');
```

```js
// Helper function to format wrestler names as "Last, First M"
function formatWrestlerName(name) {
  if (!name || typeof name !== 'string') return name || "-";
  
  const parts = name.trim().split(/\s+/);
  if (parts.length === 1) {
    return parts[0]; // Single name, return as is
  } else if (parts.length === 2) {
    // First Last -> Last, First
    return `${parts[1]}, ${parts[0]}`;
  } else {
    // First Middle Last or First M Last -> Last, First M
    const firstName = parts[0];
    const lastName = parts[parts.length - 1];
    const middle = parts.slice(1, -1).join(' ');
    return `${lastName}, ${firstName}${middle ? ' ' + middle : ''}`;
  }
}

// Helper function to create wrestler links with formatted names
function createWrestlerLink(wrestlerName) {
  if (!wrestlerName) return wrestlerName || "-";
  const url = new URL(window.location);
  url.searchParams.set('wrestler', wrestlerName);
  const displayName = formatWrestlerName(wrestlerName);
  return `<a href="${url.href}" target="_blank" rel="noopener noreferrer">${displayName}</a>`;
}
```

```js
// Calculate date range across all matches
const dateRange = (() => {
  const dates = elo_history
    .map(d => parseDate(d.start_date_iso ?? d.start_date))
    .filter(Boolean)
    .sort((a, b) => a - b);
  
  if (dates.length === 0) return { min: null, max: null, count: 0 };
  
  return {
    min: dates[0],
    max: dates[dates.length - 1],
    count: dates.length
  };
})();
```

```js
// Helper to robustly parse dates from parquet (string/date/number)
function parseDate(v) {
  if (v == null) return null;
  // Prefer ISO shadow column if present on row
  if (typeof v === "string") {
    const dt = new Date(v);
    return isNaN(+dt) ? null : dt;
  }
  if (v && typeof v.toISOString === "function") return new Date(v);
  if (typeof v === "number") {
    // Heuristic: treat small numbers as days-since-epoch, large as ms
    const ms = v < 1e10 ? v * 86400000 : v;
    const dt = new Date(ms);
    return isNaN(+dt) ? null : dt;
  }
  return null;
}
```

```js
// Display date range at top of page
(() => {
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  if (!dateRange.min || !dateRange.max) {
    return wrap(`<p style="font-size: 0.9em; font-style: italic; margin: 0 0 20px 0;">No match data available</p>`);
  }
  
  const formatDate = (date) => date.toLocaleDateString('en-US', { 
    year: 'numeric', 
    month: 'long', 
    day: 'numeric' 
  });
  
  return wrap(`<p style="font-size: 0.9em; font-style: italic; margin: 0 0 20px 0;">Data coverage: ${formatDate(dateRange.min)} to ${formatDate(dateRange.max)} • ${dateRange.count.toLocaleString()} total matches</p>`);
})()
```

```js
// Map wrestler name -> team (prefer wrestlers table, fallback to history)
const nameToTeam = (() => {
  const m = new Map();
  for (const w of wrestlers) if (w && w.name) m.set(w.name, w.last_team ?? null);
  for (const d of elo_history) if (d && d.name && !m.has(d.name)) m.set(d.name, d.team ?? null);
  return m;
})();
```

```js
// Team selector (optional)
const teamOptions = [
  "All teams",
  ...Array.from(new Set(Array.from(nameToTeam.values()).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)))
];
const teamFilter = view(select(teamOptions, {label: "Team", value: teamOptions[0]}));
```

```js
// Unique wrestler names for selector
const names = Array.from(new Set(
  Array.from(nameToTeam.entries())
    .filter(([name, team]) => teamFilter === "All teams" || team === teamFilter)
    .map(([name]) => name)
)).sort((a, b) => {
  // Sort by formatted name (Last, First M)
  const formattedA = formatWrestlerName(a);
  const formattedB = formatWrestlerName(b);
  return String(formattedA).localeCompare(String(formattedB));
});
```

```js
// Wrestler selector (top of page) — persist selection per team, across changes, and in URL
const wrestler = (() => {
  const cache = (globalThis.__wrestlerByTeam ||= {});
  const teamKey = teamFilter === "All teams" ? "__ALL__" : String(teamFilter);
  const prevForTeam = cache[teamKey];
  const prevGlobal = globalThis.__wrestlerSelected;
  const initial =
    (initialWrestlerFromUrl && names.includes(initialWrestlerFromUrl)) ? initialWrestlerFromUrl :
    (prevForTeam && names.includes(prevForTeam)) ? prevForTeam :
    (prevGlobal && names.includes(prevGlobal)) ? prevGlobal :
    null; // Default to no selection
  // Create formatted name map and options array
  const nameDisplayMap = new Map();
  const formattedOptions = names.map(name => {
    const formatted = formatWrestlerName(name);
    nameDisplayMap.set(formatted, name);
    return formatted;
  });
  
  const options = ["Select a wrestler...", ...formattedOptions];
  const control = select(options, {label: "Wrestler", value: initial ? formatWrestlerName(initial) : "Select a wrestler..."});
  
  // Convert display value back to original name for persistence
  const getOriginalName = (displayValue) => {
    if (displayValue === "Select a wrestler...") return null;
    return nameDisplayMap.get(displayValue) || displayValue;
  };
  // Ensure control reflects initial pick even if Inputs reuses DOM
  control.value = initial ? formatWrestlerName(initial) : "Select a wrestler...";
  // Persist initial selection immediately (only if not the placeholder)
  if (initial && initial !== "Select a wrestler...") {
    globalThis.__wrestlerSelected = initial;
    cache[teamKey] = initial;
    setUrlParam('wrestler', initial);
  }
  const persist = () => {
    const displayValue = control.value;
    const originalName = getOriginalName(displayValue);
    if (originalName) {
      globalThis.__wrestlerSelected = originalName;
      cache[teamKey] = originalName;
      setUrlParam('wrestler', originalName);
    } else {
      globalThis.__wrestlerSelected = null;
      delete cache[teamKey];
      setUrlParam('wrestler', null);
    }
  };
  control.addEventListener("input", persist);
  control.addEventListener("change", persist);
  return view(control);
})();
```

```js
// Ensure a valid selection if team filter changes
const activeWrestler = (() => {
  if (!wrestler || wrestler === "Select a wrestler...") {
    return null; // No wrestler selected
  }
  // Convert display name back to original name for data lookup
  const displayValue = wrestler;
  const nameDisplayMap = new Map();
  names.forEach(name => {
    nameDisplayMap.set(formatWrestlerName(name), name);
  });
  const originalName = nameDisplayMap.get(displayValue) || displayValue;
  const selected = names.includes(originalName) ? originalName : null;
  // Update URL if we had to fall back to no wrestler
  if (selected !== originalName) {
    setUrlParam('wrestler', selected);
  }
  return selected;
})();
```

## Wrestler Statistics

```js
// Selected wrestler summary card (DOM)
(() => {
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  if (!activeWrestler) {
    return wrap(`<div class="empty-state"><h3>Select a wrestler to view statistics</h3><p>Choose a wrestler from the dropdown above to see their detailed statistics, match history, and performance charts.</p></div>`);
  }
  
  const w = Array.isArray(wrestlers) ? wrestlers.find(d => d.name === activeWrestler) : null;
  const fmt = (n) => Number.isFinite(+n) ? Math.round(+n) : "-";
  const fmt1 = (n) => Number.isFinite(+n) ? (+n).toFixed(1) : "-";
  const dateStr = (v) => v ? (new Date(v)).toLocaleDateString?.() ?? String(v) : "-";
  if (!w) return wrap(`<p>No summary available.</p>`);
  
  const wins = Number(w.wins ?? 0);
  const wins_fall = Number(w.wins_fall ?? 0);
  const losses = Number(w.losses ?? 0);
  const losses_fall = Number(w.losses_fall ?? 0);
  const opponent_avg_elo = Number(w.opponent_avg_elo);
  const denom = (wins + losses);
  const win_pct = denom > 0 ? (wins / denom) * 100 : null;
  // Arrange: Team at top; general stats next; Elo-related grouped at bottom
  const generalRows = [
    ["Current Team", w.last_team ?? "-"],
    ["Matches", w.matches_played ?? 0],
    ["Wins", wins],
    ["Wins (Fall)", wins_fall],
    ["Losses", losses],
    ["Losses (Fall)", losses_fall],
    ["Win %", win_pct == null ? '-' : `${fmt1(win_pct)}%`],
    ["Last Opponent", createWrestlerLink(w.last_opponent_name)],
    ["Last Match Date", dateStr(w.last_start_date)]
  ];
  const eloRows = [
    ["Current Elo", fmt(w.current_elo)],
    ["Best Elo", fmt(w.best_elo)],
    ["Best Date", dateStr(w.best_date)],
    ["Opp. Avg Elo", opponent_avg_elo == null ? '-' : fmt1(opponent_avg_elo)],
    ["Last Elo Adj", fmt(w.last_adjustment)]
  ];
  const generalPart = generalRows.map(([k,v]) => `<tr><th>${k}</th><td>${v}</td></tr>`).join("");
  const eloHeader = `<tr><th colspan="2" style="font-weight: bold; text-align: left; padding: 3px; border-top: 1px solid; border-bottom: 1px solid;">Elo Rating Stats <a href="#what-is-elo-rating"> What is Elo Rating?</a></th></tr>`;
  const eloPart = eloRows.map(([k,v]) => `<tr><th>${k}</th><td>${v}</td></tr>`).join("");
  const tableRows = generalPart + eloHeader + eloPart;
  return wrap(`
    <div>
      <h3>${formatWrestlerName(w.name)}</h3>
      <table>
        ${tableRows}
      </table>
    </div>
  `);
})()
```

## Opponent Statistics

```js
// Sortable opponent summary table
(() => {
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  if (!activeWrestler) {
    return wrap(`<div class="empty-state"><h3>Select a wrestler to view opponent statistics</h3><p>Choose a wrestler from the dropdown above to see their head-to-head records against other wrestlers.</p></div>`);
  }
  
  const rowsFor = elo_history.filter(d => d.name === activeWrestler);
  if (!rowsFor.length) {
    return wrap(`<div class="empty-state"><h3>No opponent data available</h3><p>No match history found for ${formatWrestlerName(activeWrestler)}.</p></div>`);
  }
  
  // Group matches by opponent
  const opponentStats = new Map();
  
  for (const match of rowsFor) {
    const opp = match.opponent_name;
    if (!opp) continue;
    
    if (!opponentStats.has(opp)) {
      opponentStats.set(opp, {
        name: opp,
        wins: 0,
        losses: 0,
        winsFall: 0,
        lossesFall: 0,
        totalMatches: 0,
        avgPreElo: 0,
        avgPostElo: 0,
        lastDate: null,
        eloSum: 0,
        postEloSum: 0
      });
    }
    
    const stats = opponentStats.get(opp);
    stats.totalMatches++;
    
    const isFall = String(match.decision_type || '').toLowerCase().includes('fall') || ['FALL', 'PIN'].includes(String(match.decision_type_code || '').toUpperCase());
    
    if (match.role === 'W' || match.role === 'winner') {
      stats.wins++;
      if (isFall) stats.winsFall++;
    } else if (match.role === 'L' || match.role === 'loser') {
      stats.losses++;
      if (isFall) stats.lossesFall++;
    }
    
    const preElo = Number(match.opponent_pre_elo);
    const postElo = Number(match.opponent_post_elo);
    
    if (Number.isFinite(preElo)) {
      stats.eloSum += preElo;
    }
    if (Number.isFinite(postElo)) {
      stats.postEloSum += postElo;
    }
    
    const matchDate = new Date(match.start_date_iso || match.start_date);
    if (!isNaN(matchDate) && (!stats.lastDate || matchDate > stats.lastDate)) {
      stats.lastDate = matchDate;
    }
  }
  
  // Calculate averages and prepare data
  let opponents = Array.from(opponentStats.values())
    .map(stats => ({
      ...stats,
      avgPreElo: stats.totalMatches > 0 ? stats.eloSum / stats.totalMatches : 0,
      avgPostElo: stats.totalMatches > 0 ? stats.postEloSum / stats.totalMatches : 0,
      winPct: stats.totalMatches > 0 ? (stats.wins / stats.totalMatches) * 100 : 0
    }));
  
  if (!opponents.length) return null;
  
  // Create sortable table
  const tableId = `opponent-table-${activeWrestler.replace(/[^a-zA-Z0-9]/g, '-')}`;
  let currentSort = { column: 'totalMatches', direction: 'desc' };
  
  // Initial sort
  const sortData = (column, direction) => {
    opponents.sort((a, b) => {
      let aVal = a[column];
      let bVal = b[column];
      
      // Handle date sorting
      if (column === 'lastDate') {
        aVal = aVal ? aVal.getTime() : 0;
        bVal = bVal ? bVal.getTime() : 0;
      }
      
      // Handle string sorting (case insensitive) and name column sorting with formatting
      if (typeof aVal === 'string') {
        if (column === 'name') {
          // For name column, sort by formatted names
          aVal = formatWrestlerName(aVal).toLowerCase();
          bVal = formatWrestlerName(bVal).toLowerCase();
        } else {
          aVal = aVal.toLowerCase();
          bVal = bVal.toLowerCase();
        }
      }
      
      let comparison;
      if (aVal < bVal) {
        comparison = -1;
      } else if (aVal > bVal) {
        comparison = 1;
      } else {
        // Fallback to formatted name for stable sort
        comparison = formatWrestlerName(a.name).localeCompare(formatWrestlerName(b.name));
      }
      
      return direction === 'desc' ? -comparison : comparison;
    });
  };
  
  const renderTable = () => {
    const fmt = (n) => Number.isFinite(+n) ? Math.round(+n) : "-";
    const fmt1 = (n) => Number.isFinite(+n) ? (+n).toFixed(1) : "-";
    const dateStr = (v) => v ? v.toLocaleDateString?.() ?? String(v) : "-";
    
    const getSortIcon = (column) => {
      if (currentSort.column !== column) return ' ↕';
      return currentSort.direction === 'desc' ? ' ↓' : ' ↑';
    };
    
    const rows = opponents.map(opp => `
      <tr>
        <td>${createWrestlerLink(opp.name)}</td>
        <td>${opp.totalMatches}</td>
        <td>${opp.wins}</td>
        <td>${opp.losses}</td>
        <td>${opp.winsFall}</td>
        <td>${opp.lossesFall}</td>
        <td>${fmt1(opp.winPct)}%</td>
        <td>${fmt(opp.avgPreElo)}</td>
        <td>${dateStr(opp.lastDate)}</td>
      </tr>
    `).join("");
    
    return `
      <div>
        <table id="${tableId}">
          <thead>
            <tr>
              <th data-sort="name" style="cursor: pointer; user-select: none;">Opponent${getSortIcon('name')}</th>
              <th data-sort="totalMatches" style="cursor: pointer; user-select: none;">Matches${getSortIcon('totalMatches')}</th>
              <th data-sort="wins" style="cursor: pointer; user-select: none;">Wins${getSortIcon('wins')}</th>
              <th data-sort="losses" style="cursor: pointer; user-select: none;">Losses${getSortIcon('losses')}</th>
              <th data-sort="winsFall" style="cursor: pointer; user-select: none;">Wins (Fall)${getSortIcon('winsFall')}</th>
              <th data-sort="lossesFall" style="cursor: pointer; user-select: none;">Losses (Fall)${getSortIcon('lossesFall')}</th>
              <th data-sort="winPct" style="cursor: pointer; user-select: none;">Win %${getSortIcon('winPct')}</th>
              <th data-sort="avgPreElo" style="cursor: pointer; user-select: none;">Avg Opp Elo${getSortIcon('avgPreElo')}</th>
              <th data-sort="lastDate" style="cursor: pointer; user-select: none;">Last Match${getSortIcon('lastDate')}</th>
            </tr>
          </thead>
          <tbody>
            ${rows}
          </tbody>
        </table>
      </div>
    `;
  };
  
  const handleSort = (column) => {
    // Toggle direction if same column, default to desc for new column
    if (currentSort.column === column) {
      currentSort.direction = currentSort.direction === 'desc' ? 'asc' : 'desc';
    } else {
      currentSort.column = column;
      currentSort.direction = 'desc';
    }
    
    // Sort data
    sortData(column, currentSort.direction);
    
    // Update the table content
    updateTable();
  };
  
  const updateTable = () => {
    const tableContainer = document.getElementById(`container-${tableId}`);
    if (tableContainer) {
      tableContainer.innerHTML = renderTable();
      attachEventListeners();
    }
  };
  
  const attachEventListeners = () => {
    const headers = document.querySelectorAll(`#${tableId} th[data-sort]`);
    headers.forEach(header => {
      header.addEventListener('click', () => {
        handleSort(header.dataset.sort);
      });
    });
  };
  
  // Sort initially by total matches (desc)
  sortData('totalMatches', 'desc');
  
  const tableHtml = `<div id="container-${tableId}">${renderTable()}</div>`;
  const container = wrap(tableHtml);
  
  // Attach initial event listeners
  setTimeout(() => attachEventListeners(), 0);
  
  return container;
})()
```

```js
// Coerce BigInt/strings to Numbers for plotting
function toNum(v) {
  if (v == null) return null;
  if (typeof v === "number") return v;
  if (typeof v === "bigint") return Number(v);
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}
```



```js
// Series for selected wrestler, ordered chronologically
const series = !activeWrestler ? [] : elo_history
  .filter(d => d.name === activeWrestler)
  .map(d => ({
    date: parseDate(d.start_date_iso ?? d.start_date),
    seq: toNum(d.elo_sequence),
    post_elo: toNum(d.post_elo),
    pre_elo: toNum(d.pre_elo),
    opponent: d.opponent_name,
    tournament: d.tournament_name ?? d.event_id,
    round: d.round_label ?? d.round_detail,
    role: d.role,
  }))
  .filter(d => d.date && Number.isFinite(d.seq) && Number.isFinite(d.post_elo))
  .sort((a, b) => a.date - b.date);
```

## Elo Ratings Over Time
[What is Elo rating?](#what-is-elo-rating)

```js
// Line chart of Elo over time for the selected wrestler
!activeWrestler ? "Select a wrestler to view their Elo rating history." : Plot.plot({
  width: 900,
  height: 420,
  marginLeft: 48,
  x: {axis: null},
  y: {label: `Elo (${formatWrestlerName(activeWrestler)})`, grid: true},
  marks: [
  Plot.line(series, {x: "seq", y: "post_elo", curve: "step-after"}),
    Plot.dot(series, {
      x: "seq",
      y: "post_elo",
      tip: true,
      title: d => {
        const ds = d.date ? d.date.toLocaleDateString?.() ?? String(d.date) : "";
        return `${formatWrestlerName(activeWrestler)}\n${d.tournament} — ${d.round}\nvs ${formatWrestlerName(d.opponent)}\nDate: ${ds}\nElo: ${Math.round(d.post_elo)}`
      }
    })
  ]
})
```

## Elo History Comparison
Orange lines reflect past opponents

```js
// Display scope selector for comparison chart
const displayScopeOptions = [`All ${observable.params.gov_body.toUpperCase()} Wrestlers`, 'Current Team', 'Opponents'];
const displayScope = view(select(displayScopeOptions, {label: "Compare against", value: displayScopeOptions[0]}));
```

```js
// Get list of opponents for the selected wrestler
const opponentNames = !activeWrestler ? new Set() : new Set(
  elo_history
    .filter(d => d.name === activeWrestler && d.opponent_name)
    .map(d => d.opponent_name)
);
```

```js
// Prepare overlay data based on display scope selection
const allWrestlerData = !activeWrestler ? [] : (() => {
  let filteredData = elo_history
    .map(d => ({ name: d.name, seq: toNum(d.elo_sequence), post_elo: toNum(d.post_elo) }))
    .filter(d => d.name && Number.isFinite(d.seq) && Number.isFinite(d.post_elo))
    .filter(d => d.name !== activeWrestler); // Exclude the selected wrestler from background
  
  // Apply display scope filter
  if (displayScope === 'Current Team') {
    const currentTeam = nameToTeam.get(activeWrestler);
    if (currentTeam) {
      filteredData = filteredData.filter(d => (nameToTeam.get(d.name) ?? null) === currentTeam);
    } else {
      filteredData = []; // No team data, show empty
    }
  } else if (displayScope === 'Opponents') {
    filteredData = filteredData.filter(d => opponentNames.has(d.name));
  }
  // For 'All Wrestlers', no additional filtering needed
  
  return filteredData;
})();
```

```js
// Split into opponents and non-opponents based on display scope
const opponentSeries = (() => {
  if (displayScope === 'Opponents') {
    // When showing only opponents, all data should be styled as opponents
    return allWrestlerData.sort((a, b) => {
      const n = formatWrestlerName(a.name).localeCompare(formatWrestlerName(b.name));
      return n !== 0 ? n : (a.seq - b.seq);
    });
  } else {
    // For other scopes, highlight actual opponents
    return allWrestlerData
      .filter(d => opponentNames.has(d.name))
      .sort((a, b) => {
        const n = formatWrestlerName(a.name).localeCompare(formatWrestlerName(b.name));
        return n !== 0 ? n : (a.seq - b.seq);
      });
  }
})();

const nonOpponentSeries = (() => {
  if (displayScope === 'Opponents') {
    // When showing only opponents, no non-opponent series
    return [];
  } else {
    // For other scopes, show non-opponents with faint styling
    return allWrestlerData
      .filter(d => !opponentNames.has(d.name))
      .sort((a, b) => {
        const n = formatWrestlerName(a.name).localeCompare(formatWrestlerName(b.name));
        return n !== 0 ? n : (a.seq - b.seq);
      });
  }
})();
```

```js
// Endpoint of the selected series for labeling
const selectedEnd = series.length ? series[series.length - 1] : null;
```

```js
// Render overlay chart (guard against empty data)
!activeWrestler ? "Select a wrestler to view comparison with other wrestlers." : (opponentSeries.length || nonOpponentSeries.length || series.length)
  ? (() => {
      const chart = Plot.plot({
        width: 900,
        height: 380,
        marginLeft: 48,
        x: {axis: null},
        y: {label: "Elo", grid: true},
        marks: [
          // Background: non-opponent wrestlers, very faint lines
          Plot.line(nonOpponentSeries, {
            x: "seq",
            y: "post_elo",
            z: "name",
            stroke: "#ccc",
            strokeOpacity: 0.12,
            strokeWidth: 1,
            curve: "step-after",
            tip: true,
            title: d => {
              const url = new URL(window.location);
              url.searchParams.set('wrestler', d.name);
              return `${formatWrestlerName(d.name)}\nClick to view: ${url.href}`;
            }
          }),
          // Highlighted: opponent wrestlers or all wrestlers in opponents-only view
          Plot.line(opponentSeries, {
            x: "seq",
            y: "post_elo",
            z: "name",
            stroke: "#ff7f0e",
            strokeOpacity: 0.35,
            strokeWidth: 1.5,
            curve: "step-after",
            tip: true,
            title: d => {
              const url = new URL(window.location);
              url.searchParams.set('wrestler', d.name);
              const suffix = displayScope === 'Opponents' ? '' : ' (opponent)';
              return `${formatWrestlerName(d.name)}${suffix}\nClick to view: ${url.href}`;
            }
          }),
          // Halo for selected wrestler line (drawn after background, before foreground)
          Plot.line(series, {
            x: "seq",
            y: "post_elo",
            stroke: "white",
            strokeOpacity: 0.9,
            strokeWidth: 6,
            curve: "step-after"
          }),
          // Foreground selected wrestler line
          Plot.line(series, {
            x: "seq",
            y: "post_elo",
            stroke: "steelblue",
            strokeOpacity: 1,
            strokeWidth: 3,
            curve: "step-after"
          }),
          // Label the selected wrestler at the end of their line
          ...(selectedEnd
            ? [
                Plot.text([selectedEnd], {
                  x: "seq",
                  y: "post_elo",
                  text: () => formatWrestlerName(activeWrestler),
                  dx: -8,
                  textAnchor: "end",
                  fill: "steelblue",
                  fontWeight: "bold",
                  stroke: "white",
                  strokeWidth: 3
                })
              ]
            : [])
        ]
      });
      
      // Add click handler to chart lines
      chart.addEventListener('click', (event) => {
        const target = event.target;
        if (target && target.__data__ && target.__data__.name && target.__data__.name !== activeWrestler) {
          const url = new URL(window.location);
          url.searchParams.set('wrestler', target.__data__.name);
          window.open(url.href, '_blank', 'noopener,noreferrer');
        }
      });
      
      return chart;
    })()
  : "No Elo data available to plot yet."
```

## What is Elo rating?

The Elo rating system is a method for calculating the relative skill levels of players in competitive games, famously used in chess. It assigns a numerical rating that is updated after every match based on the outcome and the difference in the players' ratings. If a higher-rated player wins as expected, they gain only a few points, but if a lower-rated player wins an upset, they earn a significant rating boost. The system is self-correcting over time, rewarding better-than-expected performance with rating increases and penalizing underperformance with decreases, providing an objective measure of a player's relative strength.


