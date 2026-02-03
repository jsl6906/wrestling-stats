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
  
  // Handle hyphenated last names that may have been split (e.g., "Cam Cook -Cash")
  // Join any part starting with hyphen to the previous part
  for (let i = parts.length - 1; i > 0; i--) {
    if (parts[i].startsWith('-')) {
      parts[i - 1] = parts[i - 1] + parts[i];
      parts.splice(i, 1);
    }
  }
  
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
    return wrap(`<div class="empty-state red"><h3>Select a wrestler to view statistics</h3><p>Choose a wrestler from the dropdown above to see their detailed statistics, match history, and performance charts.</p></div>`);
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

## Statistics by Team

```js
// Team-based statistics table
(() => {
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  if (!activeWrestler) {
    return wrap(`<div class="empty-state red"><h3>Select a wrestler to view team statistics</h3><p>Choose a wrestler from the dropdown above to see their statistics broken down by team.</p></div>`);
  }
  
  // Debug: check data availability
  const allRowsFor = elo_history.filter(d => d.name === activeWrestler);
  const rowsFor = allRowsFor.filter(d => d.bye !== true);
  if (!rowsFor.length) {
    return wrap(`<div class="empty-state"><h3>No team data available</h3><p>No match history found for ${formatWrestlerName(activeWrestler)}.</p></div>`);
  }
  
  // Group matches by team
  const teamStats = new Map();
  
  for (const match of rowsFor) {
    const team = match.team || "Unknown";
    
    if (!teamStats.has(team)) {
      teamStats.set(team, {
        team: team,
        wins: 0,
        losses: 0,
        winsFall: 0,
        lossesFall: 0,
        totalMatches: 0,
        oppEloSum: 0,
        oppEloCount: 0
      });
    }
    
    const stats = teamStats.get(team);
    
    const isFall = String(match.decision_type || '').toLowerCase().includes('fall') || ['FALL', 'PIN'].includes(String(match.decision_type_code || '').toUpperCase());
    
    if (match.role === 'W' || match.role === 'winner') {
      stats.totalMatches++;
      stats.wins++;
      if (isFall) stats.winsFall++;
    } else if (match.role === 'L' || match.role === 'loser') {
      stats.totalMatches++;
      stats.losses++;
      if (isFall) stats.lossesFall++;
    }
    
    // Track opponent ELO
    const oppPreElo = Number(match.opponent_pre_elo);
    if (Number.isFinite(oppPreElo)) {
      stats.oppEloSum += oppPreElo;
      stats.oppEloCount++;
    }
  }
  
  // Calculate averages and prepare data
  const teams = Array.from(teamStats.values())
    .map(stats => ({
      ...stats,
      winPct: stats.totalMatches > 0 ? (stats.wins / stats.totalMatches) * 100 : 0,
      avgOppElo: stats.oppEloCount > 0 ? stats.oppEloSum / stats.oppEloCount : 0
    }))
    .sort((a, b) => b.totalMatches - a.totalMatches);
  
  if (!teams.length) {
    return wrap(`<div class="empty-state"><h3>No team data available</h3><p>No team information found for ${formatWrestlerName(activeWrestler)}.</p></div>`);
  }
  
  const fmt1 = (n) => Number.isFinite(+n) ? (+n).toFixed(1) : "-";
  const fmt = (n) => Number.isFinite(+n) ? Math.round(+n) : "-";
  
  // Calculate totals for comparison with overall stats
  const totalWins = teams.reduce((sum, t) => sum + t.wins, 0);
  const totalLosses = teams.reduce((sum, t) => sum + t.losses, 0);
  
  const tableRows = teams.map(stats => `
    <tr>
      <td>${stats.team}</td>
      <td>${stats.wins}</td>
      <td>${stats.losses}</td>
      <td>${stats.winsFall}</td>
      <td>${stats.lossesFall}</td>
      <td>${fmt1(stats.winPct)}%</td>
      <td>${fmt(stats.avgOppElo)}</td>
    </tr>
  `).join("");
  
  return wrap(`
    <div>
    <p style="font-size: 0.85em; color: #666; margin-bottom: 10px;">Debug: ${allRowsFor.length} total matches, ${rowsFor.length} non-bye matches, ${totalWins} wins, ${totalLosses} losses</p>
    <table>
      <thead>
        <tr>
          <th>Team</th>
          <th>Wins</th>
          <th>Losses</th>
          <th>Wins (Fall)</th>
          <th>Losses (Fall)</th>
          <th>Win %</th>
          <th>Avg Opp Elo</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
      </tbody>
    </table>
    </div>
  `);
})()
```

## Statistics by Season

```js
// Season-based statistics table
(() => {
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  if (!activeWrestler) {
    return wrap(`<div class="empty-state red"><h3>Select a wrestler to view season statistics</h3><p>Choose a wrestler from the dropdown above to see their statistics broken down by season.</p></div>`);
  }
  
  // Debug: check data availability
  const allRowsFor = elo_history.filter(d => d.name === activeWrestler);
  const rowsFor = allRowsFor.filter(d => d.bye !== true);
  if (!rowsFor.length) {
    return wrap(`<div class="empty-state"><h3>No season data available</h3><p>No match history found for ${formatWrestlerName(activeWrestler)}.</p></div>`);
  }
  
  // Group matches by season (Sept 1 to Aug 31)
  const seasonStats = new Map();
  
  for (const match of rowsFor) {
    const matchDate = parseDate(match.start_date_iso ?? match.start_date);
    if (!matchDate) continue;
    
    // Calculate season: Sept 1 to Aug 31
    const year = matchDate.getFullYear();
    const month = matchDate.getMonth() + 1; // JavaScript months are 0-indexed
    const season = month >= 9 
      ? `${year}-${year + 1}` 
      : `${year - 1}-${year}`;
    
    if (!seasonStats.has(season)) {
      seasonStats.set(season, {
        season: season,
        wins: 0,
        losses: 0,
        winsFall: 0,
        lossesFall: 0,
        totalMatches: 0,
        oppEloSum: 0,
        oppEloCount: 0
      });
    }
    
    const stats = seasonStats.get(season);
    
    const isFall = String(match.decision_type || '').toLowerCase().includes('fall') || ['FALL', 'PIN'].includes(String(match.decision_type_code || '').toUpperCase());
    
    if (match.role === 'W' || match.role === 'winner') {
      stats.totalMatches++;
      stats.wins++;
      if (isFall) stats.winsFall++;
    } else if (match.role === 'L' || match.role === 'loser') {
      stats.totalMatches++;
      stats.losses++;
      if (isFall) stats.lossesFall++;
    }
    
    // Track opponent ELO
    const oppPreElo = Number(match.opponent_pre_elo);
    if (Number.isFinite(oppPreElo)) {
      stats.oppEloSum += oppPreElo;
      stats.oppEloCount++;
    }
  }
  
  // Calculate averages and prepare data
  const seasons = Array.from(seasonStats.values())
    .map(stats => ({
      ...stats,
      winPct: stats.totalMatches > 0 ? (stats.wins / stats.totalMatches) * 100 : 0,
      avgOppElo: stats.oppEloCount > 0 ? stats.oppEloSum / stats.oppEloCount : 0
    }))
    .sort((a, b) => b.season.localeCompare(a.season)); // Most recent season first
  
  if (!seasons.length) {
    return wrap(`<div class="empty-state"><h3>No season data available</h3><p>No season information found for ${formatWrestlerName(activeWrestler)}.</p></div>`);
  }
  
  const fmt1 = (n) => Number.isFinite(+n) ? (+n).toFixed(1) : "-";
  const fmt = (n) => Number.isFinite(+n) ? Math.round(+n) : "-";
  
  // Calculate totals for comparison with overall stats
  const totalWins = seasons.reduce((sum, s) => sum + s.wins, 0);
  const totalLosses = seasons.reduce((sum, s) => sum + s.losses, 0);
  
  const tableRows = seasons.map(stats => `
    <tr>
      <td>${stats.season}</td>
      <td>${stats.wins}</td>
      <td>${stats.losses}</td>
      <td>${stats.winsFall}</td>
      <td>${stats.lossesFall}</td>
      <td>${fmt1(stats.winPct)}%</td>
      <td>${fmt(stats.avgOppElo)}</td>
    </tr>
  `).join("");
  
  return wrap(`
    <div>
    <p style="font-size: 0.85em; color: #666; margin-bottom: 10px;">Debug: ${allRowsFor.length} total matches, ${rowsFor.length} non-bye matches, ${totalWins} wins, ${totalLosses} losses</p>
    <table>
      <thead>
        <tr>
          <th>Season</th>
          <th>Wins</th>
          <th>Losses</th>
          <th>Wins (Fall)</th>
          <th>Losses (Fall)</th>
          <th>Win %</th>
          <th>Avg Opp Elo</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
      </tbody>
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
    return wrap(`<div class="empty-state red"><h3>Select a wrestler to view opponent statistics</h3><p>Choose a wrestler from the dropdown above to see their head-to-head records against other wrestlers.</p></div>`);
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
    
    const isFall = String(match.decision_type || '').toLowerCase().includes('fall') || ['FALL', 'PIN'].includes(String(match.decision_type_code || '').toUpperCase());
    
    if (match.role === 'W' || match.role === 'winner') {
      stats.totalMatches++;
      stats.wins++;
      if (isFall) stats.winsFall++;
    } else if (match.role === 'L' || match.role === 'loser') {
      stats.totalMatches++;
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
        // For totalMatches, use wins as tiebreaker, then avgPreElo
        if (column === 'totalMatches') {
          const winsDiff = b.wins - a.wins;
          if (winsDiff !== 0) {
            return winsDiff; // Return directly, don't apply direction
          }
          const eloDiff = b.avgPreElo - a.avgPreElo;
          if (eloDiff !== 0) {
            return eloDiff; // Return directly, don't apply direction
          }
          // Fallback to formatted name for stable sort
          return formatWrestlerName(a.name).localeCompare(formatWrestlerName(b.name));
        } else {
          // Fallback to formatted name for stable sort
          comparison = formatWrestlerName(a.name).localeCompare(formatWrestlerName(b.name));
        }
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

## Upsets

```js
// Calculate biggest upset wins and losses for the selected wrestler
(() => {
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  if (!activeWrestler) {
    return wrap(`<div class="empty-state red"><h3>Select a wrestler to view upsets</h3><p>Choose a wrestler from the dropdown above to see their biggest upset wins and losses.</p></div>`);
  }
  
  const rowsFor = elo_history.filter(d => d.name === activeWrestler);
  if (!rowsFor.length) {
    return wrap(`<div class="empty-state"><h3>No upset data available</h3><p>No match history found for ${formatWrestlerName(activeWrestler)}.</p></div>`);
  }
  
  // Calculate upsets (elo gains and losses)
  const upsets = rowsFor
    .map(d => ({
      opponent_name: d.opponent_name,
      opponent_team: d.opponent_team,
      tournament: d.tournament_name ?? d.event_id,
      date: parseDate(d.start_date_iso ?? d.start_date),
      decision_type: d.decision_type,
      role: d.role,
      pre_elo: toNum(d.pre_elo),
      post_elo: toNum(d.post_elo),
      elo_change: toNum(d.post_elo) - toNum(d.pre_elo)
    }))
    .filter(d => d.date && Number.isFinite(d.elo_change));
  
  // Top 5 upset wins (biggest positive elo changes)
  const upsetWins = upsets
    .filter(d => d.elo_change > 0)
    .sort((a, b) => b.elo_change - a.elo_change)
    .slice(0, 5);
  
  // Top 5 upset losses (biggest negative elo changes)
  const upsetLosses = upsets
    .filter(d => d.elo_change < 0)
    .sort((a, b) => a.elo_change - b.elo_change)
    .slice(0, 5);
  
  const formatDate = (date) => date ? date.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }) : '-';
  
  const winsRows = upsetWins.map((d, i) => `
    <tr>
      <td>${i + 1}</td>
      <td>${createWrestlerLink(d.opponent_name)}${d.opponent_team ? ` (${d.opponent_team})` : ''}</td>
      <td>${d.decision_type || '-'}</td>
      <td>+${Math.round(d.elo_change)}</td>
      <td>${d.tournament || '-'}</td>
      <td>${formatDate(d.date)}</td>
    </tr>
  `).join("");
  
  const lossesRows = upsetLosses.map((d, i) => `
    <tr>
      <td>${i + 1}</td>
      <td>${createWrestlerLink(d.opponent_name)}${d.opponent_team ? ` (${d.opponent_team})` : ''}</td>
      <td>${d.decision_type || '-'}</td>
      <td>${Math.round(d.elo_change)}</td>
      <td>${d.tournament || '-'}</td>
      <td>${formatDate(d.date)}</td>
    </tr>
  `).join("");
  
  const winsTable = upsetWins.length > 0 ? `
    <h3>Biggest Upset Wins</h3>
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Opponent</th>
          <th>Result</th>
          <th>Elo Gain</th>
          <th>Tournament</th>
          <th>Date</th>
        </tr>
      </thead>
      <tbody>
        ${winsRows}
      </tbody>
    </table>
  ` : '<h3>Biggest Upset Wins</h3><p><em>No upset wins found.</em></p>';
  
  const lossesTable = upsetLosses.length > 0 ? `
    <h3>Biggest Upset Losses</h3>
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Opponent</th>
          <th>Result</th>
          <th>Elo Loss</th>
          <th>Tournament</th>
          <th>Date</th>
        </tr>
      </thead>
      <tbody>
        ${lossesRows}
      </tbody>
    </table>
  ` : '<h3>Biggest Upset Losses</h3><p><em>No upset losses found.</em></p>';
  
  return wrap(`
    <div>
      ${winsTable}
      <br>
      ${lossesTable}
    </div>
  `);
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
    decision_type: d.decision_type,
  }))
  .filter(d => d.date && Number.isFinite(d.seq) && Number.isFinite(d.post_elo))
  .sort((a, b) => a.date - b.date);

// Aggregated series by date for comparison chart (keep last Elo of each day)
const seriesByDate = !activeWrestler ? [] : (() => {
  const byDate = new Map();
  for (const d of series) {
    const dateStr = d.date.toISOString().split('T')[0];
    if (!byDate.has(dateStr) || d.seq > byDate.get(dateStr).seq) {
      byDate.set(dateStr, d);
    }
  }
  return Array.from(byDate.values()).sort((a, b) => a.date - b.date);
})();
```

## Elo Ratings Over Time
[What is Elo rating?](#what-is-elo-rating)

```js
// Generate unique ticks showing one label per date, with simple bucketed thinning
const dateTicksForSeries = (() => {
  if (!series.length) return [];
  const seenDates = new Map();
  for (const d of series) {
    const dateStr = d.date.toISOString().split('T')[0];
    if (!seenDates.has(dateStr)) {
      seenDates.set(dateStr, d.seq);
    }
  }
  const allTicks = Array.from(seenDates.values());
  
  // Simple bucketed thinning
  const count = allTicks.length;
  let step = 1;
  if (count >= 100) step = 5;
  else if (count >= 50) step = 3;
  else if (count >= 20) step = 2;
  
  return step === 1 ? allTicks : allTicks.filter((_, i) => i % step === 0);
})();
```

```js
// Line chart of Elo over time for the selected wrestler
!activeWrestler ? (() => {
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  return wrap(`<div class="empty-state red"><h3>Choose a wrestler to view their Elo rating history</h3><p>Select a wrestler from the dropdown above to see their Elo rating progression over time.</p></div>`);
})() : Plot.plot({
  width: 900,
  height: 420,
  marginLeft: 48,
  marginBottom: 50,
  x: {
    label: "Date",
    ticks: dateTicksForSeries,
    tickFormat: (seq) => {
      const match = series.find(d => d.seq === seq);
      return match ? match.date.toLocaleDateString('en-US', {month: 'short', day: 'numeric', year: '2-digit'}) : '';
    },
    tickRotate: -45
  },
  y: {label: `Elo (${formatWrestlerName(activeWrestler)})`, grid: true},
  marks: [
  Plot.line(series, {x: "seq", y: "post_elo", curve: "step-after"}),
    Plot.dot(series, {
      x: "seq",
      y: "post_elo",
      tip: true,
      title: d => {
        const ds = d.date ? d.date.toLocaleDateString?.() ?? String(d.date) : "";
        const winLoss = (d.role === 'W' || d.role === 'winner') ? 'Win' : 'Loss';
        const decisionType = d.decision_type || 'Unknown';
        const outcome = `${winLoss} by ${decisionType}`;
        const eloChange = Math.round(d.post_elo - d.pre_elo);
        const eloChangeStr = eloChange >= 0 ? `+${eloChange}` : `${eloChange}`;
        return `${formatWrestlerName(activeWrestler)}\n${d.tournament} — ${d.round}\nvs ${formatWrestlerName(d.opponent)}\nDate: ${ds}\nOutcome: ${outcome}\nElo: ${Math.round(d.post_elo)} (${eloChangeStr})`
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
// Prepare overlay data based on display scope selection - aggregate by date
const allWrestlerData = !activeWrestler ? [] : (() => {
  let filteredData = elo_history
    .map(d => ({ 
      name: d.name, 
      date: parseDate(d.start_date_iso ?? d.start_date),
      post_elo: toNum(d.post_elo),
      seq: toNum(d.elo_sequence)
    }))
    .filter(d => d.name && d.date && Number.isFinite(d.post_elo))
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
  
  // Aggregate by wrestler and date: keep the last Elo value for each wrestler-date combination
  const aggregated = new Map();
  for (const d of filteredData) {
    const dateStr = d.date.toISOString().split('T')[0]; // YYYY-MM-DD
    const key = `${d.name}|${dateStr}`;
    if (!aggregated.has(key) || d.seq > aggregated.get(key).seq) {
      aggregated.set(key, d);
    }
  }
  
  return Array.from(aggregated.values());
})();
```

```js
// Split into opponents and non-opponents based on display scope
const opponentSeries = (() => {
  if (displayScope === 'Opponents') {
    // When showing only opponents, all data should be styled as opponents
    return allWrestlerData.sort((a, b) => {
      const n = formatWrestlerName(a.name).localeCompare(formatWrestlerName(b.name));
      return n !== 0 ? n : (a.date - b.date);
    });
  } else {
    // For other scopes, highlight actual opponents
    return allWrestlerData
      .filter(d => opponentNames.has(d.name))
      .sort((a, b) => {
        const n = formatWrestlerName(a.name).localeCompare(formatWrestlerName(b.name));
        return n !== 0 ? n : (a.date - b.date);
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
        return n !== 0 ? n : (a.date - b.date);
      });
  }
})();
```

```js
// Endpoint of the selected series for labeling
const selectedEnd = seriesByDate.length ? seriesByDate[seriesByDate.length - 1] : null;
```

```js
// Render overlay chart (guard against empty data) with date-based x-axis
!activeWrestler ? (() => {
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  return wrap(`<div class="empty-state red"><h3>Choose a wrestler to view Elo comparison</h3><p>Select a wrestler from the dropdown above to compare their Elo progression with other wrestlers.</p></div>`);
})() : (opponentSeries.length || nonOpponentSeries.length || seriesByDate.length)
  ? (() => {
      // Create ticks showing only first date of each month
      const monthTicks = (() => {
        const allDates = new Set();
        if (seriesByDate.length) seriesByDate.forEach(d => allDates.add(d.date));
        if (opponentSeries.length) opponentSeries.forEach(d => allDates.add(d.date));
        if (nonOpponentSeries.length) nonOpponentSeries.forEach(d => allDates.add(d.date));
        
        const sortedDates = Array.from(allDates).sort((a, b) => a - b);
        const seenMonths = new Set();
        const firstOfMonth = [];
        
        for (const date of sortedDates) {
          const monthKey = `${date.getFullYear()}-${date.getMonth()}`;
          if (!seenMonths.has(monthKey)) {
            seenMonths.add(monthKey);
            firstOfMonth.push(date);
          }
        }
        
        return firstOfMonth;
      })();
      
      const chart = Plot.plot({
        width: 900,
        height: 380,
        marginLeft: 48,
        marginBottom: 50,
        x: {type: "point", label: "Date", ticks: monthTicks, tickFormat: d => d.toLocaleDateString('en-US', {month: 'short', day: 'numeric', year: '2-digit'}), tickRotate: -45},
        y: {label: "Elo", grid: true},
        marks: [
          // Background: non-opponent wrestlers, very faint lines
          Plot.line(nonOpponentSeries, {
            x: "date",
            y: "post_elo",
            z: "name",
            stroke: "#ccc",
            strokeOpacity: 0.12,
            strokeWidth: 1,
            curve: "step-after",
            tip: displayScope !== `All ${observable.params.gov_body.toUpperCase()} Wrestlers`,
            title: displayScope !== `All ${observable.params.gov_body.toUpperCase()} Wrestlers` ? (d => {
              return `${formatWrestlerName(d.name)}\nDate: ${d.date.toLocaleDateString()}\nElo: ${Math.round(d.post_elo)}`;
            }) : undefined
          }),
          // Highlighted: opponent wrestlers or all wrestlers in opponents-only view
          Plot.line(opponentSeries, {
            x: "date",
            y: "post_elo",
            z: "name",
            stroke: "#ff7f0e",
            strokeOpacity: 0.35,
            strokeWidth: 1.5,
            curve: "step-after",
            tip: displayScope !== `All ${observable.params.gov_body.toUpperCase()} Wrestlers`,
            title: displayScope !== `All ${observable.params.gov_body.toUpperCase()} Wrestlers` ? (d => {
              const suffix = displayScope === 'Opponents' ? '' : ' (opponent)';
              return `${formatWrestlerName(d.name)}${suffix}\nDate: ${d.date.toLocaleDateString()}\nElo: ${Math.round(d.post_elo)}`;
            }) : undefined
          }),
          // Halo for selected wrestler line (drawn after background, before foreground)
          Plot.line(seriesByDate, {
            x: "date",
            y: "post_elo",
            stroke: "white",
            strokeOpacity: 0.9,
            strokeWidth: 6,
            curve: "step-after"
          }),
          // Foreground selected wrestler line
          Plot.line(seriesByDate, {
            x: "date",
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
                  x: "date",
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


