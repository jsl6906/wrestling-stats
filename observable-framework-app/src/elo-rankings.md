# NVWF Individual Wrestler Statistics

```js
const elo_history_parquet = FileAttachment("./data/elo_history.parquet").parquet();
const wrestlers_parquet = FileAttachment("./data/wrestlers.parquet").parquet();
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
// Helper function to create wrestler links
function createWrestlerLink(wrestlerName) {
  if (!wrestlerName) return wrestlerName || "-";
  const url = new URL(window.location);
  url.searchParams.set('wrestler', wrestlerName);
  return `<a href="${url.href}" target="_blank" rel="noopener noreferrer">${wrestlerName}</a>`;
}
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
)).sort((a, b) => String(a).localeCompare(String(b)));
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
    names[0];
  const control = select(names, {label: "Wrestler", value: initial});
  // Ensure control reflects initial pick even if Inputs reuses DOM
  control.value = initial;
  // Persist initial selection immediately
  globalThis.__wrestlerSelected = initial;
  cache[teamKey] = initial;
  setUrlParam('wrestler', initial);
  const persist = () => {
    const v = control.value;
    globalThis.__wrestlerSelected = v;
    cache[teamKey] = v;
    setUrlParam('wrestler', v);
  };
  control.addEventListener("input", persist);
  control.addEventListener("change", persist);
  return view(control);
})();
```

```js
// Ensure a valid selection if team filter changes
const activeWrestler = (() => {
  const selected = names.includes(wrestler) ? wrestler : names[0];
  // Update URL if we had to fall back to a different wrestler
  if (selected !== wrestler) {
    setUrlParam('wrestler', selected);
  }
  return selected;
})();
```

```js
// Selected wrestler summary card (DOM)
(() => {
  const w = Array.isArray(wrestlers) ? wrestlers.find(d => d.name === activeWrestler) : null;
  const fmt = (n) => Number.isFinite(+n) ? Math.round(+n) : "-";
  const fmt1 = (n) => Number.isFinite(+n) ? (+n).toFixed(1) : "-";
  const dateStr = (v) => v ? (new Date(v)).toLocaleDateString?.() ?? String(v) : "-";
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  if (!w) return wrap(`<p>No summary available.</p>`);
  // Fallback derivations if wrestlers.parquet is missing some fields
  const rowsFor = elo_history.filter(d => d.name === activeWrestler);
  const winsDerived = rowsFor.filter(d => d.role === 'winner').length;
  const lossesDerived = rowsFor.filter(d => d.role === 'loser').length;
  const winsFallDerived = rowsFor.filter(d => d.role === 'winner' && String(d.decision_type || '').toLowerCase() === 'fall').length;
  const lossesFallDerived = rowsFor.filter(d => d.role === 'loser' && String(d.decision_type || '').toLowerCase() === 'fall').length;
  const oppAvgDerived = (() => {
    const vals = rowsFor.map(d => Number(d.opponent_pre_elo)).filter(Number.isFinite);
    return vals.length ? (vals.reduce((a,b) => a + b, 0) / vals.length) : null;
  })();
  const wins = ('wins' in w) ? Number(w.wins ?? 0) : winsDerived;
  const wins_fall = ('wins_fall' in w) ? Number(w.wins_fall ?? 0) : winsFallDerived;
  const losses = ('losses' in w) ? Number(w.losses ?? 0) : lossesDerived;
  const losses_fall = ('losses_fall' in w) ? Number(w.losses_fall ?? 0) : lossesFallDerived;
  const opponent_avg_elo = ('opponent_avg_elo' in w) ? Number(w.opponent_avg_elo) : oppAvgDerived;
  const denom = (wins + losses);
  const win_pct = denom > 0 ? (wins / denom) * 100 : null;
  // Arrange: Team at top; general stats next; Elo-related grouped at bottom
  const generalRows = [
    ["Team", w.last_team ?? "-"],
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
    ["Last Adj", fmt(w.last_adjustment)]
  ];
  const generalPart = generalRows.map(([k,v]) => `<tr><th>${k}</th><td>${v}</td></tr>`).join("");
  const eloHeader = `<tr><th colspan="2">Elo</th></tr>`;
  const eloPart = eloRows.map(([k,v]) => `<tr><th>${k}</th><td>${v}</td></tr>`).join("");
  const tableRows = generalPart + eloHeader + eloPart;
  return wrap(`
    <div>
      <h3>${w.name}</h3>
      <table>
        ${tableRows}
      </table>
    </div>
  `);
})()
```

```js
// Opponent summary table
(() => {
  const rowsFor = elo_history.filter(d => d.name === activeWrestler);
  if (!rowsFor.length) return null;
  
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
    
    if (match.role === 'W' || match.role === 'winner') {
      stats.wins++;
    } else if (match.role === 'L' || match.role === 'loser') {
      stats.losses++;
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
  
  // Calculate averages and sort by total matches desc, then by name
  const opponents = Array.from(opponentStats.values())
    .map(stats => ({
      ...stats,
      avgPreElo: stats.totalMatches > 0 ? stats.eloSum / stats.totalMatches : 0,
      avgPostElo: stats.totalMatches > 0 ? stats.postEloSum / stats.totalMatches : 0,
      winPct: stats.totalMatches > 0 ? (stats.wins / stats.totalMatches) * 100 : 0
    }))
    .sort((a, b) => b.totalMatches - a.totalMatches || a.name.localeCompare(b.name));
  
  if (!opponents.length) return null;
  
  const fmt = (n) => Number.isFinite(+n) ? Math.round(+n) : "-";
  const fmt1 = (n) => Number.isFinite(+n) ? (+n).toFixed(1) : "-";
  const dateStr = (v) => v ? v.toLocaleDateString?.() ?? String(v) : "-";
  
  const rows = opponents.map(opp => `
    <tr>
      <td>${createWrestlerLink(opp.name)}</td>
      <td>${opp.totalMatches}</td>
      <td>${opp.wins}</td>
      <td>${opp.losses}</td>
      <td>${fmt1(opp.winPct)}%</td>
      <td>${fmt(opp.avgPreElo)}</td>
      <td>${dateStr(opp.lastDate)}</td>
    </tr>
  `).join("");
  
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  return wrap(`
    <div>
      <h3>Opponent Statistics</h3>
      <table>
        <thead>
          <tr>
            <th>Opponent</th>
            <th>Matches</th>
            <th>Wins</th>
            <th>Losses</th>
            <th>Win %</th>
            <th>Avg Opp Elo</th>
            <th>Last Match</th>
          </tr>
        </thead>
        <tbody>
          ${rows}
        </tbody>
      </table>
    </div>
  `);
})()
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
const series = elo_history
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

### Elo Ratings Over Time ([What is Elo rating?](#what-is-elo-rating))
```js
// Line chart of Elo over time for the selected wrestler
Plot.plot({
  width: 900,
  height: 420,
  marginLeft: 48,
  x: {axis: null},
  y: {label: `Elo (${activeWrestler})`, grid: true},
  marks: [
  Plot.line(series, {x: "seq", y: "post_elo", curve: "step-after"}),
    Plot.dot(series, {
      x: "seq",
      y: "post_elo",
      tip: true,
      title: d => {
        const ds = d.date ? d.date.toLocaleDateString?.() ?? String(d.date) : "";
        return `${activeWrestler}\n${d.tournament} — ${d.round}\nvs ${d.opponent}\nDate: ${ds}\nElo: ${Math.round(d.post_elo)}`
      }
    })
  ]
})
```

```js
// Optional: show a small table of the last 10 matches
display(wrestlers.find(d => d.name === activeWrestler))
display(series)
```

```js
// Dynamic comparison heading
(() => {
  const h = document.createElement("h2");
  h.textContent = teamFilter === "All teams" ? "All wrestlers comparison" : `Team comparison: ${teamFilter}`;
  return h;
})()
```

```js
// Get list of opponents for the selected wrestler
const opponentNames = new Set(
  elo_history
    .filter(d => d.name === activeWrestler && d.opponent_name)
    .map(d => d.opponent_name)
);
```

```js
// Prepare overlay data: separate opponents from non-opponents for different styling
const allWrestlerData = elo_history
  .map(d => ({ name: d.name, seq: toNum(d.elo_sequence), post_elo: toNum(d.post_elo) }))
  .filter(d => d.name && Number.isFinite(d.seq) && Number.isFinite(d.post_elo))
  .filter(d => teamFilter === "All teams" || (nameToTeam.get(d.name) ?? null) === teamFilter)
  .filter(d => d.name !== activeWrestler); // Exclude the selected wrestler from background

// Split into opponents and non-opponents
const opponentSeries = allWrestlerData
  .filter(d => opponentNames.has(d.name))
  .sort((a, b) => {
    const n = String(a.name).localeCompare(String(b.name));
    return n !== 0 ? n : (a.seq - b.seq);
  });

const nonOpponentSeries = allWrestlerData
  .filter(d => !opponentNames.has(d.name))
  .sort((a, b) => {
    const n = String(a.name).localeCompare(String(b.name));
    return n !== 0 ? n : (a.seq - b.seq);
  });
```

```js
// Endpoint of the selected series for labeling
const selectedEnd = series.length ? series[series.length - 1] : null;
```

```js
// Render overlay chart (guard against empty data)
(opponentSeries.length || nonOpponentSeries.length || series.length)
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
              return `${d.name}\nClick to view: ${url.href}`;
            }
          }),
          // Highlighted: opponent wrestlers, more prominent
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
              return `${d.name} (opponent)\nClick to view: ${url.href}`;
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
                  text: () => activeWrestler,
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
