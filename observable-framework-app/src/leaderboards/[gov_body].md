# ${observable.params.gov_body.toUpperCase()} Leaderboards

```js
const individual_data = FileAttachment(`data/individual_leaderboards_${observable.params.gov_body}.parquet`).parquet();
const team_data = FileAttachment(`data/team_leaderboards_${observable.params.gov_body}.parquet`).parquet();
```

```js
// Calculate date range and total matches across all data
const dataStats = (() => {
  const individuals = Array.from(individual_data);
  
  // Collect all dates from upset_date field
  const dates = individuals
    .map(d => d.upset_date)
    .filter(Boolean)
    .map(d => new Date(d))
    .filter(d => !isNaN(d))
    .sort((a, b) => a - b);
  
  // Calculate total matches across all individual records
  const totalMatches = individuals.reduce((sum, d) => sum + (Number(d.matches_played) || 0), 0);
  
  return {
    minDate: dates.length > 0 ? dates[0] : null,
    maxDate: dates.length > 0 ? dates[dates.length - 1] : null,
    totalMatches
  };
})();
```

```js
// Display date range and total matches subtitle
(() => {
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  if (!dataStats.minDate || !dataStats.maxDate) {
    return wrap(`<p style="font-size: 0.9em; font-style: italic; margin: 0 0 20px 0;">No match data available</p>`);
  }
  
  const formatDate = (date) => date.toLocaleDateString('en-US', { 
    year: 'numeric', 
    month: 'long', 
    day: 'numeric' 
  });
  
  return wrap(`<p style="font-size: 0.9em; font-style: italic; margin: 0 0 20px 0;">Data coverage: ${formatDate(dataStats.minDate)} to ${formatDate(dataStats.maxDate)} • ${dataStats.totalMatches.toLocaleString()} total matches</p>`);
})()
```

```js
const individuals = Array.from(individual_data);
const teams = Array.from(team_data);
```

```js
import {select} from "npm:@observablehq/inputs";
import * as Plot from "npm:@observablehq/plot";
import {FileAttachment, view} from "observablehq:stdlib";
```

```js
// Get unique seasons for filtering
const seasons = ["All Seasons", ...Array.from(new Set(individuals.map(d => d.season))).sort().reverse()];
const selectedSeason = view(select(seasons, {label: "Season", value: seasons[0]}));
```

```js
// Get unique teams for filtering
const teamOptions = ["All Teams", ...Array.from(new Set(individuals.map(d => d.team).filter(Boolean))).sort()];
const selectedTeam = view(select(teamOptions, {label: "Team", value: teamOptions[0]}));
```

```js
// Helper function to get top N with ties (extends beyond N if there are ties at the cutoff)
function getTopWithTies(data, n, sortFn) {
  const sorted = [...data].sort(sortFn);
  if (sorted.length <= n) return sorted;
  
  const cutoffValue = sortFn(sorted[n - 1], sorted[n]);
  if (cutoffValue === 0) {
    // There's a tie at position n, find where ties end
    let endIndex = n;
    while (endIndex < sorted.length && sortFn(sorted[n - 1], sorted[endIndex]) === 0) {
      endIndex++;
    }
    return sorted.slice(0, endIndex);
  }
  return sorted.slice(0, n);
}

// Helper function to assign ranks with ties
function assignRanks(data, compareFn) {
  let rank = 1;
  let prevValue = null;
  let sameRankCount = 0;
  
  return data.map((item, index) => {
    if (index > 0 && compareFn(data[index - 1], item) !== 0) {
      rank += sameRankCount + 1;
      sameRankCount = 0;
    } else if (index > 0) {
      sameRankCount++;
    }
    return { ...item, rank };
  });
}
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
```

```js
// Helper function to create wrestler links
function createWrestlerLink(wrestlerName, govBody) {
  if (!wrestlerName) return wrestlerName || "-";
  const baseUrl = window.location.origin + window.location.pathname.replace(/leaderboards\/.*$/, 'individual_stats/');
  const url = `${baseUrl}${govBody}?wrestler=${encodeURIComponent(wrestlerName)}`;
  const displayName = formatWrestlerName(wrestlerName);
  return `<a href="${url}" target="_blank" rel="noopener noreferrer">${displayName}</a>`;
}
```

```js
// Helper function to convert BigInt values to numbers
function convertBigInts(obj) {
  const converted = {};
  for (const [key, value] of Object.entries(obj)) {
    converted[key] = typeof value === 'bigint' ? Number(value) : value;
  }
  return converted;
}

// Filter data by season and team
const filteredIndividuals = (() => {
  let filtered = selectedSeason === "All Seasons" 
    ? individuals 
    : individuals.filter(d => d.season === selectedSeason).map(convertBigInts);
  
  // Apply team filter
  if (selectedTeam !== "All Teams") {
    filtered = filtered.filter(d => d.team === selectedTeam);
  }
  
  return filtered;
})();

const filteredTeams = selectedSeason === "All Seasons"
  ? teams
  : teams.filter(d => d.season === selectedSeason).map(convertBigInts);
```

```js
// Helper to aggregate "All Seasons" data
function aggregateByWrestler(data) {
  const byWrestler = new Map();
  
  for (const row of data) {
    const key = row.name;
    if (!byWrestler.has(key)) {
      byWrestler.set(key, {
        name: row.name,
        team: row.team,
        season: "All Seasons",
        matches_played: 0,
        wins: 0,
        losses: 0,
        wins_fall: 0,
        highest_elo: 0,
        biggest_upset_win: 0,
        upset_event_id: null,
        upset_date: null,
        upset_tournament_name: null
      });
    }
    
    const agg = byWrestler.get(key);
    agg.matches_played += Number(row.matches_played) || 0;
    agg.wins += Number(row.wins) || 0;
    agg.losses += Number(row.losses) || 0;
    agg.wins_fall += Number(row.wins_fall) || 0;
    agg.highest_elo = Math.max(agg.highest_elo, Number(row.highest_elo) || 0);
    
    // Track biggest upset and its details
    const upsetValue = Number(row.biggest_upset_win) || 0;
    if (upsetValue > agg.biggest_upset_win) {
      agg.biggest_upset_win = upsetValue;
      agg.upset_event_id = row.upset_event_id;
      agg.upset_date = row.upset_date;
      agg.upset_tournament_name = row.upset_tournament_name;
      agg.upset_opponent_name = row.upset_opponent_name;
      agg.upset_opponent_team = row.upset_opponent_team;
      agg.upset_result = row.upset_result;
    }
  }
  
  // Calculate percentages
  return Array.from(byWrestler.values()).map(d => ({
    ...d,
    win_pct: d.matches_played > 0 ? (d.wins / d.matches_played) * 100 : 0,
    fall_pct: d.wins > 0 ? (d.wins_fall / d.wins) * 100 : 0
  }));
}

function aggregateByTeam(data) {
  const byTeam = new Map();
  
  for (const row of data) {
    const key = row.team;
    if (!byTeam.has(key)) {
      byTeam.set(key, {
        team: row.team,
        season: "All Seasons",
        matches_played: 0,
        wins: 0,
        losses: 0,
        wins_fall: 0
      });
    }
    
    const agg = byTeam.get(key);
    agg.matches_played += Number(row.matches_played) || 0;
    agg.wins += Number(row.wins) || 0;
    agg.losses += Number(row.losses) || 0;
    agg.wins_fall += Number(row.wins_fall) || 0;
  }
  
  // Calculate percentages
  return Array.from(byTeam.values()).map(d => ({
    ...d,
    win_pct: d.matches_played > 0 ? (d.wins / d.matches_played) * 100 : 0,
    fall_pct: d.wins > 0 ? (d.wins_fall / d.wins) * 100 : 0
  }));
}
```

```js
// Get the appropriate data based on season and team selection
const individualStats = (() => {
  let stats = selectedSeason === "All Seasons" 
    ? aggregateByWrestler(individuals)
    : filteredIndividuals;
  
  // Apply team filter to aggregated or filtered data
  if (selectedTeam !== "All Teams") {
    stats = stats.filter(d => d.team === selectedTeam);
  }
  
  return stats;
})();

const teamStats = selectedSeason === "All Seasons"
  ? aggregateByTeam(teams)
  : filteredTeams;
```

## Individual Leaderboards

### Top 10 by Matches Played

```js
(() => {
  const top10 = individualStats
    .sort((a, b) => (b.matches_played || 0) - (a.matches_played || 0))
    .slice(0, 10);
  
  const tableRows = top10.map((d, i) => `
    <tr>
      <td>${i + 1}</td>
      <td>${createWrestlerLink(d.name, observable.params.gov_body)}</td>
      <td>${d.team || '-'}</td>
      <td>${d.wins || 0}-${d.losses || 0}</td>
      <td>${d.matches_played || 0}</td>
    </tr>
  `).join("");
  
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  return wrap(`
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Wrestler</th>
          <th>Team</th>
          <th>Record</th>
          <th>Matches</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
      </tbody>
    </table>
  `);
})()
```

### Top 10 by Total Wins

```js
(() => {
  const top10 = individualStats
    .sort((a, b) => (b.wins || 0) - (a.wins || 0))
    .slice(0, 10);
  
  const tableRows = top10.map((d, i) => `
    <tr>
      <td>${i + 1}</td>
      <td>${createWrestlerLink(d.name, observable.params.gov_body)}</td>
      <td>${d.team || '-'}</td>
      <td>${d.wins || 0}</td>
      <td>${d.wins || 0}-${d.losses || 0}</td>
      <td>${d.matches_played || 0}</td>
    </tr>
  `).join("");
  
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  return wrap(`
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Wrestler</th>
          <th>Team</th>
          <th>Wins</th>
          <th>Record</th>
          <th>Total Matches</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
      </tbody>
    </table>
  `);
})()
```

### Top 10 by Win Percentage
*Minimum 10 matches*

```js
(() => {
  const filtered = individualStats.filter(d => (d.matches_played || 0) >= 10);
  const sortFn = (a, b) => {
    const pctDiff = (b.win_pct || 0) - (a.win_pct || 0);
    if (pctDiff !== 0) return pctDiff;
    return (b.wins || 0) - (a.wins || 0); // Tiebreaker: more wins
  };
  const top = getTopWithTies(filtered, 10, sortFn);
  const ranked = assignRanks(top, (a, b) => {
    const pctDiff = (b.win_pct || 0) - (a.win_pct || 0);
    if (pctDiff !== 0) return pctDiff;
    return (b.wins || 0) - (a.wins || 0);
  });
  
  const tableRows = ranked.map(d => `
    <tr>
      <td>${d.rank}</td>
      <td>${createWrestlerLink(d.name, observable.params.gov_body)}</td>
      <td>${d.team || '-'}</td>
      <td>${(d.win_pct || 0).toFixed(1)}%</td>
      <td>${d.wins || 0}-${d.losses || 0}</td>
      <td>${d.matches_played || 0}</td>
    </tr>
  `).join("");
  
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  return wrap(`
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Wrestler</th>
          <th>Team</th>
          <th>Win %</th>
          <th>Record</th>
          <th>Total Matches</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
      </tbody>
    </table>
  `);
})()
```

### Top 10 by Fall Percentage
*Minimum 10 matches and 1 fall*

```js
(() => {
  const filtered = individualStats.filter(d => (d.matches_played || 0) >= 10 && (d.wins_fall || 0) >= 1);
  
  if (filtered.length === 0) {
    const wrap = (html) => {
      const div = document.createElement("div");
      div.innerHTML = html.trim();
      return div.firstChild || div;
    };
    return wrap(`<p><em>No wrestlers meet the minimum criteria (10 matches and 1 fall).</em></p>`);
  }
  
  const sortFn = (a, b) => {
    const pctDiff = (b.fall_pct || 0) - (a.fall_pct || 0);
    if (pctDiff !== 0) return pctDiff;
    return (b.wins_fall || 0) - (a.wins_fall || 0); // Tiebreaker: more falls
  };
  const top = getTopWithTies(filtered, 10, sortFn);
  const ranked = assignRanks(top, (a, b) => {
    const pctDiff = (b.fall_pct || 0) - (a.fall_pct || 0);
    if (pctDiff !== 0) return pctDiff;
    return (b.wins_fall || 0) - (a.wins_fall || 0);
  });
  
  const tableRows = ranked.map(d => `
    <tr>
      <td>${d.rank}</td>
      <td>${createWrestlerLink(d.name, observable.params.gov_body)}</td>
      <td>${d.team || '-'}</td>
      <td>${(d.fall_pct || 0).toFixed(1)}%</td>
      <td>${d.wins_fall || 0}</td>
      <td>${d.wins || 0}</td>
      <td>${d.wins || 0}-${d.losses || 0}</td>
    </tr>
  `).join("");
  
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  return wrap(`
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Wrestler</th>
          <th>Team</th>
          <th>Fall %</th>
          <th>Falls</th>
          <th>Total Wins</th>
          <th>Record</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
      </tbody>
    </table>
  `);
})()
```

### Top 10 by Highest Elo Rating
[What is Elo rating?](#what-is-elo-rating)

```js
(() => {
  const top10 = individualStats
    .sort((a, b) => (b.highest_elo || 0) - (a.highest_elo || 0))
    .slice(0, 10);
  
  const tableRows = top10.map((d, i) => `
    <tr>
      <td>${i + 1}</td>
      <td>${createWrestlerLink(d.name, observable.params.gov_body)}</td>
      <td>${d.team || '-'}</td>
      <td>${Math.round(d.highest_elo || 0)}</td>
      <td>${d.wins || 0}-${d.losses || 0}</td>
      <td>${d.matches_played || 0}</td>
    </tr>
  `).join("");
  
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  return wrap(`
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Wrestler</th>
          <th>Team</th>
          <th>Highest Elo</th>
          <th>Record</th>
          <th>Total Matches</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
      </tbody>
    </table>
  `);
})()
```

### Top 10 Biggest Upset Winners
*Largest single-match Elo gain*

```js
(() => {
  const top10 = individualStats
    .filter(d => (d.biggest_upset_win || 0) > 0)
    .sort((a, b) => (b.biggest_upset_win || 0) - (a.biggest_upset_win || 0))
    .slice(0, 10);
  
  const tableRows = top10.map((d, i) => `
    <tr>
      <td>${i + 1}</td>
      <td>${createWrestlerLink(d.name, observable.params.gov_body)}${d.team ? ` (${d.team})` : ''}</td>
      <td>${d.upset_opponent_name ? createWrestlerLink(d.upset_opponent_name, observable.params.gov_body) + (d.upset_opponent_team ? ` (${d.upset_opponent_team})` : '') : '-'}</td>
      <td>${d.upset_result || '-'}</td>
      <td>+${Math.round(d.biggest_upset_win || 0)}</td>
      <td>${d.upset_tournament_name || '-'}</td>
      <td>${d.upset_date ? new Date(d.upset_date).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }) : '-'}</td>
    </tr>
  `).join("");
  
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  return wrap(`
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Wrestler</th>
          <th>Opponent</th>
          <th>Result</th>
          <th>Elo Gain</th>
          <th>Tournament</th>
          <th>Date</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
      </tbody>
    </table>
  `);
})()
```

## Team Leaderboards

### Top 10 Teams by Matches Played

```js
(() => {
  const sorted = teamStats
    .sort((a, b) => (b.matches_played || 0) - (a.matches_played || 0));
  const top10 = sorted.slice(0, 10);
  
  const selectedTeamInTop10 = selectedTeam !== "All Teams" && top10.some(d => d.team === selectedTeam);
  const selectedTeamData = selectedTeam !== "All Teams" && !selectedTeamInTop10 
    ? sorted.find(d => d.team === selectedTeam)
    : null;
  const selectedTeamRank = selectedTeamData 
    ? sorted.findIndex(d => d.team === selectedTeam) + 1
    : null;
  
  const createRow = (d, rank, isHighlight = false) => `
    <tr${isHighlight ? ' style="color: steelblue;"' : ''}>
      <td>${rank}</td>
      <td>${d.team || '-'}</td>
      <td>${d.wins || 0}-${d.losses || 0}</td>
      <td>${d.matches_played || 0}</td>
    </tr>
  `;
  
  const tableRows = top10.map((d, i) => 
    createRow(d, i + 1, selectedTeam !== "All Teams" && d.team === selectedTeam)
  ).join("");
  
  const extraRows = selectedTeamData ? `
    <tr><td colspan="4" style="border: none; padding: 8px 0;"></td></tr>
    ${createRow(selectedTeamData, selectedTeamRank, true)}
  ` : '';
  
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  return wrap(`
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Team</th>
          <th>Record</th>
          <th>Matches</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
        ${extraRows}
      </tbody>
    </table>
  `);
})()
```

### Top 10 Teams by Total Wins

```js
(() => {
  const sorted = teamStats
    .sort((a, b) => (b.wins || 0) - (a.wins || 0));
  const top10 = sorted.slice(0, 10);
  
  const selectedTeamInTop10 = selectedTeam !== "All Teams" && top10.some(d => d.team === selectedTeam);
  const selectedTeamData = selectedTeam !== "All Teams" && !selectedTeamInTop10 
    ? sorted.find(d => d.team === selectedTeam)
    : null;
  const selectedTeamRank = selectedTeamData 
    ? sorted.findIndex(d => d.team === selectedTeam) + 1
    : null;
  
  const createRow = (d, rank, isHighlight = false) => `
    <tr${isHighlight ? ' style="color: steelblue;"' : ''}>
      <td>${rank}</td>
      <td>${d.team || '-'}</td>
      <td>${d.wins || 0}</td>
      <td>${d.wins || 0}-${d.losses || 0}</td>
      <td>${d.matches_played || 0}</td>
    </tr>
  `;
  
  const tableRows = top10.map((d, i) => 
    createRow(d, i + 1, selectedTeam !== "All Teams" && d.team === selectedTeam)
  ).join("");
  
  const extraRows = selectedTeamData ? `
    <tr><td colspan="5" style="border: none; padding: 8px 0;"></td></tr>
    ${createRow(selectedTeamData, selectedTeamRank, true)}
  ` : '';
  
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  return wrap(`
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Team</th>
          <th>Wins</th>
          <th>Record</th>
          <th>Total Matches</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
        ${extraRows}
      </tbody>
    </table>
  `);
})()
```

### Top 10 Teams by Win Percentage
*Minimum 50 matches*

```js
(() => {
  const filtered = teamStats.filter(d => (d.matches_played || 0) >= 50);
  const sortFn = (a, b) => {
    const pctDiff = (b.win_pct || 0) - (a.win_pct || 0);
    if (pctDiff !== 0) return pctDiff;
    return (b.wins || 0) - (a.wins || 0); // Tiebreaker: more wins
  };
  const top = getTopWithTies(filtered, 10, sortFn);
  const ranked = assignRanks(top, (a, b) => {
    const pctDiff = (b.win_pct || 0) - (a.win_pct || 0);
    if (pctDiff !== 0) return pctDiff;
    return (b.wins || 0) - (a.wins || 0);
  });
  
  // Check for selected team
  const selectedTeamInTop = selectedTeam !== "All Teams" && ranked.some(d => d.team === selectedTeam);
  const selectedTeamData = selectedTeam !== "All Teams" && !selectedTeamInTop 
    ? filtered.find(d => d.team === selectedTeam)
    : null;
  
  // Rank the selected team if not in top
  let selectedTeamRank = null;
  if (selectedTeamData) {
    const allRanked = assignRanks([...filtered].sort(sortFn), (a, b) => {
      const pctDiff = (b.win_pct || 0) - (a.win_pct || 0);
      if (pctDiff !== 0) return pctDiff;
      return (b.wins || 0) - (a.wins || 0);
    });
    selectedTeamRank = allRanked.find(d => d.team === selectedTeam)?.rank;
  }
  
  const createRow = (d, isHighlight = false) => `
    <tr${isHighlight ? ' style="color: steelblue;"' : ''}>
      <td>${d.rank}</td>
      <td>${d.team || '-'}</td>
      <td>${(d.win_pct || 0).toFixed(1)}%</td>
      <td>${d.wins || 0}-${d.losses || 0}</td>
      <td>${d.matches_played || 0}</td>
    </tr>
  `;
  
  const tableRows = ranked.map(d => 
    createRow(d, selectedTeam !== "All Teams" && d.team === selectedTeam)
  ).join("");
  
  const extraRows = selectedTeamData && selectedTeamRank ? `
    <tr><td colspan="5" style="border: none; padding: 8px 0;"></td></tr>
    ${createRow({...selectedTeamData, rank: selectedTeamRank}, true)}
  ` : '';
  
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  return wrap(`
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Team</th>
          <th>Win %</th>
          <th>Record</th>
          <th>Total Matches</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
        ${extraRows}
      </tbody>
    </table>
  `);
})()
```

### Top 10 Teams by Fall Percentage
*Minimum 50 matches and 5 falls*

```js
(() => {
  const filtered = teamStats.filter(d => (d.matches_played || 0) >= 50 && (d.wins_fall || 0) >= 5);
  const sortFn = (a, b) => {
    const pctDiff = (b.fall_pct || 0) - (a.fall_pct || 0);
    if (pctDiff !== 0) return pctDiff;
    return (b.wins_fall || 0) - (a.wins_fall || 0); // Tiebreaker: more falls
  };
  const top = getTopWithTies(filtered, 10, sortFn);
  const ranked = assignRanks(top, (a, b) => {
    const pctDiff = (b.fall_pct || 0) - (a.fall_pct || 0);
    if (pctDiff !== 0) return pctDiff;
    return (b.wins_fall || 0) - (a.wins_fall || 0);
  });
  
  // Check for selected team
  const selectedTeamInTop = selectedTeam !== "All Teams" && ranked.some(d => d.team === selectedTeam);
  const selectedTeamData = selectedTeam !== "All Teams" && !selectedTeamInTop 
    ? filtered.find(d => d.team === selectedTeam)
    : null;
  
  // Rank the selected team if not in top
  let selectedTeamRank = null;
  if (selectedTeamData) {
    const allRanked = assignRanks([...filtered].sort(sortFn), (a, b) => {
      const pctDiff = (b.fall_pct || 0) - (a.fall_pct || 0);
      if (pctDiff !== 0) return pctDiff;
      return (b.wins_fall || 0) - (a.wins_fall || 0);
    });
    selectedTeamRank = allRanked.find(d => d.team === selectedTeam)?.rank;
  }
  
  const createRow = (d, isHighlight = false) => `
    <tr${isHighlight ? ' style="color: steelblue;"' : ''}>
      <td>${d.rank}</td>
      <td>${d.team || '-'}</td>
      <td>${(d.fall_pct || 0).toFixed(1)}%</td>
      <td>${d.wins_fall || 0}</td>
      <td>${d.wins || 0}</td>
      <td>${d.wins || 0}-${d.losses || 0}</td>
    </tr>
  `;
  
  const tableRows = ranked.map(d => 
    createRow(d, selectedTeam !== "All Teams" && d.team === selectedTeam)
  ).join("");
  
  const extraRows = selectedTeamData && selectedTeamRank ? `
    <tr><td colspan="6" style="border: none; padding: 8px 0;"></td></tr>
    ${createRow({...selectedTeamData, rank: selectedTeamRank}, true)}
  ` : '';
  
  const wrap = (html) => {
    const div = document.createElement("div");
    div.innerHTML = html.trim();
    return div.firstChild || div;
  };
  
  return wrap(`
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th>Team</th>
          <th>Fall %</th>
          <th>Falls</th>
          <th>Total Wins</th>
          <th>Record</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
        ${extraRows}
      </tbody>
    </table>
  `);
})()
```

## What is Elo rating?

The Elo rating system is a method for calculating the relative skill levels of players in competitive games, famously used in chess. It assigns a numerical rating that is updated after every match based on the outcome and the difference in the players' ratings. If a higher-rated player wins as expected, they gain only a few points, but if a lower-rated player wins an upset, they earn a significant rating boost. The system is self-correcting over time, rewarding better-than-expected performance with rating increases and penalizing underperformance with decreases, providing an objective measure of a player's relative strength.
