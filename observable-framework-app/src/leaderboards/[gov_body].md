---
toc: false
---

```js
import {fmtDate, fmtInt, fmtPct, formatName, getUrlParam, plain, record, setUrlParams, shortSeason} from "../components/format.js";
import {emptyState, kpi, nameLink} from "../components/ui.js";
```

```js
const gb = observable.params.gov_body;
const govBodies = await FileAttachment("../data/gov_bodies.json").json();
const gbInfo = govBodies.find((d) => d.suffix === gb) ?? {label: gb.toUpperCase(), fullName: ""};
const individuals = plain(await FileAttachment(`data/individual_leaderboards_${observable.params.gov_body}.parquet`).parquet());
const teams = plain(await FileAttachment(`data/team_leaderboards_${observable.params.gov_body}.parquet`).parquet());
```

```js
const ALL_SEASONS = "All seasons";
const ALL_TEAMS = "All teams";
const seasonOptions = [ALL_SEASONS, ...d3.sort(new Set(individuals.map((d) => d.season)), (s) => s).reverse()];
const teamOptions = [ALL_TEAMS, ...d3.sort(new Set(individuals.map((d) => d.team).filter(Boolean)))];
const urlSeason = getUrlParam("season");
const urlTeam = getUrlParam("team");
const seasonInput = Inputs.select(seasonOptions, {
  label: "Season",
  value: seasonOptions.includes(urlSeason) ? urlSeason : seasonOptions[0],
  format: (s) => (s === ALL_SEASONS ? s : shortSeason(s))
});
const teamInput = Inputs.select(teamOptions, {label: "Team", value: teamOptions.includes(urlTeam) ? urlTeam : ALL_TEAMS});
const selectedSeason = Generators.input(seasonInput);
const selectedTeam = Generators.input(teamInput);
```

```js
setUrlParams({season: selectedSeason === ALL_SEASONS ? null : selectedSeason, team: selectedTeam === ALL_TEAMS ? null : selectedTeam});
const allSeasons = selectedSeason === ALL_SEASONS;
const oneTeam = selectedTeam !== ALL_TEAMS;

const withPcts = (d) => ({
  ...d,
  win_pct: d.matches_played > 0 ? (100 * d.wins) / d.matches_played : 0,
  fall_pct: d.wins > 0 ? (100 * d.wins_fall) / d.wins : 0
});

// "All seasons" sums each wrestler's seasons (filtered to the selected team first, so a
// wrestler who changed teams only counts their matches for that team).
function aggregateWrestlers(rows) {
  return d3.groups(rows, (d) => d.name).map(([name, v]) => {
    const upset = d3.greatest(v, (d) => d.biggest_upset_win ?? 0);
    return withPcts({
      name,
      team: d3.greatest(v, (d) => d.season).team,
      matches_played: d3.sum(v, (d) => d.matches_played),
      wins: d3.sum(v, (d) => d.wins),
      losses: d3.sum(v, (d) => d.losses),
      wins_fall: d3.sum(v, (d) => d.wins_fall),
      highest_elo: d3.max(v, (d) => d.highest_elo),
      current_elo: v[0].current_elo,
      biggest_upset_win: upset?.biggest_upset_win ?? 0,
      upset_date: upset?.upset_date,
      upset_tournament_name: upset?.upset_tournament_name,
      upset_opponent_name: upset?.upset_opponent_name,
      upset_opponent_team: upset?.upset_opponent_team,
      upset_result: upset?.upset_result
    });
  });
}

function aggregateTeams(rows) {
  return d3.groups(rows, (d) => d.team).map(([team, v]) => withPcts({
    team,
    matches_played: d3.sum(v, (d) => d.matches_played),
    wins: d3.sum(v, (d) => d.wins),
    losses: d3.sum(v, (d) => d.losses),
    wins_fall: d3.sum(v, (d) => d.wins_fall)
  }));
}

const indRows = individuals.filter((d) => (allSeasons || d.season === selectedSeason) && (!oneTeam || d.team === selectedTeam));
const individualStats = allSeasons ? aggregateWrestlers(indRows) : indRows;
const teamStats = allSeasons ? aggregateTeams(teams) : teams.filter((d) => d.season === selectedSeason);
```

```js
// Minimum-match thresholds scale with the data: 20% of the median, clamped to bounds that
// are looser for narrow views (one team and/or one season) and stricter for broad ones.
const thresholds = (() => {
  const median = (rows) => d3.quantileSorted(d3.sort(rows.map((d) => d.matches_played).filter((m) => m > 0)), 0.5) ?? 0;
  const oneSeason = !allSeasons;
  const indBounds = oneTeam && oneSeason ? [3, 8] : oneTeam || oneSeason ? [5, 12] : [10, 20];
  const teamBounds = oneSeason ? [20, 60] : [50, 100];
  const clamp = (v, [lo, hi]) => Math.max(lo, Math.min(hi, v));
  const individual = clamp(Math.round(median(individualStats) * 0.2), indBounds);
  const team = clamp(Math.round(median(teamStats) * 0.2), teamBounds);
  return {individual, individualFall: Math.max(1, Math.round(individual * 0.1)), team, teamFall: Math.max(3, Math.round(team * 0.1))};
})();
```

```js
// Top n with ties: extends past n while rows tie the nth, and assigns competition ranks (1, 2, 2, 4…).
function topWithTies(rows, n, cmp) {
  const sorted = [...rows].sort(cmp);
  let end = Math.min(n, sorted.length);
  while (end < sorted.length && cmp(sorted[end - 1], sorted[end]) === 0) end++;
  return rankAll(sorted.slice(0, end), cmp);
}

function rankAll(sorted, cmp) {
  const out = [];
  sorted.forEach((d, i) => out.push({...d, rank: i > 0 && cmp(sorted[i - 1], d) === 0 ? out[i - 1].rank : i + 1}));
  return out;
}

const by = (...keys) => (a, b) => {
  for (const k of keys) {
    const diff = (b[k] ?? 0) - (a[k] ?? 0);
    if (diff) return diff;
  }
  return 0;
};

/**
 * A leaderboard card. For team boards, the selected team is highlighted and, if it falls
 * outside the top 10, appended below a divider with its overall rank.
 */
function board({title, note = "", rows, cmp, value, fmt = fmtInt, label, sub, team = false, base = 0}) {
  if (!rows.length) return html`<div class="card"><h2>${title}</h2>${note ? html`<h3>${note}</h3>` : ""}${emptyState("No one qualifies yet", "Try a different season or team.")}</div>`;
  const top = topWithTies(rows, 10, cmp);
  let extra = null;
  if (team && oneTeam && !top.some((d) => d.team === selectedTeam)) {
    extra = rankAll([...rows].sort(cmp), cmp).find((d) => d.team === selectedTeam) ?? null;
  }
  const max = (d3.max(rows, (d) => d[value]) ?? 0) - base || 1;
  const item = (d) => html`<li class=${team && oneTeam && d.team === selectedTeam ? "highlight" : ""} style=${`--pct:${Math.max(0, (100 * ((d[value] ?? 0) - base)) / max)}%`}>
    <span class="rk">${d.rank}</span>
    <span class="nm">${label(d)}<small>${sub(d)}</small></span>
    <span class="val">${fmt(d[value])}</span>
  </li>`;
  return html`<div class="card lb-card">
    <h2>${title}</h2>
    ${note ? html`<h3>${note}</h3>` : ""}
    <ol class="toplist bars">${top.map(item)}${extra ? html`<li class="sep" aria-hidden="true"></li>${item(extra)}` : ""}</ol>
  </div>`;
}

const wrestlerLabel = (d) => nameLink(gb, d.name);
const teamSub = (d) => [oneTeam ? null : d.team, record(d.wins, d.losses)].filter(Boolean).join(" · ");
const pct1 = (v) => fmtPct(v, 1);
```

<div class="hero">
  <img class="hero-logo" src="../assets/logo.png" alt="">
  <div class="hero-body">
    <h1>${gbInfo.label} Leaderboards</h1>
    <h2>${gbInfo.fullName}</h2>
    <p>Season and career leaders: volume, winning, pins, peak Elo and the biggest upsets. Filter by season or team.</p>
  </div>
  <div class="hero-side">${seasonInput}${teamInput}</div>
</div>

```js
const totalMatches = Math.round(d3.sum(teamStats, (d) => d.matches_played) / 2);
const scopeLabel = `${allSeasons ? "All seasons" : shortSeason(selectedSeason)}${oneTeam ? ` · ${selectedTeam}` : ""}`;
display(html`<div class="grid grid-cols-4">
  ${kpi("Wrestlers", fmtInt(individualStats.length), scopeLabel, {tone: "navy"})}
  ${oneTeam
    ? kpi("Team record", (() => { const t = teamStats.find((d) => d.team === selectedTeam); return t ? record(t.wins, t.losses) : "–"; })(), "individual bouts", {tone: "navy"})
    : kpi("Matches", fmtInt(totalMatches), `${fmtInt(teamStats.length)} teams`, {tone: "navy"})}
  ${kpi("Win % minimum", `${thresholds.individual} matches`, `teams: ${thresholds.team} matches`)}
  ${kpi("Fall % minimum", `${thresholds.individualFall} fall${thresholds.individualFall === 1 ? "" : "s"}`, `teams: ${thresholds.teamFall} falls`)}
</div>`);
```

<div class="section-title"><h2>Individual leaders</h2><span>Top 10 (ties included) · thresholds adjust to the filter</span></div>

```js
display(html`<div class="grid grid-cols-3">
  ${board({title: "Highest Elo", note: "Peak rating reached (bars from the 1,000 starting rating)", base: 1000, rows: individualStats, cmp: by("highest_elo"), value: "highest_elo", label: wrestlerLabel, sub: (d) => [oneTeam ? null : d.team, `now ${fmtInt(d.current_elo)}`].filter(Boolean).join(" · ")})}
  ${board({title: "Most wins", rows: individualStats, cmp: by("wins"), value: "wins", label: wrestlerLabel, sub: teamSub})}
  ${board({title: "Most matches", rows: individualStats, cmp: by("matches_played"), value: "matches_played", label: wrestlerLabel, sub: teamSub})}
  ${board({title: "Win percentage", note: `Minimum ${thresholds.individual} matches`, rows: individualStats.filter((d) => d.matches_played >= thresholds.individual), cmp: by("win_pct", "wins"), value: "win_pct", fmt: pct1, label: wrestlerLabel, sub: teamSub})}
  ${board({title: "Fall percentage", note: `Minimum ${thresholds.individual} matches and ${thresholds.individualFall} fall${thresholds.individualFall === 1 ? "" : "s"}`, rows: individualStats.filter((d) => d.matches_played >= thresholds.individual && d.wins_fall >= thresholds.individualFall), cmp: by("fall_pct", "wins_fall"), value: "fall_pct", fmt: pct1, label: wrestlerLabel, sub: (d) => [oneTeam ? null : d.team, `${d.wins_fall} falls in ${d.wins} wins`].filter(Boolean).join(" · ")})}
  ${board({title: "Most falls", rows: individualStats, cmp: by("wins_fall", "fall_pct"), value: "wins_fall", label: wrestlerLabel, sub: (d) => [oneTeam ? null : d.team, `${fmtPct(d.fall_pct)} of wins`].filter(Boolean).join(" · ")})}
</div>`);
```

<div class="section-title"><h2>Biggest upsets</h2><span>Largest single-match Elo gain from a win</span></div>

```js
{
  const upsets = topWithTies(individualStats.filter((d) => d.biggest_upset_win > 0), 10, by("biggest_upset_win"));
  display(upsets.length
    ? Inputs.table(upsets, {
        columns: ["rank", "name", "upset_opponent_name", "upset_result", "biggest_upset_win", "upset_tournament_name", "upset_date"],
        header: {rank: "#", name: "Winner", upset_opponent_name: "Beat", upset_result: "Result", biggest_upset_win: "Elo gain", upset_tournament_name: "Tournament", upset_date: "Date"},
        format: {
          name: (n, i, data) => html`${nameLink(gb, n)} <small class="muted">${data[i].team ?? ""}</small>`,
          upset_opponent_name: (n, i, data) => n ? html`${nameLink(gb, n)} <small class="muted">${data[i].upset_opponent_team ?? ""}</small>` : "–",
          biggest_upset_win: (v) => html`<span class="delta up">+${fmtInt(v)}</span>`,
          upset_date: fmtDate
        },
        width: {rank: 40, biggest_upset_win: 80},
        sort: "biggest_upset_win",
        reverse: true,
        select: false,
        rows: 10.5
      })
    : emptyState("No upsets recorded", "Try a different season or team."));
}
```

<div class="section-title"><h2>Team leaders</h2><span>${oneTeam ? `${selectedTeam} highlighted` : "Pick a team above to highlight it"}</span></div>

```js
const teamLabel = (d) => d.team ?? "–";
display(html`<div class="grid grid-cols-2">
  ${board({team: true, title: "Most wins", rows: teamStats, cmp: by("wins"), value: "wins", label: teamLabel, sub: (d) => `${record(d.wins, d.losses)} · ${fmtPct(d.win_pct)}`})}
  ${board({team: true, title: "Win percentage", note: `Minimum ${thresholds.team} matches`, rows: teamStats.filter((d) => d.matches_played >= thresholds.team), cmp: by("win_pct", "wins"), value: "win_pct", fmt: pct1, label: teamLabel, sub: (d) => record(d.wins, d.losses)})}
  ${board({team: true, title: "Most matches", rows: teamStats, cmp: by("matches_played"), value: "matches_played", label: teamLabel, sub: (d) => record(d.wins, d.losses)})}
  ${board({team: true, title: "Fall percentage", note: `Minimum ${thresholds.team} matches and ${thresholds.teamFall} falls`, rows: teamStats.filter((d) => d.matches_played >= thresholds.team && d.wins_fall >= thresholds.teamFall), cmp: by("fall_pct", "wins_fall"), value: "fall_pct", fmt: pct1, label: teamLabel, sub: (d) => `${d.wins_fall} falls in ${d.wins} wins`})}
</div>`);
```

<details class="method">
<summary>What is an Elo rating?</summary>

Elo is a rating system (originally from chess) that updates after every match based on the result and the gap between the two ratings. Beating a higher-rated opponent earns many points; beating a much lower-rated one earns few. Over time it gives an objective, self-correcting measure of relative strength. "Biggest upset" is the largest single-match gain from a win, which happens when a lower-rated wrestler beats a much stronger one.

</details>
