---
toc: false
---

```js
import {fmtDate, fmtInt, formatName, shortSeason} from "./components/format.js";
import {kpi} from "./components/ui.js";
```

```js
const govBodies = await FileAttachment("data/gov_bodies.json").json();
const summary = await FileAttachment("data/summary.json").json();
const bodies = govBodies.map((g) => ({...g, ...(summary[g.suffix] ?? {})}));
const lastDate = d3.max(bodies, (d) => d.last_date);
```

<div class="hero">
  <img class="hero-logo" src="./assets/logo.png" alt="Wrestling Stats Explorer logo">
  <div class="hero-body">
    <h1>Wrestling Stats Explorer</h1>
    <h2>Elo rankings, wrestler profiles, leaderboards and head-to-head matchups</h2>
    <p>Match results for ${govBodies.map((g) => g.label).join(", ")}, as reflected in <a href="https://www.trackwrestling.com/Login.jsp">Trackwrestling.com</a> and <a href="https://www.themat.com/results">USA Wrestling results</a>. Data through ${fmtDate(lastDate)}.</p>
  </div>
</div>

<div class="grid grid-cols-4">
  ${kpi("Wrestlers", fmtInt(d3.sum(bodies, (d) => d.wrestlers)), `across ${bodies.length} governing bodies`, {tone: "navy"})}
  ${kpi("Matches", fmtInt(d3.sum(bodies, (d) => d.matches)), "with Elo ratings", {tone: "navy"})}
  ${kpi("Tournaments", fmtInt(d3.sum(bodies, (d) => d.tournaments)), "", {tone: "navy"})}
  ${kpi("Since", fmtDate(d3.min(bodies, (d) => d.first_date), {month: "short", year: "numeric"}), `through ${fmtDate(lastDate, {month: "short", year: "numeric"})}`, {tone: "accent"})}
</div>

<div class="section-title"><h2>Choose a governing body</h2></div>

```js
const leaderLink = (gb, name) => html`<a class="wrestler-link" href=${`./individual_stats/${gb}?wrestler=${encodeURIComponent(name)}`}>${formatName(name)}</a>`;
display(html`<div class="grid grid-cols-2">${bodies.map((g) => html`<div class="card gb-card">
  <header><h2>${g.label}</h2><span>${g.fullName ?? ""}</span></header>
  <div class="gb-stats">
    <div><b>${fmtInt(g.wrestlers)}</b><small>Wrestlers</small></div>
    <div><b>${fmtInt(g.matches)}</b><small>Matches</small></div>
    <div><b>${fmtInt(g.season_teams)}</b><small>${g.season ? `${shortSeason(g.season)} teams` : "Teams"}</small></div>
  </div>
  ${g.leaders?.length ? html`<div>
    <h3>Top rated · ${shortSeason(g.season)}</h3>
    <ol class="toplist">${g.leaders.map((d, i) => html`<li><span class="rk">${i + 1}</span><span class="nm">${leaderLink(g.suffix, d.name)}<small>${d.team ?? ""}</small></span><span class="val">${fmtInt(d.elo)}</span></li>`)}</ol>
  </div>` : ""}
  <div class="gb-links">
    <a class="btn primary" href=${`./rankings/${g.suffix}`}>Rankings</a>
    <a class="btn" href=${`./individual_stats/${g.suffix}`}>Profiles</a>
    <a class="btn" href=${`./leaderboards/${g.suffix}`}>Leaderboards</a>
    <a class="btn" href=${`./compare/${g.suffix}`}>Head-to-head</a>
  </div>
</div>`)}</div>`);
```

<div class="section-title"><h2>What's inside</h2></div>

<div class="grid grid-cols-4 features">
  <div class="card"><h2>🏆 Rankings</h2><p>Season Elo power rankings overall, by weight class and by team, with 30-day movers.</p></div>
  <div class="card"><h2>👤 Profiles</h2><p>Every wrestler's rating and rank over time, season splits, placements and full match history.</p></div>
  <div class="card"><h2>📊 Leaderboards</h2><p>Season and career leaders in wins, win %, pins, peak Elo and biggest upsets.</p></div>
  <div class="card"><h2>⚔️ Head-to-head</h2><p>Compare two wrestlers: win probability, direct meetings and common opponents.</p></div>
</div>

<details class="method">
<summary>How ratings work</summary>

Every wrestler starts at 1,000 Elo. After each match, the winner takes points from the loser; how many depends on how surprising the result was. Beating a much higher-rated opponent earns a lot, and beating a much lower-rated one earns little. Rankings only include wrestlers with enough matches in the season, so they stay meaningful.

</details>
