---
toc: false
---

```js
import {MIN_SEASON_MATCHES, fmtDate, fmtInt, fmtSigned, formatName, getUrlParam, parseDay, plain, record, setUrlParams, shortSeason} from "../components/format.js";
import {badge, delta, emptyState, kpi, nameLink} from "../components/ui.js";
import {seasonList, seasonRankings, teamRankings, weightRankings} from "../components/rankings.js";
```

```js
const gb = observable.params.gov_body;
const govBodies = await FileAttachment("../data/gov_bodies.json").json();
const gbInfo = govBodies.find((d) => d.suffix === gb) ?? {label: gb.toUpperCase(), fullName: ""};
const seasonsAll = plain(await FileAttachment(`../data/gb/${observable.params.gov_body}/seasons.parquet`).parquet());
const trendAll = plain(await FileAttachment(`../data/gb/${observable.params.gov_body}/trend.parquet`).parquet());
const seasons = seasonList(seasonsAll);
const seasonInput = Inputs.select(seasons, {
  label: "Season",
  value: seasons.includes(getUrlParam("season")) ? getUrlParam("season") : seasons[0],
  format: shortSeason
});
const season = Generators.input(seasonInput);
```

```js
setUrlParams({season: season === seasons[0] ? null : season});
const isCurrent = season === seasons[0];
const seasonRows = seasonsAll.filter((d) => d.season === season);
const ranked = seasonRankings(seasonRows);
const weights = weightRankings(ranked);
const teams = teamRankings(ranked);
const lastDate = seasonRows.reduce((m, d) => (d.last_date > m ? d.last_date : m), "");
const totalMatches = Math.round(seasonRows.reduce((s, d) => s + d.matches, 0) / 2);
const movesLabel = `30 days to ${fmtDate(lastDate, {month: "short", day: "numeric"})}`;
const top = ranked[0];
const hottest = ranked.filter((d) => d.m30 > 0 && d.elo_change30 != null).sort((a, b) => b.elo_change30 - a.elo_change30)[0];
```

<div class="hero">
  <img class="hero-logo" src="../assets/logo.png" alt="">
  <div class="hero-body">
    <h1>${gbInfo.label} Rankings</h1>
    <h2>${gbInfo.fullName} · ${shortSeason(season)} season · through ${fmtDate(lastDate)}</h2>
    <p>Elo power rankings for every wrestler with ${MIN_SEASON_MATCHES}+ matches in the season, plus weight-class and team rankings.</p>
  </div>
  <div class="hero-side">${seasonInput}</div>
</div>

<div class="grid grid-cols-4">
  ${kpi("Ranked wrestlers", fmtInt(ranked.length), `${fmtInt(seasonRows.length)} wrestled this season`, {tone: "navy"})}
  ${kpi("Matches", fmtInt(totalMatches), `${fmtInt(teams.length)} ranked teams`, {tone: "navy"})}
  ${kpi("No. 1 overall", top ? nameLink(gb, top.name) : "–", top ? `${top.team ?? ""} · ${fmtInt(top.end_elo)} Elo` : "", {tone: "accent"})}
  ${kpi("Hottest wrestler", hottest ? nameLink(gb, hottest.name) : "–", hottest ? `${fmtSigned(hottest.elo_change30)} Elo in the ${movesLabel}` : "", {tone: "accent"})}
</div>

<div class="section-title"><h2>Overall power rankings</h2><span>Ranked by latest Elo · movement vs. 30 days earlier</span></div>

```js
const overallSearch = view(Inputs.search(ranked, {placeholder: "Filter by name, team or weight…", columns: ["name", "team", "w_key"]}));
```

```js
const [minElo, maxElo] = d3.extent(ranked, (d) => d.end_elo);
const eloBar = (v) => html`<div class="elo-bar"><span>${fmtInt(v)}</span><span><span style="width:${(100 * (v - minElo)) / (maxElo - minElo || 1)}%"></span></span></div>`;
const signed = (v) => html`<span class="delta ${v > 0 ? "up" : v < 0 ? "down" : "flat"}">${fmtSigned(v)}</span>`;
display(
  Inputs.table(overallSearch, {
    columns: ["rank", "move", "name", "team", "w_key", "wins", "end_elo", "elo_change30"],
    header: {rank: "#", move: "30d", name: "Wrestler", team: "Team", w_key: "Weight", wins: "Record", end_elo: "Elo", elo_change30: "Elo 30d"},
    format: {
      move: (v) => (v === "new" ? badge("new") : delta(v)),
      name: (v) => nameLink(gb, v),
      w_key: (v) => v ?? "–",
      wins: (v, i, data) => record(data[i].wins, data[i].losses),
      end_elo: eloBar,
      elo_change30: signed
    },
    width: {rank: 44, move: 56, w_key: 80, wins: 70, end_elo: 200, elo_change30: 70},
    align: {rank: "right", move: "center", wins: "right", elo_change30: "right"},
    select: false,
    rows: 20.5
  })
);
```

<div class="grid grid-cols-2">
<div class="card">
<h2>Race for the top</h2>
<h3>Weekly overall rank of the current top 10</h3>

```js
const top10 = ranked.slice(0, 10).map((d) => d.name);
const top10Set = new Set(top10);
const trend = trendAll.filter((d) => d.season === season && top10Set.has(d.name)).map((d) => ({...d, date: parseDay(d.wk)}));
display(
  trend.length
    ? resize((width) => Plot.plot({
        width,
        height: 380,
        marginRight: 110,
        x: {label: null},
        y: {reverse: true, domain: [1, 25], label: "Rank", clamp: true, ticks: [1, 5, 10, 15, 20, 25]},
        color: {scheme: "tableau10", domain: top10},
        marks: [
          Plot.ruleY([1], {stroke: "var(--theme-foreground-faintest)"}),
          Plot.line(trend, {x: "date", y: "rank", z: "name", stroke: "name", strokeWidth: 2, curve: "bump-x", clip: true}),
          Plot.dot(trend, Plot.selectLast({x: "date", y: "rank", z: "name", fill: "name", r: 3})),
          Plot.text(trend, Plot.selectLast({x: "date", y: "rank", z: "name", fill: "name", text: (d) => formatName(d.name).split(",")[0], dx: 6, textAnchor: "start"})),
          Plot.tip(trend, Plot.pointer({x: "date", y: "rank", title: (d) => `${formatName(d.name)}\n#${d.rank} · ${fmtInt(d.elo)} Elo\nweek of ${fmtDate(d.wk)}`}))
        ]
      }))
    : emptyState("No trend data", "Not enough weekly snapshots this season.")
);
```

</div>
<div class="card">
<h2>Biggest movers</h2>
<h3>Elo change over the ${movesLabel} · wrestlers active in that window</h3>

```js
const active = ranked.filter((d) => d.m30 > 0 && d.elo_change30 != null);
const risers = [...active].sort((a, b) => b.elo_change30 - a.elo_change30).slice(0, 8).filter((d) => d.elo_change30 > 0);
const fallers = [...active].sort((a, b) => a.elo_change30 - b.elo_change30).slice(0, 8).filter((d) => d.elo_change30 < 0);
const movers = [...risers, ...fallers.reverse()];
display(
  movers.length
    ? resize((width) => Plot.plot({
        width,
        height: 380,
        marginLeft: 150,
        marginRight: 10,
        x: {label: "Elo change", grid: true},
        y: {label: null, domain: movers.map((d) => d.name), tickFormat: formatName},
        marks: [
          Plot.barX(movers, {x: "elo_change30", y: "name", fill: (d) => (d.elo_change30 > 0 ? "var(--up)" : "var(--down)"), rx: 3}),
          Plot.text(risers, {x: "elo_change30", y: "name", text: (d) => fmtSigned(d.elo_change30), textAnchor: "end", dx: -4, fill: "white", fontWeight: 600}),
          Plot.text(fallers, {x: "elo_change30", y: "name", text: (d) => fmtSigned(d.elo_change30), textAnchor: "start", dx: 4, fill: "white", fontWeight: 600}),
          Plot.ruleX([0]),
          Plot.tip(movers, Plot.pointerY({x: "elo_change30", y: "name", title: (d) => `${formatName(d.name)} (${d.team ?? "–"})\n${fmtSigned(d.elo_change30)} Elo in ${d.m30} matches\nNow #${d.rank} overall`}))
        ]
      }))
    : emptyState("No recent movement", "Nobody wrestled in this window.")
);
```

</div>
</div>

<div class="section-title"><h2>Weight-class rankings</h2><span>By each wrestler's most recent weight in the season</span></div>

```js
const weightKeys = [...weights.keys()];
const defaultWeight = weightKeys.includes(getUrlParam("weight")) ? getUrlParam("weight") : weightKeys[0];
const weightInput = Inputs.select(weightKeys, {label: "Weight class", value: defaultWeight});
const weight = weightKeys.length ? Generators.input(weightInput) : null;
if (!weightKeys.length) {
  display(emptyState("Weight classes unavailable", "Weight names for this season's events aren't in a consistent format, so weight-class rankings can't be built."));
} else if (weightKeys.length <= 20) {
  display(html`<div class="grid grid-cols-4">${weightKeys.map((k) => html`<div class="card weight-card">
    <header><h3>${k}</h3><span class="muted small">${weights.get(k).length} ranked</span></header>
    <ol class="toplist">${weights.get(k).slice(0, 5).map((d) => html`<li><span class="rk">${d.w_rank}</span><span class="nm">${nameLink(gb, d.name)}<small>${d.team ?? ""}</small></span><span class="val">${fmtInt(d.end_elo)}</span></li>`)}</ol>
  </div>`)}</div>`);
}
if (weightKeys.length) display(html`<div class="toolbar">${weightInput}</div>`);
```

```js
if (weight) {
  setUrlParams({weight: weight === weightKeys[0] ? null : weight});
  display(
    Inputs.table(weights.get(weight), {
      columns: ["w_rank", "name", "team", "wins", "end_elo", "rank", "elo_change30"],
      header: {w_rank: "#", name: "Wrestler", team: "Team", wins: "Record", end_elo: "Elo", rank: "Overall", elo_change30: "Elo 30d"},
      format: {
        name: (v) => nameLink(gb, v),
        wins: (v, i, data) => record(data[i].wins, data[i].losses),
        end_elo: fmtInt,
        rank: (v) => `#${v}`,
        elo_change30: signed
      },
      width: {w_rank: 44, wins: 70, end_elo: 70, rank: 70, elo_change30: 70},
      align: {w_rank: "right", wins: "right", end_elo: "right", rank: "right", elo_change30: "right"},
      select: false,
      rows: 15.5
    })
  );
}
```

<div class="section-title"><h2>Team power rankings</h2><span>Average Elo of each team's top 10 ranked wrestlers (min. 5)</span></div>

<div class="grid grid-cols-2">
<div class="card">

```js
display(
  teams.length
    ? Inputs.table(teams, {
        columns: ["rank", "team", "score", "ranked", "top25", "best"],
        header: {rank: "#", team: "Team", score: "Power", ranked: "Ranked", top25: "Top 25", best: "Best wrestler"},
        format: {
          score: fmtInt,
          best: (d) => html`${nameLink(gb, d.name)} <span class="muted small">${fmtInt(d.end_elo)}</span>`
        },
        width: {rank: 40, score: 60, ranked: 60, top25: 60},
        align: {rank: "right", score: "right", ranked: "right", top25: "right"},
        select: false,
        rows: 17.5
      })
    : emptyState("No team rankings", "Not enough ranked wrestlers per team.")
);
```

</div>
<div class="card">
<h2>Team depth</h2>
<h3>Top-10 lineup Elo for the 15 strongest teams · gold tick = power score</h3>

```js
const topTeams = teams.slice(0, 15);
const depthRows = topTeams.flatMap((t) => t.members);
display(
  topTeams.length
    ? resize((width) => Plot.plot({
        width,
        height: 420,
        marginLeft: 140,
        x: {label: "Elo", grid: true},
        y: {label: null, domain: topTeams.map((d) => d.team)},
        marks: [
          Plot.dot(depthRows, {x: "end_elo", y: "team", fill: "var(--brand-navy)", fillOpacity: 0.6, r: 4}),
          Plot.tickX(topTeams, {x: "score", y: "team", stroke: "var(--brand-gold)", strokeWidth: 3}),
          Plot.tip(depthRows, Plot.pointer({x: "end_elo", y: "team", title: (d) => `${formatName(d.name)}\n${fmtInt(d.end_elo)} Elo · #${d.rank} overall${d.w_key ? `\n${d.w_key}` : ""}`}))
        ]
      }))
    : emptyState("No team data")
);
```

</div>
</div>

<details class="method">
<summary>How rankings work</summary>

- **Elo** starts at 1000. Each match moves both wrestlers' ratings based on the result and how surprising it was — beating a higher-rated opponent earns more. Ratings carry over between seasons and drift back toward 1000 during long layoffs.
- **Overall rankings** order every wrestler with at least ${MIN_SEASON_MATCHES} matches in the selected season by their latest Elo. Ties share a rank.
- **30-day movement** compares each rank with the rank as of 30 days before the season's most recent match. "new" means the wrestler wasn't yet eligible then.
- **Weight-class rankings** use the weight a wrestler most recently competed at; a weight is shown when at least 5 wrestlers qualify.
- **Team power** is the average Elo of a team's 10 best ranked wrestlers (teams need at least 5).

Rankings are computed from public results only and are unofficial.

</details>
