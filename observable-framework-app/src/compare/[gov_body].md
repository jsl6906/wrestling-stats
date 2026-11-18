---
toc: false
---

```js
import {expectedScore, fmtDate, fmtInt, fmtPct, formatName, getUrlParam, isWin, pageHref, parseDay, plain, record, resultLabel, setUrlParams, shardOf, shortSeason, winPct} from "../components/format.js";
import {badge, emptyState, kpi, nameLink, resultPill, wrestlerSearch} from "../components/ui.js";
import {seasonAxis} from "../components/charts.js";
```

```js
const gb = observable.params.gov_body;
const govBodies = await FileAttachment("../data/gov_bodies.json").json();
const gbInfo = govBodies.find((d) => d.suffix === gb) ?? {label: gb.toUpperCase(), fullName: ""};
const wrestlers = plain(await FileAttachment(`../data/gb/${observable.params.gov_body}/wrestlers.parquet`).parquet());
const directory = new Map(wrestlers.map((d) => [d.name, d]));
// One file per shard; FileAttachment paths must be literal so every shard is listed.
const historyFiles = [
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_00.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_01.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_02.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_03.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_04.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_05.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_06.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_07.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_08.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_09.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_10.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_11.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_12.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_13.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_14.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_15.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_16.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_17.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_18.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_19.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_20.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_21.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_22.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_23.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_24.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_25.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_26.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_27.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_28.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_29.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_30.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/history_31.parquet`)
];
const rankFiles = [
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_00.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_01.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_02.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_03.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_04.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_05.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_06.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_07.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_08.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_09.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_10.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_11.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_12.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_13.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_14.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_15.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_16.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_17.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_18.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_19.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_20.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_21.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_22.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_23.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_24.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_25.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_26.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_27.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_28.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_29.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_30.parquet`),
  FileAttachment(`../data/gb/${observable.params.gov_body}/ranks_31.parquet`)
];
```

```js
const searchA = wrestlerSearch(wrestlers, {value: getUrlParam("a"), placeholder: "First wrestler…", label: "Wrestler A"});
const searchB = wrestlerSearch(wrestlers, {value: getUrlParam("b"), placeholder: "Second wrestler…", label: "Wrestler B"});
const nameA = Generators.input(searchA);
const nameB = Generators.input(searchB);
```

```js
setUrlParams({a: nameA, b: nameB});
const COLORS = ["var(--accent-line)", "var(--brand-gold)"];
const picked = [nameA, nameB].filter(Boolean);

async function loadWrestler(name) {
  if (!name) return null;
  const s = shardOf(name);
  const [h, r] = await Promise.all([historyFiles[s].parquet(), rankFiles[s].parquet()]);
  const matches = plain(Array.from(h).filter((d) => d.name === name))
    .sort((a, b) => a.ord - b.ord)
    .map((d) => ({...d, day: parseDay(d.date), win: isWin(d.result)}));
  const ranks = plain(Array.from(r).filter((d) => d.name === name));
  return {name, info: directory.get(name), matches, rank: ranks.at(-1)};
}

const [A, B] = await Promise.all([loadWrestler(nameA), loadWrestler(nameB)]);
const both = A && B && A.name !== B.name;
```

```js
for (const el of document.querySelectorAll(".needs-data")) el.hidden = !(Boolean(both));
```

<div class="hero">
  <img class="hero-logo" src="../assets/logo.png" alt="">
  <div class="hero-body">
    <h1>Head-to-Head</h1>
    <h2>Compare any two ${gbInfo.label} wrestlers: ratings, meetings and common opponents</h2>
    <div class="versus-inputs">${searchA}<span class="vs">vs</span>${searchB}</div>
  </div>
</div>

```js
if (!both) {
  display(emptyState(
    A && B ? "Pick two different wrestlers" : "Pick two wrestlers to compare",
    "Use the search boxes above. Tip: open the Opponents tab on any wrestler profile and click ⇄ to compare directly."
  ));
}
```

```js
function sideCard(W, color) {
  const i = W.info ?? {};
  const fallsW = i.wins_fall ?? 0;
  return html`<div class="card versus-card" style="--side:${color}">
    <h2>${nameLink(gb, W.name)}</h2>
    <h3>${[i.team, i.w_key].filter(Boolean).join(" · ") || "–"}</h3>
    <div class="hero-badges" style="justify-content:flex-start;margin:0.5rem 0">
      ${W.rank ? badge(`#${fmtInt(W.rank.rank)} overall · ${shortSeason(W.rank.season)}`, W.rank.rank <= 25 ? "gold" : "") : badge("unranked")}
      ${W.rank?.w_rank ? badge(`#${W.rank.w_rank} at ${W.rank.w_key}`) : ""}
    </div>
    <dl class="stat-list">
      <dt>Current Elo</dt><dd>${fmtInt(i.elo)}</dd>
      <dt>Peak Elo</dt><dd>${fmtInt(i.best_elo)} <small>${fmtDate(i.best_date)}</small></dd>
      <dt>Record</dt><dd>${record(i.wins, i.losses)} <small>${fmtPct(winPct(i.wins ?? 0, i.losses ?? 0))}</small></dd>
      <dt>Wins by fall</dt><dd>${fmtInt(fallsW)} <small>${i.wins ? fmtPct((100 * fallsW) / i.wins) : ""}</small></dd>
      <dt>Avg. opponent Elo</dt><dd>${fmtInt(i.opp_avg_elo)}</dd>
      <dt>Last match</dt><dd>${fmtDate(i.last_date)}</dd>
    </dl>
  </div>`;
}

if (both) {
  const pA = expectedScore(A.info.elo, B.info.elo);
  display(html`<div class="grid grid-cols-2">${sideCard(A, COLORS[0])}${sideCard(B, COLORS[1])}</div>`);
  display(html`<div class="card">
    <h2>Projected matchup</h2>
    <h3>Win probability from current Elo ratings (${fmtInt(A.info.elo)} vs ${fmtInt(B.info.elo)})</h3>
    <div class="prob-bar">
      <div style="flex:${pA};background:${COLORS[0]}"><span>${formatName(A.name)} ${fmtPct(100 * pA, 0)}</span></div>
      <div style="flex:${1 - pA};background:${COLORS[1]}"><span>${fmtPct(100 * (1 - pA), 0)} ${formatName(B.name)}</span></div>
    </div>
    <p class="muted" style="margin:0.5rem 0 0">Ratings reflect each wrestler's most recent match; a long layoff or a weight change makes the projection less reliable.</p>
  </div>`);
}
```

```js
const meetings = both ? A.matches.filter((d) => d.opp === B.name).reverse() : [];
const commonOpps = (() => {
  if (!both) return [];
  const byOpp = (W) => d3.group(W.matches.filter((d) => d.opp && d.opp !== A.name && d.opp !== B.name), (d) => d.opp);
  const ga = byOpp(A), gbm = byOpp(B);
  return [...ga.keys()].filter((k) => gbm.has(k)).map((opp) => {
    const ma = ga.get(opp), mb = gbm.get(opp);
    const last = d3.greatest([...ma, ...mb], (d) => d.ord);
    return {
      opp,
      team: last?.opp_team ?? "",
      elo: directory.get(opp)?.elo ?? null,
      aW: ma.filter((d) => d.win).length, aL: ma.filter((d) => !d.win).length,
      bW: mb.filter((d) => d.win).length, bL: mb.filter((d) => !d.win).length
    };
  }).map((d) => ({...d, a: record(d.aW, d.aL), b: record(d.bW, d.bL)})).sort((x, y) => (y.elo ?? 0) - (x.elo ?? 0));
})();
```

```js
if (both) {
  const aWins = meetings.filter((d) => d.win).length;
  const common = {a: d3.sum(commonOpps, (d) => d.aW), a_l: d3.sum(commonOpps, (d) => d.aL), b: d3.sum(commonOpps, (d) => d.bW), bl: d3.sum(commonOpps, (d) => d.bL)};
  display(html`<div class="grid grid-cols-3">
    ${kpi("Head-to-head", meetings.length ? `${aWins}–${meetings.length - aWins}` : "Never met", meetings.length ? `${formatName(A.name)} first · ${meetings.length} meeting${meetings.length > 1 ? "s" : ""}` : "no recorded matches between them", {tone: "gold"})}
    ${kpi("Common opponents", fmtInt(commonOpps.length), "wrestlers both have faced")}
    ${kpi("Vs. common opponents", commonOpps.length ? `${record(common.a, common.a_l)} | ${record(common.b, common.bl)}` : "–", commonOpps.length ? `${formatName(A.name)} | ${formatName(B.name)}` : "")}
  </div>`);
}
```

<div class="card needs-data">
<h2>Elo rating over time</h2>
<h3>Each line is one season; dots are matches. Meetings between the two are ringed.</h3>

```js
if (both) {
  const series = [A, B].flatMap((W) => W.matches.map((d) => ({...d, who: formatName(W.name)})));
  const met = series.filter((d) => d.opp === A.name || d.opp === B.name);
  const axis = seasonAxis(series);
  display(resize((width) => Plot.plot({
    width,
    height: 340,
    marginLeft: 50,
    x: axis.scale,
    y: {label: "Elo", grid: true, tickFormat: fmtInt},
    color: {domain: [formatName(A.name), formatName(B.name)], range: COLORS, legend: true},
    marks: [
      axis.separators,
      Plot.line(series, {x: axis.x, y: "post", stroke: "who", z: (d) => `${d.who}|${d.season}`, strokeWidth: 2, curve: "step-after"}),
      Plot.dot(series, {x: axis.x, y: "post", fill: "who", r: 2.5, tip: true,
        title: (d) => `${d.who} · ${fmtDate(d.date)}\n${d.win ? "W" : "L"} vs ${formatName(d.opp)} · ${resultLabel(d)}\nElo ${fmtInt(d.post)}`}),
      Plot.dot(met, {x: axis.x, y: "post", r: 7, stroke: "currentColor", strokeWidth: 1.5})
    ]
  })));
}
```

</div>

<div class="card needs-data">
<h2>Direct meetings</h2>
<h3>Every recorded match between the two, newest first</h3>

```js
if (both) {
  display(meetings.length
    ? Inputs.table(meetings.map((d) => ({
        date: d.date,
        winner: d.win ? A.name : B.name,
        result: resultLabel(d),
        tournament: d.tournament ?? "",
        round: d.round ?? "",
        eloA: d.pre,
        eloB: d.opp_elo
      })), {
        columns: ["date", "winner", "result", "tournament", "round", "eloA", "eloB"],
        header: {date: "Date", winner: "Winner", result: "Result", tournament: "Tournament", round: "Round", eloA: `${formatName(A.name)} Elo`, eloB: `${formatName(B.name)} Elo`},
        format: {date: fmtDate, winner: (n) => nameLink(gb, n), eloA: fmtInt, eloB: fmtInt},
        select: false,
        rows: 10.5
      })
    : emptyState("They haven't wrestled each other", "No match between these two appears in the data."));
}
```

</div>

<div class="card needs-data">
<h2>Common opponents</h2>
<h3>How each fared against wrestlers they've both faced (sorted by opponent's current Elo)</h3>

```js
if (both) {
  const fmtRec = (w, l) => html`<span class="${w > l ? "win-text" : w < l ? "loss-text" : ""}">${record(w, l)}</span>`;
  display(commonOpps.length
    ? Inputs.table(commonOpps, {
        columns: ["opp", "team", "elo", "a", "b"],
        header: {opp: "Opponent", team: "Team", elo: "Elo", a: formatName(A.name), b: formatName(B.name)},
        format: {opp: (n) => nameLink(gb, n), elo: fmtInt, a: (_, i, data) => fmtRec(data[i].aW, data[i].aL), b: (_, i, data) => fmtRec(data[i].bW, data[i].bL)},
        select: false,
        rows: 15.5
      })
    : emptyState("No common opponents", "These two haven't faced any of the same wrestlers."));
}
```

</div>

```js
if (both) {
  display(html`<div class="toolbar">
    <a class="btn" href=${pageHref("compare", gb, {a: B.name, b: A.name})}>⇄ Swap sides</a>
    <a class="btn" href=${pageHref("individual_stats", gb, {wrestler: A.name})}>${formatName(A.name)} profile</a>
    <a class="btn" href=${pageHref("individual_stats", gb, {wrestler: B.name})}>${formatName(B.name)} profile</a>
  </div>`);
}
```
