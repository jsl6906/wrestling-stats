---
toc: false
---

```js
import {fmtDate, fmtFallTime, fmtInt, fmtPct, fmtSigned, formatName, getUrlParam, isWin, pageHref, parseDay, plain, record, resultLabel, setUrlParams, shardOf, shortSeason, toNum, winPct} from "../components/format.js";
import {badge, emptyState, kpi, medal, nameLink, resultPill, wrestlerSearch} from "../components/ui.js";
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
const searchInput = wrestlerSearch(wrestlers, {value: getUrlParam("wrestler"), placeholder: `Search ${fmtInt(wrestlers.length)} ${gbInfo.label} wrestlers by name or team…`});
const selected = Generators.input(searchInput);
```

```js
setUrlParams({wrestler: selected});
const w = selected ? directory.get(selected) : null;
const [histTable, rankTable] = selected ? await Promise.all([historyFiles[shardOf(selected)].parquet(), rankFiles[shardOf(selected)].parquet()]) : [[], []];
const matches = plain(Array.from(histTable).filter((d) => d.name === selected))
  .sort((a, b) => a.ord - b.ord)
  .map((d) => ({...d, day: parseDay(d.date), win: isWin(d.result), change: d.post - d.pre}));
const ranks = plain(Array.from(rankTable).filter((d) => d.name === selected)).map((d) => ({...d, day: parseDay(d.wk)}));
const latestRank = ranks.at(-1);
```

```js
for (const el of document.querySelectorAll(".needs-data")) el.hidden = !(matches.length > 0);
```

```js
const placementsAll = plain(await FileAttachment(`../data/gb/${observable.params.gov_body}/placements.parquet`).parquet());
const bandsAll = plain(await FileAttachment(`../data/gb/${observable.params.gov_body}/bands.parquet`).parquet());
```

```js
const placements = selected ? placementsAll.filter((d) => d.name === selected).sort((a, b) => (b.date ?? "").localeCompare(a.date ?? "")) : [];
const titles = placements.filter((d) => d.place === 1).length;
const heroBadges = w
  ? [
      latestRank ? badge(`#${fmtInt(latestRank.rank)} of ${fmtInt(latestRank.n)} · ${shortSeason(latestRank.season)}`, "gold") : null,
      latestRank?.w_rank ? badge(`#${latestRank.w_rank} of ${latestRank.w_n} at ${latestRank.w_key}`) : null,
      titles ? badge(`🏆 ${titles}× champion`) : null
    ].filter(Boolean)
  : [];
```

<div class="hero">
  <img class="hero-logo" src="../assets/logo.png" alt="">
  <div class="hero-body">
    <h1>${w ? formatName(w.name) : `${gbInfo.label} Wrestler Profiles`}</h1>
    <h2>${w ? [w.team, w.w_key, `last match ${fmtDate(w.last_date)}`].filter(Boolean).join(" · ") : gbInfo.fullName}</h2>
    <div style="margin-top:0.9rem">${searchInput}</div>
  </div>
  <div class="hero-side hero-badges">${heroBadges}</div>
</div>

```js
if (!selected) {
  const latest = wrestlers.reduce((m, d) => (d.last_date > m ? d.last_date : m), "");
  const cutoff = new Date(parseDay(latest) - 120 * 864e5).toISOString().slice(0, 10);
  const featured = wrestlers.filter((d) => d.last_date >= cutoff && d.matches >= 10).sort((a, b) => b.elo - a.elo).slice(0, 12);
  display(html`<div class="card">
    <h2>Highest-rated active wrestlers</h2>
    <h3>Pick a wrestler to see their full profile, or search above</h3>
    <ol class="toplist" style="columns: 2 320px; column-gap: 2rem">${featured.map((d, i) => html`<li style="break-inside:avoid"><span class="rk">${i + 1}</span><span class="nm">${nameLink(gb, d.name)}<small>${[d.team, d.w_key].filter(Boolean).join(" · ")}</small></span><span class="val">${fmtInt(d.elo)}</span></li>`)}</ol>
  </div>`);
} else if (!matches.length) {
  display(emptyState("No matches found", "This wrestler has no recorded (non-bye) matches."));
}
```

```js
const wins = matches.filter((d) => d.win).length;
const losses = matches.length - wins;
const fallWins = matches.filter((d) => d.win && d.is_fall).length;
const fallLosses = matches.filter((d) => !d.win && d.is_fall).length;
const peak = matches.reduce((m, d) => (d.post > (m?.post ?? -Infinity) ? d : m), null);
const current = matches.at(-1);
const oppAvg = d3.mean(matches, (d) => d.opp_elo);
const bigWin = matches.filter((d) => d.win && d.opp_elo).sort((a, b) => b.opp_elo - a.opp_elo)[0];
```

```js
if (matches.length) display(html`<div class="grid grid-cols-4">
  ${kpi("Current Elo", fmtInt(current.post), `peak ${fmtInt(peak.post)} · ${fmtDate(peak.date)}`, {tone: "accent"})}
  ${kpi("Career record", record(wins, losses), `${fmtPct(winPct(wins, losses), 1)} wins · ${matches.length} matches`, {tone: "navy"})}
  ${kpi("Falls", `${fallWins}–${fallLosses}`, `${fmtPct(wins ? (100 * fallWins) / wins : null)} of wins by fall · avg. opp. ${fmtInt(oppAvg)}`, {tone: "navy"})}
  ${kpi("Best win", bigWin ? nameLink(gb, bigWin.opp) : "–", bigWin ? `${fmtInt(bigWin.opp_elo)} Elo · ${fmtDate(bigWin.date)}` : "", {tone: "accent"})}
</div>`);
if (placements.length) display(html`<div class="card">
  <h2>Tournament placements</h2>
  <h3>USA Wrestling bracket results</h3>
  <ul class="toplist">${placements.map((p) => html`<li><span>${medal(p.place)}</span><span class="nm">${p.tournament ?? p.event_id}<small>${[p.weight_class, fmtDate(p.date)].filter(Boolean).join(" · ")}</small></span><span class="val muted small">${p.team ?? ""}</span></li>`)}</ul>
</div>`);
```

<div class="card needs-data">
<h2>Elo rating over time</h2>
<h3>Each dot is a match (green = win, orange = loss). Shaded bands: middle 50% and 80% of ranked wrestlers that week; dashed line = median.</h3>

```js
const showOpp = view(Inputs.toggle({label: "Opponents", value: false}));
```

```js
const wrestlerSeasons = new Set(matches.map((d) => d.season));
const bands = bandsAll.filter((d) => wrestlerSeasons.has(d.season)).map((d) => ({...d, day: parseDay(d.wk)}));
const axis = seasonAxis([...matches, ...bands]);
const xpos = axis.x;
display(
  matches.length
    ? resize((width) => Plot.plot({
        width,
        height: 380,
        x: axis.scale,
        y: {label: "Elo", grid: true, nice: true},
        color: {domain: [true, false], range: ["var(--win)", "var(--loss)"]},
        marks: [
          axis.separators,
          Plot.areaY(bands, {x: xpos, y1: "p10", y2: "p90", z: "season", fill: "var(--theme-foreground-faint)", fillOpacity: 0.18, curve: "step-after"}),
          Plot.areaY(bands, {x: xpos, y1: "p25", y2: "p75", z: "season", fill: "var(--theme-foreground-faint)", fillOpacity: 0.25, curve: "step-after"}),
          Plot.lineY(bands, {x: xpos, y: "p50", z: "season", stroke: "var(--theme-foreground-muted)", strokeDasharray: "3,3", strokeWidth: 1, curve: "step-after"}),
          showOpp ? Plot.dot(matches, {x: xpos, y: "opp_elo", r: 2.5, fill: "var(--theme-foreground-muted)", fillOpacity: 0.5}) : null,
          Plot.lineY(matches, {x: xpos, y: "post", z: "season", stroke: "var(--accent-line)", strokeWidth: 2}),
          Plot.dot(matches, {x: xpos, y: "post", fill: "win", r: 3.5, stroke: "var(--theme-background)", strokeWidth: 0.75}),
          Plot.tip(matches, Plot.pointer({
            x: xpos,
            y: "post",
            title: (d) => `${fmtDate(d.date)} · ${d.tournament ?? ""}\n${d.win ? "W" : "L"} vs ${formatName(d.opp)}${d.opp_elo ? ` (${fmtInt(d.opp_elo)})` : ""} · ${resultLabel(d)}\nElo ${fmtInt(d.pre)} → ${fmtInt(d.post)} (${fmtSigned(d.change)})`
          }))
        ]
      }))
    : ""
);
```

</div>

<div class="card needs-data">
<h2>Ranking history</h2>
<h3>Weekly overall and weight-class rank (log scale)</h3>

```js
const rankSeries = ranks.flatMap((d) => [
  {day: d.day, season: d.season, series: "Overall", rank: d.rank, of: d.n},
  ...(d.w_rank ? [{day: d.day, season: d.season, series: "Weight class", rank: d.w_rank, of: d.w_n, w_key: d.w_key}] : [])
]);
display(
  ranks.length
    ? resize((width) => Plot.plot({
        width,
        height: 260,
        x: axis.scale,
        y: {type: "log", reverse: true, label: "Rank", grid: true, domain: [1, Math.max(10, d3.max(rankSeries, (d) => d.rank))], tickFormat: (d) => `#${fmtInt(d)}`, ticks: [1, 3, 10, 25, 100, 250, 1000, 2500, 10000].filter((t) => t <= Math.max(10, d3.max(rankSeries, (d) => d.rank)))},
        color: {domain: ["Overall", "Weight class"], range: ["var(--accent-line)", "var(--brand-gold)"], legend: true},
        marks: [
          axis.separators,
          Plot.lineY(rankSeries, {x: xpos, y: "rank", z: (d) => `${d.series}|${d.season}`, stroke: "series", strokeWidth: 2, curve: "step-after"}),
          Plot.tip(rankSeries, Plot.pointer({x: xpos, y: "rank", stroke: "series", title: (d) => `${d.series}${d.w_key ? ` (${d.w_key})` : ""}\n#${d.rank} of ${fmtInt(d.of)}\nweek of ${fmtDate(d.day)}`}))
        ]
      }))
    : emptyState("Not ranked yet", "Wrestlers are ranked once they have 3 matches in a season.")
);
```

</div>
<div class="card needs-data">
<h2>Season by season</h2>
<h3>Record, Elo and final ranking each season</h3>

```js
const finalRank = d3.rollup(ranks, (v) => v.at(-1), (d) => d.season);
const seasonRows = d3.groups(matches, (d) => d.season).map(([season, v]) => {
  const r = finalRank.get(season);
  return {
    season,
    team: d3.groupSort(v, (g) => -g.length, (d) => d.team).at(0) ?? "–",
    weight: v.findLast((d) => d.w_key)?.w_key ?? v.at(-1).weight ?? "–",
    wins: v.filter((d) => d.win).length,
    losses: v.filter((d) => !d.win).length,
    start: v[0].pre,
    end: v.at(-1).post,
    rank: r?.rank ?? null,
    w_rank: r?.w_rank ? `#${r.w_rank} at ${r.w_key}` : "–"
  };
});
display(
  Inputs.table(seasonRows, {
    columns: ["season", "team", "weight", "wins", "end", "rank", "w_rank"],
    header: {season: "Season", team: "Team", weight: "Wt", wins: "W–L", end: "Final Elo", rank: "Rank", w_rank: "Weight rank"},
    format: {
      season: shortSeason,
      wins: (v, i, data) => record(v, data[i].losses),
      end: (v, i, data) => html`${fmtInt(v)} <span class="delta ${v - data[i].start >= 0 ? "up" : "down"}">${fmtSigned(v - data[i].start)}</span>`,
      rank: (v) => `#${fmtInt(v)}`
    },
    align: {wins: "right", rank: "right"},
    select: false,
    sort: "season",
    reverse: true
  })
);
```

</div>

<div class="needs-data">

```js
const tab = view(Inputs.radio(["Matches", "Opponents", "Upsets", "Teams"], {value: "Matches"}));
```

</div>

```js
const signed = (v) => html`<span class="delta ${v > 0 ? "up" : v < 0 ? "down" : "flat"}">${fmtSigned(v)}</span>`;
const oppLink = (v) => nameLink(gb, v);
let table = null;
if (!matches.length) {
  table = null;
} else if (tab === "Matches") {
  table = Inputs.table([...matches].reverse(), {
    columns: ["date", "tournament", "round", "weight", "opp", "opp_team", "opp_elo", "win", "dec", "change", "post"],
    header: {date: "Date", tournament: "Tournament", round: "Round", weight: "Weight", opp: "Opponent", opp_team: "Opp. team", opp_elo: "Opp. Elo", win: "", dec: "Result", change: "Δ Elo", post: "Elo"},
    format: {date: (v) => fmtDate(v), opp: oppLink, opp_elo: fmtInt, win: resultPill, dec: (v, i, data) => resultLabel(data[i]), change: signed, post: fmtInt},
    width: {date: 100, win: 30, opp_elo: 70, change: 60, post: 60},
    align: {win: "center", opp_elo: "right", change: "right", post: "right"},
    select: false,
    rows: 20.5
  });
} else if (tab === "Opponents") {
  const opps = d3.groups(matches, (d) => d.opp).map(([opp, v]) => ({
    opp,
    team: v.at(-1).opp_team,
    n: v.length,
    wins: v.filter((d) => d.win).length,
    losses: v.filter((d) => !d.win).length,
    last: v.at(-1).date,
    opp_elo: d3.mean(v, (d) => d.opp_elo),
    net: d3.sum(v, (d) => d.change)
  })).sort((a, b) => b.n - a.n || b.opp_elo - a.opp_elo);
  table = Inputs.table(opps, {
    columns: ["opp", "team", "n", "wins", "opp_elo", "net", "last"],
    header: {opp: "Opponent", team: "Team", n: "Met", wins: "W–L", opp_elo: "Avg. opp. Elo", net: "Net Δ Elo", last: "Last met"},
    format: {
      opp: (v) => html`${oppLink(v)} <a class="muted small" title="Head-to-head" href=${pageHref("compare", gb, {a: selected, b: v})}>⇄</a>`,
      wins: (v, i, data) => record(v, data[i].losses),
      opp_elo: fmtInt,
      net: signed,
      last: (v) => fmtDate(v)
    },
    align: {n: "right", wins: "right"},
    select: false,
    rows: 20.5
  });
} else if (tab === "Upsets") {
  const ups = matches
    .filter((d) => Number.isFinite(d.opp_elo) && ((d.win && d.opp_elo > d.pre) || (!d.win && d.opp_elo < d.pre)))
    .map((d) => ({...d, kind: d.win ? "Upset win" : "Upset loss", gap: Math.abs(d.opp_elo - d.pre)}))
    .sort((a, b) => b.gap - a.gap);
  table = ups.length
    ? Inputs.table(ups, {
        columns: ["kind", "gap", "date", "opp", "opp_team", "opp_elo", "pre", "dec", "change"],
        header: {kind: "Type", gap: "Elo gap", date: "Date", opp: "Opponent", opp_team: "Opp. team", opp_elo: "Opp. Elo", pre: "Own Elo", dec: "Result", change: "Δ Elo"},
        format: {kind: (v) => badge(v, v === "Upset win" ? "gold" : ""), gap: fmtInt, date: (v) => fmtDate(v), opp: oppLink, opp_elo: fmtInt, pre: fmtInt, dec: (v, i, data) => resultLabel(data[i]), change: signed},
        select: false,
        rows: 20.5
      })
    : emptyState("No upsets", "Every result went the way the ratings predicted.");
} else if (tab === "Teams") {
  const teams = d3.groups(matches, (d) => d.team ?? "–").map(([team, v]) => ({
    team,
    seasons: [...new Set(v.map((d) => shortSeason(d.season)))].join(", "),
    n: v.length,
    wins: v.filter((d) => d.win).length,
    losses: v.filter((d) => !d.win).length,
    falls: v.filter((d) => d.win && d.is_fall).length,
    net: d3.sum(v, (d) => d.change)
  }));
  table = Inputs.table(teams, {
    columns: ["team", "seasons", "n", "wins", "falls", "net"],
    header: {team: "Team", seasons: "Seasons", n: "Matches", wins: "W–L", falls: "Falls", net: "Net Δ Elo"},
    format: {wins: (v, i, data) => record(v, data[i].losses), net: signed},
    select: false
  });
}
if (table) display(table);
```

```js
if (matches.length) {
  const download = html`<button class="btn">📥 Download match history (.xlsx)</button>`;
  download.onclick = async () => {
    const XLSX = await import("npm:xlsx");
    const rows = matches.map((d) => ({
      Date: d.day ? d.day.toLocaleDateString("en-US") : "",
      Tournament: d.tournament ?? d.event_id ?? "",
      Round: d.round ?? "",
      Weight: d.weight ?? "",
      Team: d.team ?? "",
      Opponent: d.opp ?? "",
      "Opponent Team": d.opp_team ?? "",
      Result: d.win ? "Win" : "Loss",
      "Decision Type": d.dec ?? "",
      Score: Number.isFinite(toNum(d.margin)) ? `${d.win ? "+" : "-"}${d.margin}` : "",
      "Match Time": fmtFallTime(d.fall_s),
      "Pre Elo": Math.round(d.pre),
      "Post Elo": Math.round(d.post),
      "Elo Change": Math.round(d.change),
      "Opponent Pre Elo": Number.isFinite(d.opp_elo) ? d.opp_elo : ""
    }));
    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.json_to_sheet(rows);
    ws["!cols"] = [12, 40, 20, 12, 25, 25, 25, 8, 15, 8, 10, 9, 9, 10, 15].map((wch) => ({wch}));
    XLSX.utils.book_append_sheet(wb, ws, "Matches");
    XLSX.writeFile(wb, `${selected.replace(/[^a-zA-Z0-9]/g, "_")}_matches_${new Date().toISOString().slice(0, 10)}.xlsx`);
  };
  display(html`<div class="toolbar">${download}<a class="btn" href=${pageHref("compare", gb, {a: selected})}>⇄ Compare with another wrestler</a><a class="btn" href=${pageHref("rankings", gb)}>🏅 ${gbInfo.label} rankings</a></div>`);
}
```

<details class="method">
<summary>What is an Elo rating?</summary>

Elo is a rating system (made famous by chess) that estimates relative skill from head-to-head results. Everyone starts at 1000. After each match the winner takes points from the loser — only a few if the favorite wins, many for an upset — so ratings self-correct over time. A 200-point edge means the higher-rated wrestler is expected to win about 76% of the time. Ratings drift back toward 1000 during long layoffs.

</details>
