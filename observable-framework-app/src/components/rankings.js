import {MIN_SEASON_MATCHES, compareWeightKeys, competitionRank, plain} from "./format.js";

/**
 * Season power rankings from seasons.parquet rows: ranked by end-of-season (current) Elo among
 * wrestlers with MIN_SEASON_MATCHES+ matches. Also computes the rank 30 days earlier (from elo_30,
 * among wrestlers already eligible then) to show movement.
 */
export function seasonRankings(rows) {
  const eligible = rows.filter((d) => d.matches >= MIN_SEASON_MATCHES && Number.isFinite(d.end_elo));
  const ranked = competitionRank(eligible, (d) => d.end_elo);
  const before = competitionRank(
    eligible.filter((d) => d.matches - d.m30 >= MIN_SEASON_MATCHES && Number.isFinite(d.elo_30)),
    (d) => d.elo_30,
    "rank30"
  );
  const rank30 = new Map(before.map((d) => [d.name, d.rank30]));
  return ranked.map((d) => {
    const r30 = rank30.get(d.name) ?? null;
    return {
      ...d,
      rank30: r30,
      move: r30 == null ? "new" : r30 - d.rank,
      elo_change30: Number.isFinite(d.elo_30) ? d.end_elo - d.elo_30 : null,
      season_change: Number.isFinite(d.start_elo) ? d.end_elo - d.start_elo : null
    };
  });
}

/** Weight-class rankings: { key -> ranked rows } for weights with at least `minSize` ranked wrestlers. */
export function weightRankings(ranked, {minSize = 5} = {}) {
  const groups = new Map();
  for (const d of ranked) {
    if (!d.w_key) continue;
    if (!groups.has(d.w_key)) groups.set(d.w_key, []);
    groups.get(d.w_key).push(d);
  }
  return new Map(
    [...groups]
      .filter(([, v]) => v.length >= minSize)
      .sort(([a], [b]) => compareWeightKeys(a, b))
      .map(([k, v]) => [k, competitionRank(v, (d) => d.end_elo, "w_rank")])
  );
}

/**
 * Team power rankings: mean Elo of each team's top `depth` ranked wrestlers (needs `minSize`).
 */
export function teamRankings(ranked, {depth = 10, minSize = 5} = {}) {
  const teams = new Map();
  for (const d of ranked) {
    if (!d.team) continue;
    if (!teams.has(d.team)) teams.set(d.team, []);
    teams.get(d.team).push(d);
  }
  const rows = [];
  for (const [team, members] of teams) {
    if (members.length < minSize) continue;
    members.sort((a, b) => b.end_elo - a.end_elo);
    const top = members.slice(0, depth);
    const mean = top.reduce((s, d) => s + d.end_elo, 0) / top.length;
    const mean30 = top.filter((d) => Number.isFinite(d.elo_30));
    rows.push({
      team,
      score: mean,
      score30: mean30.length ? mean30.reduce((s, d) => s + d.elo_30, 0) / mean30.length : null,
      ranked: members.length,
      top25: members.filter((d) => d.rank <= 25).length,
      best: members[0],
      wins: members.reduce((s, d) => s + d.wins, 0),
      losses: members.reduce((s, d) => s + d.losses, 0),
      members: top
    });
  }
  return competitionRank(rows, (d) => d.score);
}

export function seasonList(rows) {
  return [...new Set(rows.map((d) => d.season))].sort().reverse();
}

export {plain};
