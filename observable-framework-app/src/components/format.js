// Shared formatting and domain helpers. Keep SHARD_COUNT / MIN_SEASON_MATCHES / shardOf in sync
// with code/site_queries.py.

export const SHARD_COUNT = 32;
export const MIN_SEASON_MATCHES = 3;

export function shardOf(name) {
  let sum = 0;
  for (const ch of name) sum += ch.codePointAt(0);
  return sum % SHARD_COUNT;
}

export const shardId = (name) => String(shardOf(name)).padStart(2, "0");

// "First Middle Last" -> "Last, First Middle"; rejoins split hyphenated names ("Cam Cook -Cash").
export function formatName(name) {
  if (!name || typeof name !== "string") return name || "–";
  const parts = name.trim().split(/\s+/);
  for (let i = parts.length - 1; i > 0; i--) {
    if (parts[i].startsWith("-")) {
      parts[i - 1] += parts[i];
      parts.splice(i, 1);
    }
  }
  if (parts.length === 1) return parts[0];
  const suffix = parts.length > 2 && SUFFIXES.test(parts.at(-1)) ? parts.pop() : null;
  let start = parts.length - 1;
  while (start > 1 && PARTICLES.test(parts[start - 1])) start--;
  const last = parts.slice(start).join(" ") + (suffix ? ` ${suffix}` : "");
  return `${last}, ${parts.slice(0, start).join(" ")}`;
}

const PARTICLES = /^(van|von|de|del|della|der|den|da|di|du|la|le|st\.?|mc|mac)$/i;
const SUFFIXES = /^(jr\.?|sr\.?|ii|iii|iv|v)$/i;

// Parse "YYYY-MM-DD" as a local date (new Date("YYYY-MM-DD") is UTC and shows the prior day in the US).
export function parseDay(v) {
  if (v == null) return null;
  if (v instanceof Date) return v;
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(v));
  if (m) return new Date(+m[1], +m[2] - 1, +m[3]);
  const d = new Date(v);
  return isNaN(d) ? null : d;
}

export const toNum = (v) => (v == null ? null : typeof v === "bigint" ? Number(v) : Number.isFinite(+v) ? +v : null);

// Arrow rows are proxies with BigInt fields; normalize to plain objects with numbers.
export function plain(table) {
  return Array.from(table, (row) => {
    const o = {};
    for (const [k, v] of Object.entries(row.toJSON ? row.toJSON() : row)) o[k] = typeof v === "bigint" ? Number(v) : v;
    return o;
  });
}

export function seasonOf(date) {
  const d = parseDay(date);
  if (!d) return null;
  const y = d.getMonth() >= 8 ? d.getFullYear() : d.getFullYear() - 1;
  return `${y}-${y + 1}`;
}

export const shortSeason = (s) => (s ? s.replace(/^(\d{4})-\d{2}(\d{2})$/, "$1–$2") : s);

const DATE_OPTS = {month: "short", day: "numeric", year: "numeric"};
// Ignore non-object second args so it can be passed directly as an Inputs.table formatter (value, index, data).
export const fmtDate = (v, opts) => {
  const d = parseDay(v);
  return d ? d.toLocaleDateString("en-US", opts && typeof opts === "object" ? opts : DATE_OPTS) : "–";
};

export const fmtInt = (v) => (Number.isFinite(toNum(v)) ? Math.round(toNum(v)).toLocaleString("en-US") : "–");
export function fmtPct(v, digits, data) {
  // A third arg means we're being called as an Inputs.table formatter, where `digits` is the row index.
  const dp = data === undefined && typeof digits === "number" ? digits : 0;
  return Number.isFinite(toNum(v)) ? `${toNum(v).toFixed(dp)}%` : "–";
}
export const fmtSigned = (v) => {
  const n = Math.round(toNum(v) ?? NaN);
  return Number.isFinite(n) ? (n > 0 ? `+${n}` : n < 0 ? `−${Math.abs(n)}` : "0") : "–";
};
export const record = (w, l) => `${w ?? 0}–${l ?? 0}`;
export const winPct = (w, l) => (w + l > 0 ? (100 * w) / (w + l) : null);
export const ordinal = (n) => {
  const s = ["th", "st", "nd", "rd"], v = n % 100;
  return n + (s[(v - 20) % 10] || s[v] || s[0]);
};

export const isWin = (result) => result === "W" || result === "winner";

// Elo expected score (same formula as code/calculate_elo.py)
export const expectedScore = (ra, rb) => 1 / (1 + 10 ** ((rb - ra) / 400));

export function fmtFallTime(seconds) {
  const s = toNum(seconds);
  if (!s || s <= 0) return "";
  return `${Math.floor(s / 60)}:${String(Math.round(s % 60)).padStart(2, "0")}`;
}

// Short result label like "Fall 1:23", "Dec 7-3" style from available fields
export function resultLabel(m) {
  const dec = m.dec_code || m.dec || "";
  if (m.is_fall && m.fall_s) return `${dec || "Fall"} ${fmtFallTime(m.fall_s)}`;
  if (Number.isFinite(toNum(m.margin)) && toNum(m.margin) > 0) return `${dec} (+${m.margin})`.trim();
  return dec || "–";
}

// Sort weight-class keys numerically, grouping by age prefix ("12U 78" after "10U 85").
export function compareWeightKeys(a, b) {
  const pa = /^(.*?)\s*(\d+)$/.exec(a) || ["", a, "0"];
  const pb = /^(.*?)\s*(\d+)$/.exec(b) || ["", b, "0"];
  const age = (s) => (s ? parseInt(s) || 1000 + s.charCodeAt(0) : 0);
  return age(pa[1]) - age(pb[1]) || pa[1].localeCompare(pb[1]) || +pa[2] - +pb[2];
}

// Standard competition ranking (1, 2, 2, 4) over rows sorted by `value` descending.
export function competitionRank(rows, value, key = "rank") {
  const sorted = [...rows].sort((a, b) => value(b) - value(a));
  let prev, rank = 0;
  return sorted.map((d, i) => {
    const v = value(d);
    if (i === 0 || v !== prev) rank = i + 1;
    prev = v;
    return {...d, [key]: rank};
  });
}

export function pageHref(page, govBody, params = {}) {
  const q = new URLSearchParams(Object.entries(params).filter(([, v]) => v != null && v !== ""));
  return `../${page}/${govBody}${q.size ? `?${q}` : ""}`;
}

export const wrestlerHref = (govBody, name) => pageHref("individual_stats", govBody, {wrestler: name});

export function getUrlParam(name) {
  return new URL(window.location).searchParams.get(name);
}

export function setUrlParams(params) {
  const url = new URL(window.location);
  for (const [k, v] of Object.entries(params)) {
    if (v == null || v === "") url.searchParams.delete(k);
    else url.searchParams.set(k, v);
  }
  window.history.replaceState(window.history.state, "", url);
}
