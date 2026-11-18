import * as Plot from "npm:@observablehq/plot";
import * as d3 from "npm:d3";
import {shortSeason} from "./format.js";

/**
 * Compressed time axis that lays seasons side by side, skipping off-season gaps.
 * `rows` need {season, day}; x positions are in days from the first season's start.
 */
export function seasonAxis(rows, {gap = 12} = {}) {
  const segments = [];
  for (const [season, days] of d3.sort(d3.group(rows, (d) => d.season), ([s]) => s)) {
    const [start, end] = d3.extent(days, (d) => d.day);
    const prev = segments.at(-1);
    const offset = prev ? prev.offset + (prev.end - prev.start) / 864e5 + gap : 0;
    segments.push({season, start, end, offset, mid: offset + (end - start) / 864e5 / 2});
  }
  const bySeason = new Map(segments.map((s) => [s.season, s]));
  const x = (d) => {
    const s = bySeason.get(d.season);
    return s ? s.offset + (d.day - s.start) / 864e5 : NaN;
  };
  const last = segments.at(-1);
  return {
    segments,
    x,
    scale: {
      label: null,
      tickSize: 0,
      ticks: segments.map((s) => s.mid),
      tickFormat: (v) => shortSeason(segments.find((s) => s.mid === v)?.season),
      domain: last ? [0, last.offset + (last.end - last.start) / 864e5] : undefined
    },
    separators: Plot.ruleX(segments.slice(1), {x: (s) => s.offset - gap / 2, stroke: "var(--theme-foreground-faintest)", strokeWidth: 1})
  };
}
