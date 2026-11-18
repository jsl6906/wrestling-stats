// See https://observablehq.com/framework/config for documentation.
import * as fs from "node:fs";
import * as path from "node:path";

// Governing bodies are the single source of truth for routes (shared with the Python pipeline and CI)
const govBodies = JSON.parse(fs.readFileSync(path.join(process.cwd(), "..", "governing_bodies.json"), "utf8"));
const suffixes = govBodies.map((gb) => gb.acronym.toLowerCase());
const bySuffix = new Map(govBodies.map((gb) => [gb.acronym.toLowerCase(), gb]));

// Per-governing-body pages; [gov_body].md files are rendered once per suffix.
const sections = [
  {name: "Rankings", dir: "rankings"},
  {name: "Wrestler Profiles", dir: "individual_stats"},
  {name: "Leaderboards", dir: "leaderboards"},
  {name: "Head-to-Head", dir: "compare"}
];

const escapeHtml = (s) => String(s).replace(/[&<>"]/g, (c) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"})[c]);

export default {
  title: "Wrestling Stats Explorer",

  pages: suffixes.map((suffix) => ({
    name: bySuffix.get(suffix).acronym.replace(/_/g, " "),
    open: false,
    pages: sections.map(({name, dir}) => ({name, path: `/${dir}/${suffix}`}))
  })),

  head: `<link rel="icon" href="favicon.svg" type="image/svg+xml">
<link rel="apple-touch-icon" href="assets/apple-touch-icon.png">
<meta name="theme-color" content="#0b2545">`,

  header: ({path: pagePath}) => {
    const gb = bySuffix.get(pagePath.split("/")[2]);
    const depth = pagePath.split("/").length - 2;
    const home = depth > 0 ? "../".repeat(depth) : "./";
    return `<div class="brand">
  <a href="${home}"><img src="assets/logo-64.png" alt=""><span>Wrestling Stats Explorer</span></a>
  ${gb ? `<span class="brand-sub">/ ${escapeHtml(gb.name)}</span>` : ""}
</div>`;
  },

  footer: `Elo ratings & rankings computed from public match results (Trackwrestling, USA Wrestling). Unofficial — for fun and scouting.`,

  style: "style.css",
  root: "src",
  toc: false,
  pager: false,
  search: true,

  interpreters: {
    ".py": ["uv", "run", "python"],
    ".sas7bdat": ["sas"]
  },

  async *dynamicPaths() {
    for (const suffix of suffixes) {
      for (const {dir} of sections) yield `/${dir}/${suffix}`;
    }
  }
};
