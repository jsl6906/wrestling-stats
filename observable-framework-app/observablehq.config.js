// See https://observablehq.com/framework/config for documentation.
import * as fs from "node:fs";
import * as path from "node:path";

// Read database files and extract suffixes (done at config time)
const outputDir = path.join(process.cwd(), "..", "output");
const files = fs.readdirSync(outputDir);
const suffixes = files
  .filter(file => file.startsWith("trackwrestling_") && file.endsWith(".db"))
  .map(file => file.replace("trackwrestling_", "").replace(".db", ""));

export default {
  // The app’s title; used in the sidebar and webpage titles.
  title: "TrackWrestling Tournament Stats Explorer",

  // The pages and sections in the sidebar. If you don’t specify this option,
  // all pages will be listed in alphabetical order. Listing pages explicitly
  // lets you organize them into sections and have unlisted pages.
  pages: suffixes.map(suffix => ({
    name: suffix.toUpperCase(),
    open: true,
    pages: [
      {name: "Individual Stats", path: `/individual_stats/${suffix}`}
    ]
  })),

  // Content to add to the head of the page, e.g. for a favicon:
  head: '<link rel="icon" href="observable.png" type="image/png" sizes="32x32">',

  // The path to the source root.
  root: "src",

  interpreters: {
    ".py": ["uv", "run", "python"],
    ".sas7bdat": ["sas"]
  },

  // Some additional configuration options and their defaults:
  // theme: "default", // try "light", "dark", "slate", etc.
  // header: "", // what to show in the header (HTML)
  // footer: "Built with Observable.", // what to show in the footer (HTML)
  // sidebar: true, // whether to show the sidebar
  // toc: true, // whether to show the table of contents
  // pager: true, // whether to show previous & next links in the footer
  // output: "dist", // path to the output root for build
  // search: true, // activate search
  // linkify: true, // convert URLs in Markdown to links
  // typographer: false, // smart quotes and other typographic improvements
  // preserveExtension: false, // drop .html from URLs
  // preserveIndex: false, // drop /index from URLs

  async *dynamicPaths() {
    // Read database files from output directory
    const outputDir = path.join(process.cwd(), "..", "output");
    const files = fs.readdirSync(outputDir);
    
    // Filter for .db files and extract the suffix (e.g., "nvwf" from "trackwrestling_nvwf.db")
    const suffixes = files
      .filter(file => file.startsWith("trackwrestling_") && file.endsWith(".db"))
      .map(file => file.replace("trackwrestling_", "").replace(".db", ""));
    
    // Yield a path for each suffix
    for (const suffix of suffixes) {
      yield `/individual_stats/${suffix}`;
    }
  }
};
