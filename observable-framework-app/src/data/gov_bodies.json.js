// Data loader: emits a JSON array of governing bodies from the repo-root governing_bodies.json.
import * as fs from "node:fs";
import * as path from "node:path";
import * as url from "node:url";

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
const govBodiesPath = path.resolve(__dirname, "..", "..", "..", "governing_bodies.json");

const govBodies = JSON.parse(fs.readFileSync(govBodiesPath, "utf8"));

const result = govBodies
  .map((gb) => {
    const suffix = gb.acronym.toLowerCase();
    return {
      suffix,
      label: gb.acronym.replace(/_/g, " "),
      fullName: gb.name ?? null,
    };
  })
  .sort((a, b) => a.suffix.localeCompare(b.suffix));

process.stdout.write(JSON.stringify(result));
