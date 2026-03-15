// Data loader: emits a JSON array of governing body suffixes found in the output directory.
import * as fs from "node:fs";
import * as path from "node:path";
import * as url from "node:url";

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
const outputDir = path.resolve(__dirname, "..", "..", "..", "output");

const knownNames = {
  nvwf: "Northern Virginia Wrestling Federation",
  vhsl: "Virginia High School League",
  nysphsaa: "New York State Public High School Athletic Association",
  va_usa: "Virginia USA Wrestling",
};

const suffixes = fs
  .readdirSync(outputDir)
  .filter((f) => f.startsWith("trackwrestling_") && f.endsWith(".db"))
  .map((f) => f.replace("trackwrestling_", "").replace(".db", ""))
  .sort();

const result = suffixes.map((suffix) => ({
  suffix,
  label: suffix.toUpperCase().replace(/_/g, " "),
  fullName: knownNames[suffix] ?? null,
}));

process.stdout.write(JSON.stringify(result));
