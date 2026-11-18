import {html} from "npm:htl";
import {fmtInt, formatName, wrestlerHref} from "./format.js";

export function kpi(label, value, sub = "", {tone = ""} = {}) {
  return html`<div class="card kpi ${tone}">
    <div class="kpi-label">${label}</div>
    <div class="kpi-value">${value}</div>
    ${sub ? html`<div class="kpi-sub">${sub}</div>` : ""}
  </div>`;
}

export function emptyState(title, text = "") {
  return html`<div class="empty-state"><h3>${title}</h3>${text ? html`<p>${text}</p>` : ""}</div>`;
}

export function badge(text, tone = "") {
  return html`<span class="badge ${tone}">${text}</span>`;
}

// Rank movement indicator: positive = moved up.
export function delta(n, {suffix = ""} = {}) {
  if (!Number.isFinite(n) || n === 0) return html`<span class="delta flat">–</span>`;
  return n > 0
    ? html`<span class="delta up">▲${Math.abs(n)}${suffix}</span>`
    : html`<span class="delta down">▼${Math.abs(n)}${suffix}</span>`;
}

export function medal(place) {
  const cls = place === 1 ? "gold" : place === 2 ? "silver" : place === 3 ? "bronze" : "";
  return html`<span class="medal ${cls}" title="${place} place">${place}</span>`;
}

export function nameLink(govBody, name, {display} = {}) {
  if (!name) return "–";
  return html`<a class="wrestler-link" href=${wrestlerHref(govBody, name)}>${display ?? formatName(name)}</a>`;
}

export function resultPill(win) {
  return html`<span class="pill ${win ? "win" : "loss"}">${win ? "W" : "L"}</span>`;
}

/**
 * Type-ahead wrestler search. Works as an Observable input: `.value` is the selected
 * wrestler's raw name (or null) and an "input" event fires on selection.
 * Rows need {name, team, elo, matches, w_key?}.
 */
export function wrestlerSearch(rows, {value = null, placeholder = "Search wrestlers by name or team…", label = "", limit = 10} = {}) {
  const index = rows.map((d) => ({
    d,
    hay: `${d.name} ${d.team ?? ""}`.toLowerCase(),
    words: `${d.name} ${d.team ?? ""}`.toLowerCase().split(/[\s,.'-]+/).filter(Boolean)
  }));
  const byName = new Map(rows.map((d) => [d.name, d]));

  const input = html`<input type="search" autocomplete="off" spellcheck="false" placeholder=${placeholder} aria-label=${label || placeholder}>`;
  const list = html`<ul class="ws-list" role="listbox" hidden></ul>`;
  const root = html`<div class="ws">${label ? html`<label class="ws-label">${label}</label>` : ""}<div class="ws-box">${input}${list}</div></div>`;

  let results = [];
  let active = -1;
  let current = byName.has(value) ? value : null;
  input.value = current ? formatName(current) : "";

  function search(q) {
    const tokens = q.toLowerCase().split(/[\s,]+/).filter(Boolean);
    if (!tokens.length) return [];
    const out = [];
    for (const e of index) {
      if (tokens.every((t) => e.words.some((w) => w.startsWith(t)) || (t.length > 2 && e.hay.includes(t)))) out.push(e.d);
    }
    return out.sort((a, b) => (b.matches ?? 0) - (a.matches ?? 0)).slice(0, limit);
  }

  function render() {
    list.replaceChildren(
      ...results.map((d, i) =>
        html`<li role="option" class=${i === active ? "active" : ""} onmousedown=${(e) => (e.preventDefault(), choose(d.name))}>
          <span class="ws-name">${formatName(d.name)}</span>
          <span class="ws-meta">${[d.team, d.w_key ? `${d.w_key}` : null].filter(Boolean).join(" · ")}</span>
          <span class="ws-elo">${fmtInt(d.elo)}</span>
        </li>`
      )
    );
    list.hidden = results.length === 0;
  }

  function choose(name) {
    current = name;
    input.value = name ? formatName(name) : "";
    results = [];
    render();
    input.blur();
    root.dispatchEvent(new Event("input", {bubbles: true}));
  }

  input.addEventListener("input", (e) => {
    e.stopPropagation();
    results = search(input.value);
    active = results.length ? 0 : -1;
    render();
  });
  input.addEventListener("keydown", (e) => {
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      if (!results.length) return;
      active = (active + (e.key === "ArrowDown" ? 1 : -1) + results.length) % results.length;
      render();
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (results[active]) choose(results[active].name);
    } else if (e.key === "Escape") {
      results = [];
      render();
      input.value = current ? formatName(current) : "";
    }
  });
  input.addEventListener("focus", () => input.select());
  input.addEventListener("blur", () => {
    results = [];
    render();
    if (!input.value.trim() && current) choose(null);
    else input.value = current ? formatName(current) : "";
  });

  Object.defineProperty(root, "value", {
    get: () => current,
    set: (v) => {
      current = byName.has(v) ? v : null;
      input.value = current ? formatName(current) : "";
    }
  });
  return root;
}
