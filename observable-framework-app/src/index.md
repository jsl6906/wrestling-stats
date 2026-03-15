---
toc: false
---

```js
const govBodies = await FileAttachment("data/gov_bodies.json").json();
```

# TrackWrestling Tournament Stats Explorer

This site presents data on the matches of ${govBodies.map(g => g.label).join(", ")}, as reflected in [Trackwrestling.com](https://www.trackwrestling.com/Login.jsp).

## Quick Navigation

```js
{
  const container = document.createElement("div");
  for (const { suffix, label, fullName } of govBodies) {
    const section = document.createElement("div");
    section.innerHTML = `
      <h3>${label}${fullName ? ` (${fullName})` : ""}</h3>
      <ul>
        <li><a href="/individual_stats/${suffix}">Individual Stats</a></li>
        <li><a href="/leaderboards/${suffix}">Leaderboards</a></li>
      </ul>
    `;
    container.appendChild(section);
  }
  display(container);
}
```
