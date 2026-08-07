# JIIG 2.0 Product Review

This review evaluates JIIG against its primary product promise: when a Lakeflow Job or Pipeline is failing, a responder should immediately understand the downstream exposure, causal dependencies, business-facing consumers, and owners who need to act.

## P0 — Trust and incident correctness

| Finding | Why it mattered | JIIG 2.0 status |
|---|---|---|
| Failure history was treated as an active incident for the full lookback window | A recovered Job could remain red for seven days | Fixed: the latest run/update must still be failed |
| ROOT/CASCADE used only direct failed-to-failed edges | `failed A → healthy B → failed C` incorrectly labeled C as root | Fixed: server-side multi-hop reach collapses paths between failed nodes, then failure timing is checked |
| Incident list and subgraph used different failure windows | A node outside the selected window could still render as failed | Fixed: subgraph and explorer status are recomputed with the selected window |
| Raw IDs discarded entity kind | A Job and Pipeline with the same raw ID could merge into one graph node | Fixed: graph identity is canonical `KIND:id`; raw ID is retained as `entity_id` |
| Missing SCD2 metadata hid real failures | Run history or lineage could prove a Job exists while the UI marked it healthy or omitted it | Fixed: unregistered Job/Pipeline nodes preserve failure history and lineage owner fallback |
| App runtime did not bind explicitly to the Databricks Apps port/address | Deployment could return 502 or fail behind the Apps proxy | Fixed: `run_app.py` binds to `0.0.0.0:$DATABRICKS_APP_PORT`; CORS/XSRF proxy settings are explicit |
| Bundle was tied to one person's workspace, root path, and production permissions | Another customer could not deploy safely | Fixed: hard-coded host, user path, and permission were removed; profile and required workspace ID are explicit |

## P1 — Core product comprehension

| Finding | Improvement |
|---|---|
| First screen was a large incident table that required manual selection | Highest-impact incident is selected automatically with exposure, business surfaces, owners, and evidence summary |
| Graph was visually detached from the table-level cause | Every affected row now includes a causal path with intermediate tables or trigger evidence |
| Blast-radius label did not match the selected depth | Queue reach is calculated server-side for the active depth and labeled with that depth |
| Hub/authority insight was not visible and overview omitted degree columns | Dedicated intelligence view shows top hubs, authorities, and a dependency position map |
| “Critical tables” counted edge appearances, not unique exposed assets | Tables now rank by the unique downstream assets reachable through them |
| Leaf consumers could link to invalid Pipeline URLs | Deep links are only shown for supported Job/Pipeline asset types |
| Notification draft omitted affected kinds with unresolved owners | All affected kinds remain in the brief; unresolved ownership is an explicit action item |
| Freshness was workload activity rather than graph build time | `snapshot_time` is written into the graph output and used by the app |
| All Streamlit tabs executed even when hidden | Navigation renders only the selected product surface |
| Full-graph criticality sampling broke paths and produced a hairball | Explorer renders a connected ego graph around a searched asset |

## P2 — Product polish and operations

| Finding | Improvement |
|---|---|
| Stock Streamlit styling, emoji semantics, weak hierarchy | Introduced an operations-console design system with semantic incident color, compact navigation, consistent cards, and responsive layout |
| Color alone carried graph meaning | Graph legend combines color with textual role and entity icon; evidence table provides a non-visual path alternative |
| No incident search or focus control | Searchable focused-incident and asset selectors are first-class controls |
| No data-quality surface | Operations view exposes snapshot age, owner gaps, asset coverage, and shared-write exclusions |
| README screenshots were stale and exposed real workspace data | Replaced with browser-rendered anonymous demo screenshots generated from repository code |
| No tests | Added graph traversal, time-aware cascade, critical-table, canonical-ID, and demo contract tests |

## Remaining high-value gaps

These are not hidden by the UI and should guide future development:

1. **Task-level incident evidence** — current lineage identifies what the Job or Pipeline has written in the observation window, not the exact output of the failed task/run.
2. **Configured orchestration edges** — Run Job task dependencies should be extracted directly when a stable native source is available.
3. **Temporal lineage confidence** — edge first/last seen time and access frequency would distinguish active contracts from stale historical relationships.
4. **Business-aware centrality** — current hub/authority ranks are structural. Weighted terminal reach, usage frequency, business tier, failure rate, and ownership concentration would improve prioritization.
5. **Incident lifecycle** — acknowledgement, assignment, notification delivery, recovery confirmation, and post-incident history require a persistent operational store rather than read-only analytics tables.
6. **Owner model** — human owner, run-as principal, team, on-call target, source, and confidence should become separate fields.

## Release acceptance criteria

- Opening the app with incidents selects the largest scoped blast radius without another click.
- A recovered workload is not shown as an open incident.
- A failed node reached through healthy intermediates can be labeled likely cascade.
- Every graph edge returned to the client has both canonical endpoints present.
- Every affected asset has a textual path; table evidence is shown when available.
- Owner gaps and stale snapshots are visible, never silently treated as healthy.
- The app can deploy without a hard-coded user, workspace host, or root path.
- Documentation screenshots come from anonymous fixture data and the current application code.
