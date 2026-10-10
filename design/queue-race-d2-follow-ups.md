# Queue-race follow-ups

These items are not part of the queue-race fix and are tracked separately.

| Area | Follow-up |
|---|---|
| Continue-as-new admission | Remove the admission window in providers: write the continue-as-new start so no fetch cutoff can exclude it, and delay abandoned instances with an instance cooldown instead of re-timing rows. Until then a waiting instance is re-fetched on the unregistered-handler backoff and poisoned after `max_attempts` if the start never appears. |
| Collision wording | Include terminal-after-continue-as-new status in collision-notification wording. |
| Dropped abandon futures | Await the pre-existing dropped `abandon_orchestration_item` futures, or remove them intentionally. |
| Collision on poison | Notify a colliding start's parent when the same batch is consumed by poison handling. |
| Schedule lookup | Replace the O(n) schedule-to-token lookup with a reverse map. |
| Successor first-turn commit failure | When a successor's first-turn commit fails, the infrastructure-failure path still builds its failure from the previous execution's history and commits it under the successor's execution id (related to #58). Poison already records the successor as its own execution. |
