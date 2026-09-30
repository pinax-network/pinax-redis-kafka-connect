# Usage alert email

The Mandrill template the connector sends through when a team crosses a usage milestone (50, 75, 90, 100, 150 or
200% of its included credits). Live name: `usage-notifications-template`, the `templateSlug` in prod and dev.

| File | What it is |
|---|---|
| `build.py` | Generates the per-plan, per-milestone copy. **Edit this for copy changes.** |
| `usage-notifications.src.html` | Layout: header, usage box, button, footer. Slots `{{PREVIEW}}`, `{{HERO}}`, `{{NEXT}}` are filled by `build.py`. |
| `usage-notifications.html`, `.txt` | Generated. What is published to Mandrill. Don't edit by hand. |

## How the template works

- **Merge language:** Mailchimp (`*|TeamName|*`, `*|IF:…|*`), because the connector doesn't set `merge_language`.
- **Merge vars:** see the connector [README](../../README.md#notification-milestones). `UsagePercent` is optional.
- **Milestone from the amount:** Mandrill can compare a merge var with a literal, not with another merge var.
  - Free ($25 included) and Pro ($200 included) work out the milestone from `BilledCredits` with fixed bounds.
  - That's exact, because the connector notifies the highest milestone at or below the amount used.
  - **If the Free or Pro allowance changes, update `FREE`/`PRO` in `build.py` and republish.**
- **Fallbacks:** the email falls back when:
  - the included amount isn't the expected one;
  - the amount has a thousands comma (`1,250.00` doesn't compare as a number);
  - the plan is something else, e.g. enterprise.

  It then uses `UsagePercent` if the connector sends it. Otherwise it shows "$X of $Y used this month".
- **No unsubscribe link, by design.** These are service notices: service pausing, overage billing. A Mandrill
  unsubscribe would block all mail from `info@` to that address.
- **The subject is set by the connector** (`RedisSinkTask.usageMailSubject`), which overrides the template's subject.
- **Typographic apostrophes (’):** `build.py` converts them, because Space Mono draws a straight `'` as a heavy tick.

## Changing it

```bash
python3 emails/usage-notifications/build.py              # regenerate after editing build.py or the layout
scripts/sync-mandrill-usage-template.sh                  # dry run: does live match the repo?
scripts/sync-mandrill-usage-template.sh --stage          # publish to usage-notifications-template-v2 (unused)
scripts/sync-mandrill-usage-template.sh --test you@pinax.network
scripts/sync-mandrill-usage-template.sh --apply          # publish live; prints the backup path
scripts/sync-mandrill-usage-template.sh --restore <file> # roll back to that backup
```

Commit the regenerated files before publishing. The script refuses to publish if the committed files don't match
`build.py`.
