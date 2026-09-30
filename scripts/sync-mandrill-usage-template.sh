#!/usr/bin/env bash
# Publishes emails/usage-notifications/ to Mandrill. The connector sends through the template named by its
# templateSlug config (usage-notifications-template in prod and dev), so publishing there changes live email.
#
#   scripts/sync-mandrill-usage-template.sh                 # dry run: does the live template match the repo?
#   scripts/sync-mandrill-usage-template.sh --stage         # publish to the staging name (not used by the connector)
#   scripts/sync-mandrill-usage-template.sh --test EMAIL    # send one sample per plan/milestone through staging
#   scripts/sync-mandrill-usage-template.sh --apply         # publish to the live template (saves a backup first)
#   scripts/sync-mandrill-usage-template.sh --restore FILE  # republish a backup saved by --apply
#
# Needs MANDRILL_API_KEY in the environment, plus curl, jq and python3. Only --test sends email, to EMAIL only.
set -euo pipefail

: "${MANDRILL_API_KEY:?set MANDRILL_API_KEY}"
LIVE="usage-notifications-template"
STAGING="usage-notifications-template-v2"
dir="$(cd "$(dirname "$0")/../emails/usage-notifications" && pwd)"
api="https://mandrillapp.com/api/1.0"
call() { curl -sS "$api/$1" -H 'Content-Type: application/json' -d "$(jq -c --arg key "$MANDRILL_API_KEY" '. + {key:$key}' <<<"$2")"; }
fail_on_error() { jq -e 'type == "object" and .status == "error"' <<<"$1" >/dev/null && { echo "Mandrill error: $(jq -r '.name + ": " + .message' <<<"$1")" >&2; exit 1; } || true; }

# The generated files must match build.py, so nobody publishes a hand edit that the next build would drop.
python3 "$dir/build.py" >/dev/null
if ! git -C "$dir" diff --quiet -- .; then
  echo "emails/usage-notifications/ changed after running build.py; commit the regenerated files first:" >&2
  git -C "$dir" diff --stat -- . >&2
  exit 1
fi

publish() { # name
  local payload result
  payload=$(jq -n --arg name "$1" --rawfile code "$dir/usage-notifications.html" --rawfile text "$dir/usage-notifications.txt" \
    '{name:$name, subject:"An Update on your Monthly Usage", from_email:"info@pinax.network", from_name:"Pinax",
      code:$code, text:$text, publish:true, labels:["usage-alerts"]}')
  if call templates/info "$(jq -n --arg n "$1" '{name:$n}')" | jq -e 'type == "object" and .status == "error"' >/dev/null; then
    result=$(call templates/add "$payload")
  else
    result=$(call templates/update "$payload")
  fi
  fail_on_error "$result"
  jq -r '"published \(.slug) at \(.published_at) UTC"' <<<"$result"
}

case "${1:-}" in
"")
  live=$(call templates/info "$(jq -n --arg n "$LIVE" '{name:$n}')")
  fail_on_error "$live"
  # Mandrill re-encodes quotes in attributes when it stores HTML ('Space Mono' becomes &#039;Space Mono&#039;), so compare decoded.
  if jq -r .publish_code <<<"$live" | python3 -c 'import html, sys; a = html.unescape(sys.stdin.read()).strip(); b = html.unescape(open(sys.argv[1], encoding="utf-8").read()).strip(); sys.exit(a != b)' "$dir/usage-notifications.html"; then
    echo "$LIVE matches the repo (published $(jq -r .published_at <<<"$live") UTC)"
  else
    echo "$LIVE differs from the repo (published $(jq -r .published_at <<<"$live") UTC). --stage and --test, then --apply."
  fi
  ;;
--stage)
  publish "$STAGING"
  ;;
--test)
  to="${2:?usage: --test EMAIL}"
  # plan, BilledCredits, IncludedCredits, subject (the one the connector sends for that milestone)
  while IFS='|' read -r plan billed included subject; do
    vars=$(jq -n --arg tp "$plan" --arg bc "$billed" --arg ic "$included" \
      '[{name:"TeamName",content:"Acme Labs"},{name:"TeamPlan",content:$tp},{name:"BilledCredits",content:$bc},{name:"IncludedCredits",content:$ic}]')
    msg=$(jq -n --arg to "$to" --arg subject "$subject" --argjson vars "$vars" \
      '{to:[{email:$to,type:"to"}], from_email:"info@pinax.network", subject:$subject, global_merge_vars:$vars, tags:["usage-alerts-test"]}')
    printf '%-6s %-9s ' "$plan" "\$$billed"
    call messages/send-template "$(jq -n --arg t "$STAGING" --argjson m "$msg" --argjson c "$vars" '{template_name:$t, template_content:$c, message:$m}')" |
      jq -c 'if type == "array" then map(.status) else {error: .name, message} end'
  done <<'CASES'
free|12.51|25.00|You've used 50% of your free Pinax credits
free|22.51|25.00|You've used 90% of your free Pinax credits
free|25.07|25.00|Your Pinax service is paused: free credits used up
pro|180.40|200.00|You've used 90% of your included Pro usage
pro|200.35|200.00|You've used all of your included Pro usage
pro|300.25|200.00|You're past your included Pro usage (150%)
pro|1,250.00|200.00|You're past your included Pro usage (200%)
CASES
  ;;
--apply)
  backup="${TMPDIR:-/tmp}/$LIVE.$(date -u +%Y%m%dT%H%M%SZ).json"
  call templates/info "$(jq -n --arg n "$LIVE" '{name:$n}')" > "$backup"
  fail_on_error "$(cat "$backup")"
  echo "saved the current live version to $backup (restore with --restore $backup)"
  publish "$LIVE"
  ;;
--restore)
  b="${2:?usage: --restore FILE}"
  result=$(call templates/update "$(jq -c --arg n "$LIVE" '{name:$n, subject:.publish_subject, from_email:.publish_from_email,
    from_name:.publish_from_name, code:.publish_code, text:(.publish_text // ""), publish:true, labels:.labels}' "$b")")
  fail_on_error "$result"
  jq -r '"restored \(.slug) at \(.published_at) UTC"' <<<"$result"
  ;;
*)
  sed -n '2,11p' "$0" >&2
  exit 2
  ;;
esac
