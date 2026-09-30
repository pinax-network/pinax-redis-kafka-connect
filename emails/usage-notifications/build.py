#!/usr/bin/env python3
"""Generates usage-notifications.html/.txt (Mailchimp merge language, as the connector sends no merge_language).

The connector sends TeamName, TeamPlan, BilledCredits, IncludedCredits (USD, no currency symbol, comma thousands
separator) and, once pinax-redis-kafka-connect#8 ships, UsagePercent. Mandrill conditionals can compare a merge var
with a literal but not with another merge var, and a value with a comma (>= 1,000) doesn't compare as a number. So:

- Free ($25 included) and Pro ($200 included) derive the milestone from BilledCredits with fixed bounds. That is exact:
  the connector notifies the highest milestone crossed, which is the highest milestone <= BilledCredits.
- IF the included amount isn't the expected one (plan allowance changed, or an odd subscription), or the value has a
  comma, the email falls back to UsagePercent when present, else to a generic heading with the figures.

If the Free or Pro allowance ever changes, update FREE/PRO below and republish.
"""
import html
import re
from pathlib import Path

APOSTROPHE = re.compile(r"(?<=[A-Za-z])'(?=[A-Za-z])")

FREE = 25
PRO = 200
MONO = "'Space Mono',ui-monospace,SFMono-Regular,Menlo,Consolas,monospace"
SANS = "'Space Grotesk',-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif"
ACCENT, WARN, TRACK = "#90FFEA", "#EE758C", "#262626"
APP = "https://app.pinax.network"


def fmt(n):
    return f"{n:g}"


def bar(pct):
    """Progress bar, capped at 100%. Past 100% it turns the brand pink."""
    width = min(pct, 100)
    color = WARN if pct >= 100 else ACCENT
    fill = f'<td width="{width}%" height="8" bgcolor="{color}" style="height:8px;line-height:8px;font-size:0;background:{color};border-radius:4px;">&nbsp;</td>'
    rest = "" if width >= 100 else f'<td width="{100 - width}%" height="8" bgcolor="{TRACK}" style="height:8px;line-height:8px;font-size:0;background:{TRACK};">&nbsp;</td>'
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="{TRACK}" '
            f'style="background:{TRACK};border-radius:4px;margin:0 0 20px 0;"><tr>{fill}{rest}</tr></table>')


def h1(text):
    return (f'<h1 class="h1" style="margin:0 0 20px 0;font-family:{MONO};font-size:32px;line-height:40px;font-weight:700;'
            f'color:#FFFFFE;">{text}</h1>')


def strong(text):
    return f'<strong style="color:#FFFFFE;">{text}</strong>'


# (plan, lower bound in USD, milestone %, heading, next-step html, next-step text)
FREE_PAUSED_NEXT = (f"{strong('Service is paused')} until your credits reset on the 1st. Upgrade to Pro to resume now.",
                    "Service is paused until your credits reset on the 1st. Upgrade to Pro to resume now.")
FREE_BEFORE_NEXT = (f"When you reach ${FREE}, {strong('the Free plan pauses service')} until your credits reset on the 1st. Upgrade to Pro to keep your data flowing.",
                    f"When you reach ${FREE}, the Free plan pauses service until your credits reset on the 1st. Upgrade to Pro to keep your data flowing.")
PRO_BEFORE_NEXT = (f"{strong('Your service continues past 100%')} unless you've set a usage limit. Usage beyond your ${PRO} included is billed at the end of the month.",
                   f"Your service continues past 100% unless you've set a usage limit. Usage beyond your ${PRO} included is billed at the end of the month.")
PRO_AFTER_NEXT = (f"{strong('Your service continues')} unless you've set a usage limit. Usage beyond your ${PRO} included is billed at the end of the month.",
                  f"Your service continues unless you've set a usage limit. Usage beyond your ${PRO} included is billed at the end of the month.")

STATES = {
    "free": [
        (FREE * 1.00, 100, "Your free credits are used up", *FREE_PAUSED_NEXT),
        (FREE * 0.90, 90, "You've used 90% of your free credits", *FREE_BEFORE_NEXT),
        (FREE * 0.75, 75, "You've used 75% of your free credits", *FREE_BEFORE_NEXT),
        (FREE * 0.50, 50, "You've used half of your free credits", *FREE_BEFORE_NEXT),
    ],
    "pro": [
        (PRO * 2.00, 200, "You're at 200% of your included usage", *PRO_AFTER_NEXT),
        (PRO * 1.50, 150, "You're at 150% of your included usage", *PRO_AFTER_NEXT),
        (PRO * 1.00, 100, "You've used all of your included usage", *PRO_AFTER_NEXT),
        (PRO * 0.90, 90, "You've used 90% of your included usage", *PRO_BEFORE_NEXT),
        (PRO * 0.75, 75, "You've used 75% of your included usage", *PRO_BEFORE_NEXT),
        (PRO * 0.50, 50, "You've used half of your included usage", *PRO_BEFORE_NEXT),
    ],
}
INCLUDED = {"free": FREE, "pro": PRO}

# Fallback (allowance not the expected one, amount >= $1,000, or another plan). Uses UsagePercent when the connector sends it.
FALLBACK_H1 = "*|IF:USAGEPERCENT|*You've used *|UsagePercent|*% of this month's credits*|ELSE:|*$*|BilledCredits|* of $*|IncludedCredits|* used this month*|END:IF|*"
FALLBACK_NEXT = {
    "free": (f"When your included credits run out, {strong('the Free plan pauses service')} until they reset on the 1st. Upgrade to Pro to keep your data flowing.",
             "When your included credits run out, the Free plan pauses service until they reset on the 1st. Upgrade to Pro to keep your data flowing."),
    "pro": (f"{strong('Your service continues')} unless you've set a usage limit. Usage beyond your $*|IncludedCredits|* included is billed at the end of the month.",
            "Your service continues unless you've set a usage limit. Usage beyond your $*|IncludedCredits|* included is billed at the end of the month."),
    "other": (f"{strong('Your service continues')} under the terms of your plan. Reply to this email if you have any questions.",
              "Your service continues under the terms of your plan. Reply to this email if you have any questions."),
}


def next_p(inner):
    return f'<p style="margin:0;font-family:{SANS};font-size:16px;line-height:26px;color:#BFBFBE;">{inner}</p>'


def plan_chain(plan, block):
    """block(state) -> content for a state; block(None) -> fallback content. Returns the IF chain for one plan."""
    out = f"*|IF:INCLUDEDCREDITS={INCLUDED[plan]}|*"
    for i, st in enumerate(STATES[plan]):
        out += ("*|IF:" if i == 0 else "*|ELSEIF:") + f"BILLEDCREDITS>={fmt(st[0])}|*" + block(st)
    out += "*|ELSE:|*" + block(None) + "*|END:IF|*"
    out += "*|ELSE:|*" + block(None) + "*|END:IF|*"
    return out


def by_plan(free, pro, other):
    return f"*|IF:TEAMPLAN=free|*{free}*|ELSEIF:TEAMPLAN=pro|*{pro}*|ELSE:|*{other}*|END:IF|*"


def hero_html():
    def blk(plan):
        return lambda st: (h1(st[2]) + bar(st[1])) if st else h1(FALLBACK_H1)
    return by_plan(plan_chain("free", blk("free")), plan_chain("pro", blk("pro")), h1(FALLBACK_H1))


def next_html():
    return by_plan(plan_chain("free", lambda st: next_p(st[3] if st else FALLBACK_NEXT["free"][0])),
                   plan_chain("pro", lambda st: next_p(st[3] if st else FALLBACK_NEXT["pro"][0])),
                   next_p(FALLBACK_NEXT["other"][0]))


def preview():
    def free(st):
        if not st:
            return "Team usage this month: $*|BilledCredits|* of $*|IncludedCredits|* included."
        return "Service is paused until the 1st. Upgrade to Pro to resume." if st[1] >= 100 else f"$*|BilledCredits|* of your ${FREE} in free credits used this month."
    def pro(st):
        if not st:
            return "Team usage this month: $*|BilledCredits|* of $*|IncludedCredits|* included."
        return f"$*|BilledCredits|* used this month (${PRO} included)." if st[1] >= 100 else f"$*|BilledCredits|* of your ${PRO} included usage this month."
    return by_plan(plan_chain("free", free), plan_chain("pro", pro), "Team usage this month: $*|BilledCredits|* of $*|IncludedCredits|* included.")


def text_version():
    def h(st):
        return st[2] if st else FALLBACK_H1
    head = by_plan(plan_chain("free", h), plan_chain("pro", h), FALLBACK_H1)
    nxt = by_plan(plan_chain("free", lambda st: st[4] if st else FALLBACK_NEXT["free"][1]),
                  plan_chain("pro", lambda st: st[4] if st else FALLBACK_NEXT["pro"][1]),
                  FALLBACK_NEXT["other"][1])
    plan_label = "*|IF:TEAMPLAN=free|*Free*|ELSEIF:TEAMPLAN=pro|*Pro*|ELSEIF:TEAMPLAN=enterprise|*Enterprise*|ELSE:|**|TeamPlan|**|END:IF|*"
    return f"""{head}

Team: *|TeamName|*
Plan: {plan_label}
Used: $*|BilledCredits|* / $*|IncludedCredits|* included

{nxt}

*|IF:TEAMPLAN=free|*Upgrade to Pro: {APP}/usage?upgradePlan=true*|ELSE:|*View usage: {APP}/usage*|END:IF|*
*|IF:TEAMPLAN=pro|*Review your usage limit: {APP}/settings/general
*|END:IF|*
Usage resets on the 1st of each month. Questions? Reply to this email.

--
Pinax · 330 Rue Avro · Pointe-Claire, QC H9R 5W9 · Canada
You're receiving this because this address gets usage notices for your team on Pinax.
"""


def main():
    here = Path(__file__).parent
    tpl = (here / "usage-notifications.src.html").read_text()
    out = (tpl.replace("{{PREVIEW}}", preview())
              .replace("{{HERO}}", hero_html())
              .replace("{{NEXT}}", next_html()))
    # Typographic apostrophes: Space Mono draws a straight ' as a heavy vertical tick. Only letter'letter is replaced,
    # so quoted font names in style attributes ('Space Mono') are untouched.
    out = APOSTROPHE.sub("\u2019", out)
    (here / "usage-notifications.html").write_text(out)
    (here / "usage-notifications.txt").write_text(APOSTROPHE.sub("\u2019", text_version()))
    print("built", len(out), "bytes")


if __name__ == "__main__":
    main()
