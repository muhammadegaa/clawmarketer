---
name: clawmarketer-meta
description: Analyze Meta Ads campaign performance — fetch live data, clean, generate AI report, send charts + CSV to Telegram, push live progress to ClawMarketer dashboard.
user-invocable: true
---

## When to invoke
- User says "analyze ads", "analyze my ads", "run ads report", "meta ads report"
- User says "analyze ads last 7 days / last month / last quarter"
- User says "how are my ads doing"

## Instructions

Run the Meta Ads analysis pipeline:

```bash
cd ~/clawmarketer
python openclaw_agent.py
```

The script will:
1. Fetch live campaign data from Meta Ads API
2. Clean and analyze the data
3. Generate spend/CTR/ROAS charts
4. Write an AI report via Groq
5. Send charts + CSV + report to Telegram
6. Push live progress to the ClawMarketer dashboard

## Date range
Parse the user's message for a time range and pass it:

```bash
python openclaw_agent.py  # defaults to last 30 days
```

If the user specifies a range, edit the `run()` call at the bottom of openclaw_agent.py or pass via env:
- "last 7 days" → `date_preset=last_7d`
- "last month" → `date_preset=last_month`
- "last quarter" → `date_preset=last_quarter`

## Config
All credentials are in `~/clawmarketer/clawmarketer.env`.
If the script fails with a missing token error, tell the user to fill in that file.

## Response format
After the script completes, report back:
```
✅ Meta Ads report sent to Telegram.
Check your ClawMarketer dashboard for the live results: https://clawmarketer.vercel.app
```
