---
name: clawmarketer-data
description: Scan local CSV/Excel files, clean and deduplicate them, send cleaned files + AI summary to Telegram, push progress to ClawMarketer dashboard.
user-invocable: true
---

## When to invoke
- User says "clean my data", "clean data", "clean my files"
- User says "analyze my files", "process my data"
- User says "clean data in [folder path]"

## Instructions

Run the data cleansing pipeline:

```bash
cd /Users/muhammadegaa/Documents/Sides/clawmarketer
python openclaw_data_agent.py
```

The script will:
1. Scan DATA_DIR (set in clawmarketer.env) for CSV/Excel files
2. Remove duplicates, fix nulls, normalize columns
3. Save clean_ versions of each file in the same folder
4. Send a summary + cleaned files to Telegram
5. Push live progress to the ClawMarketer dashboard

## Config
All credentials and DATA_DIR are in `/Users/muhammadegaa/Documents/Sides/clawmarketer/clawmarketer.env`.

If the user specifies a different folder, temporarily override:
```bash
DATA_DIR=~/path/to/folder python openclaw_data_agent.py
```

## Response format
After the script completes, report back:
```
✅ Data cleansing complete. Cleaned files sent to Telegram.
Check your ClawMarketer dashboard: https://clawmarketer.vercel.app
```
