import os
import json
from groq import Groq

MODEL = "llama-3.3-70b-versatile"


def _build_prompt(overall: dict, top_bottom: dict, anomalies: list) -> str:
    return f"""You are a senior digital marketing analyst. Based on the Meta Ads performance data below, write a clear, actionable report for a business owner.

## Overall Metrics
{json.dumps(overall, indent=2)}

## Top & Bottom Performers
{json.dumps(top_bottom, indent=2)}

## Anomalies Detected
{json.dumps(anomalies, indent=2)}

Write a report with these sections:
1. **Executive Summary** (3-4 sentences, plain English)
2. **What's Working** (specific campaigns/metrics with numbers)
3. **What Needs Attention** (specific problems with urgency)
4. **Recommended Actions** (3-5 concrete next steps, prioritized)

Be direct. Use the actual numbers. No fluff."""


def generate(analysis: dict, api_key: str) -> str:
    client = Groq(api_key=api_key)

    prompt = _build_prompt(
        overall=analysis.get("overall", {}),
        top_bottom=analysis.get("top_bottom", {}),
        anomalies=analysis.get("anomalies", []),
    )

    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.4,
    )

    return response.choices[0].message.content


def save_report(report_text: str, output_path: str):
    with open(output_path, "w") as f:
        f.write(report_text)
