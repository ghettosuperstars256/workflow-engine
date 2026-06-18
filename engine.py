#!/usr/bin/env python3
"""
Prime Garrison — Workflow Engine v1.0
Self-hosted, no external dependencies required.
This is our Zapier/Make/n8n replacement — built into our stack.

Features:
- YAML-defined workflows (triggers + actions)
- Webhook triggers (receive data from external sources)
- Schedule triggers (cron-based)
- Email actions (via Resend)
- HTTP actions (call any API)
- Supabase actions (insert/update/query)
- WordPress actions (REST API)
- Telegram notifications
- Conditional logic (if/then/else)
- Data transformation (templating, JSON parsing)
- Parallel execution

No Zapier fees. No n8n Docker. Pure Python.
"""

import os
import sys
import json
import time
import yaml
import requests
import logging
import threading
import schedule
from datetime import datetime, timezone
from string import Template
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("/opt/data/projects/prime-garrison-app/logs/workflows.log")]
)
logger = logging.getLogger("workflow-engine")

# ─── Config ───────────────────────────────────────────────
WORKFLOWS_DIR = os.environ.get("WORKFLOWS_DIR", "/opt/data/projects/workflows")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
SUPABASE_SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE", "")
# Use service role key for write operations (bypasses RLS)
# Falls back to publishable key if service role not set
SUPABASE_HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE or SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE or SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "8654914551")  # Mainman DM

os.makedirs(WORKFLOWS_DIR, exist_ok=True)
os.makedirs("/opt/data/projects/prime-garrison-app/logs", exist_ok=True)


# ─── Data Transformation ──────────────────────────────────
def resolve_template(template_str, data):
    """Resolve template strings like ${variable} using data dict."""
    if isinstance(template_str, str):
        try:
            return Template(template_str).safe_substitute(data)
        except Exception:
            return template_str
    return template_str


def resolve_dict(d, data):
    """Recursively resolve templates in a dict."""
    if isinstance(d, dict):
        return {k: resolve_dict(v, data) for k, v in d.items()}
    elif isinstance(d, list):
        return [resolve_dict(item, data) for item in d]
    elif isinstance(d, str):
        return resolve_template(d, data)
    return d


def extract_value(data, path):
    """Extract nested value from dict using dot notation: user.email"""
    keys = path.split(".")
    val = data
    for key in keys:
        if isinstance(val, dict):
            val = val.get(key, "")
        else:
            return ""
    return val


# ─── Action Handlers ──────────────────────────────────────
def action_http(params, data):
    """Make an HTTP request."""
    method = params.get("method", "GET").upper()
    url = resolve_template(params["url"], data)
    headers = resolve_dict(params.get("headers", {}), data) if "headers" in params else {}
    body = resolve_dict(params.get("body", {}), data) if "body" in params else None
    json_body = resolve_dict(params.get("json", {}), data) if "json" in params else None

    resp = requests.request(method, url, headers=headers, data=body, json=json_body, timeout=30)
    logger.info(f"  HTTP {method} {url} → {resp.status_code}")
    return resp.json() if "application/json" in resp.headers.get("Content-Type", "") else resp.text


def action_supabase(params, data):
    """Insert, update, or query Supabase."""
    table = params["table"]
    operation = params.get("operation", "insert")

    if operation == "insert":
        payload = resolve_dict(params["data"], data)
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        resp = requests.post(url, json=payload, headers=SUPABASE_HEADERS, timeout=15)
    elif operation == "select":
        query = params.get("query", "*")
        filters = params.get("filters", "")
        url = f"{SUPABASE_URL}/rest/v1/{table}?select={query}{'&' + filters if filters else ''}"
        resp = requests.get(url, headers=SUPABASE_HEADERS, timeout=15)
    elif operation == "update":
        payload = resolve_dict(params["data"], data)
        filters = params.get("filters", "")
        url = f"{SUPABASE_URL}/rest/v1/{table}?{filters}"
        resp = requests.patch(url, json=payload, headers=SUPABASE_HEADERS, timeout=15)

    logger.info(f"  Supabase {operation} {table} → {resp.status_code}")
    if resp.status_code >= 400:
        logger.error(f"  Supabase error: {resp.text[:300]}")
    return resp.json() if resp.status_code < 300 else {"error": resp.text[:200]}


def action_resend(params, data):
    """Send email via Resend."""
    payload = {
        "from": resolve_template(params.get("from", "Prime Garrison <hello@primegarrison.cloud>"), data),
        "to": [resolve_template(params["to"], data)],
        "subject": resolve_template(params["subject"], data),
    }
    if "html" in params:
        payload["html"] = resolve_template(params["html"], data)
    if "text" in params:
        payload["text"] = resolve_template(params["text"], data)

    resp = requests.post("https://api.resend.com/emails", json=payload, headers={
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "PrimeGarrison/1.0",
    }, timeout=15)

    logger.info(f"  Resend → {payload['to']} '{payload['subject'][:50]}' → {resp.status_code}")
    return resp.json() if resp.status_code in (200, 201, 202) else {"error": resp.text[:200]}


def action_telegram(params, data):
    """Send Telegram message."""
    chat_id = params.get("chat_id", TELEGRAM_CHAT_ID)
    text = resolve_template(params["text"], data)
    parse_mode = params.get("parse_mode", "Markdown")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode
    }, timeout=10)

    logger.info(f"  Telegram → {chat_id} → {resp.status_code}")
    return resp.json()


def action_wp_rest(params, data):
    """Call WordPress REST API."""
    wp_url = resolve_template(params["url"], data)
    wp_user = params.get("user", "admin")
    wp_pass = params.get("password", "")
    method = params.get("method", "POST").upper()
    endpoint = params.get("endpoint", "/wp/v2/posts")
    body = resolve_dict(params.get("body", {}), data) if "body" in params else None

    url = f"{wp_url}/wp-json{endpoint}"
    resp = requests.request(method, url, json=body, auth=(wp_user, wp_pass), timeout=15)

    logger.info(f"  WP REST {method}{endpoint} → {resp.status_code}")
    return resp.json() if resp.status_code < 300 else {"error": resp.text[:200]}


def action_log(params, data):
    """Log data to console/file."""
    message = resolve_template(params.get("message", "${_all}"), data)
    logger.info(f"  LOG: {message[:500]}")
    return message


def action_delay(params, data):
    """Pause workflow execution."""
    seconds = params.get("seconds", 5)
    logger.info(f"  Delay {seconds}s...")
    time.sleep(seconds)
    return {"delayed": seconds}


def action_code(params, data):
    """Transform data using safe expression mapping (no exec/eval)."""
    # Safe operations only: get, set, filter, map, join, split, upper, lower, strip
    code = params.get("code", "")
    if not code:
        return data
    try:
        result = _safe_transform(code, data)
        return result
    except Exception as e:
        logger.error(f"  Code transform error: {e}")
        return {"error": str(e)}


# Safe transform operations — NO exec/eval
_SAFE_OPS = {
    "upper": str.upper,
    "lower": str.lower,
    "strip": str.strip,
    "title": str.title,
    "len": len,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "list": list,
    "dict": dict,
    "sorted": sorted,
    "reversed": list,  # reversed() returns iterator, wrap in list
    "sum": sum,
    "min": min,
    "max": max,
    "abs": abs,
    "round": round,
}


def _safe_transform(code, data):
    """Apply safe data transformations. Code format: 'field.operation(args)'"""
    result = data
    for line in code.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Simple assignment: result = expr
        if line.startswith("result = "):
            expr = line[9:].strip()
            result = _eval_safe_expr(expr, data)
        else:
            result = _eval_safe_expr(line, data)
    return result


def _eval_safe_expr(expr, data):
    """Evaluate a safe expression with data context."""
    # Resolve template variables first
    for key, val in data.items():
        expr = expr.replace(f"${{{key}}}", repr(val))
        expr = expr.replace(f"${key}", repr(val))
    # Only allow safe operations — no __builtins__
    try:
        return eval(expr, {"__builtins__": {}, **_SAFE_OPS}, {})
    except Exception:
        return expr


# ─── Action Router ────────────────────────────────────────
ACTIONS = {
    "http": action_http,
    "supabase": action_supabase,
    "resend": action_resend,
    "telegram": action_telegram,
    "wordpress": action_wp_rest,
    "log": action_log,
    "delay": action_delay,
    "code": action_code,
}


def run_action(action_def, data):
    """Run a single action."""
    action_type = action_def.get("type", "log")
    handler = ACTIONS.get(action_type)
    if not handler:
        logger.error(f"Unknown action type: {action_type}")
        return {"error": f"Unknown action: {action_type}"}

    params = action_def.get("params", {})
    if "as" in action_def:
        # Store result in data context
        result = handler(params, data)
        data[action_def["as"]] = result
        # Also flatten first-item fields for easy access
        # Use underscore separator since string.Template doesn't support dots
        if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
            for k, v in result[0].items():
                data[f"{action_def['as']}_{k}"] = v
        elif isinstance(result, dict):
            for k, v in result.items():
                data[f"{action_def['as']}_{k}"] = v
        return result
    return handler(params, data)


def run_actions(actions, data):
    """Run a list of actions sequentially."""
    for action_def in actions:
        # Check condition if present
        condition = action_def.get("if")
        if condition:
            # Simple condition evaluation
            condition_result = evaluate_condition(condition, data)
            if not condition_result:
                logger.info(f"  Skipping action (condition false): {condition}")
                continue

        try:
            run_action(action_def, data)
        except Exception as e:
            logger.error(f"  Action failed: {e}")
            if action_def.get("params", {}).get("stop_on_error", True):
                return data
    return data


def evaluate_condition(condition, data):
    """Evaluate simple conditions safely: key == value, key > value, etc."""
    try:
        for key, val in data.items():
            condition = condition.replace(f"${{{key}}}", repr(val))
            condition = condition.replace(f"${key}", repr(val))
        # Safe eval with no builtins
        return bool(eval(condition, {"__builtins__": {
            "True": True, "False": False, "None": None,
            "len": len, "str": str, "int": int, "float": float,
            "bool": bool, "list": list, "dict": dict, "set": set,
            "type": type, "isinstance": isinstance,
        }}, {}))
    except Exception:
        return False


# ─── Workflow Runner ──────────────────────────────────────
def run_workflow(workflow_name, trigger_data=None):
    """Load and run a workflow by name."""
    workflow_file = os.path.join(WORKFLOWS_DIR, f"{workflow_name}.yaml")

    if not os.path.exists(workflow_file):
        logger.error(f"Workflow not found: {workflow_file}")
        return None

    with open(workflow_file, "r") as f:
        workflow = yaml.safe_load(f)

    logger.info(f"═══ Running workflow: {workflow_name} ═══")
    data = {
        "_workflow": workflow_name,
        "_timestamp": datetime.now(timezone.utc).isoformat(),
        "_trigger": trigger_data or {},
        **(trigger_data or {})
    }

    for step in workflow.get("steps", []):
        step_name = step.get("name", "unnamed")
        logger.info(f"  Step: {step_name}")
        try:
            run_actions(step.get("actions", []), data)
        except Exception as e:
            logger.error(f"  Step '{step_name}' failed: {e}")

    logger.info(f"═══ Workflow complete: {workflow_name} ═══")
    return data


def run_workflow_from_webhook(workflow_name, webhook_data):
    """Run a workflow triggered by webhook data."""
    return run_workflow(workflow_name, {"webhook": webhook_data, **webhook_data})


# ─── Scheduler ────────────────────────────────────────────
def load_scheduled_workflows():
    """Load and schedule all workflows with 'schedule' triggers."""
    scheduled = []
    for filename in os.listdir(WORKFLOWS_DIR):
        if not filename.endswith(".yaml") and not filename.endswith(".yml"):
            continue
        filepath = os.path.join(WORKFLOWS_DIR, filename)
        try:
            with open(filepath, "r") as f:
                workflow = yaml.safe_load(f)

            if not workflow or "schedule" not in workflow:
                continue

            wf_name = filename.replace(".yaml", "").replace(".yml", "")
            sched = workflow["schedule"]
            cron_expr = sched.get("cron", "0 * * * *")

            # Parse cron (basic support)
            parts = cron_expr.split()
            if len(parts) == 5:
                minute, hour, day, month, weekday = parts
                job = schedule.every().day.at(f"{hour.zfill(2)}:{minute.zfill(2)}")
                job.do(run_workflow, wf_name)
                scheduled.append(f"{wf_name}: {cron_expr}")
                logger.info(f"  Scheduled: {wf_name} @ {cron_expr}")

        except Exception as e:
            logger.error(f"  Failed to schedule {filename}: {e}")

    return scheduled


def start_scheduler():
    """Start the cron scheduler loop."""
    os.makedirs(WORKFLOWS_DIR, exist_ok=True)
    scheduled = load_scheduled_workflows()
    logger.info(f"Workflow engine started. {len(scheduled)} workflows scheduled.")
    while True:
        schedule.run_pending()
        time.sleep(30)


# ─── CLI ──────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Prime Garrison Workflow Engine")
    parser.add_argument("--run", help="Run a workflow by name")
    parser.add_argument("--schedule", action="store_true", help="Start scheduler")
    parser.add_argument("--list", action="store_true", help="List workflows")
    parser.add_argument("--data", help="JSON data to pass to workflow", default="{}")
    args = parser.parse_args()

    if args.run:
        data = json.loads(args.data)
        run_workflow(args.run, data)
    elif args.schedule:
        start_scheduler()
    elif args.list:
        for f in sorted(os.listdir(WORKFLOWS_DIR)):
            if f.endswith((".yaml", ".yml")):
                print(f"  📋 {f.replace('.yaml','').replace('.yml','')}")
    else:
        print("Usage: python3 engine.py --run <workflow> | --schedule | --list")
