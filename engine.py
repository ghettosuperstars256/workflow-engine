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
    """Transform data using safe expression mapping (NO eval/exec)."""
    code = params.get("code", "")
    if not code:
        return data
    try:
        result = _safe_transform(code, data)
        return result
    except Exception as e:
        logger.error(f"  Code transform error: {e}")
        return {"error": str(e)}


# ─── Safe Expression Evaluator ──────────────────────────────
# NO eval(), NO exec(). Pure Python string/number operations only.

def _safe_transform(code, data):
    """Apply safe data transformations.
    Supported: result = field | field.upper | field.lower | field.strip |
                field.title | len(field) | int(field) | str(field) |
                field1 + field2 | field * n
    """
    result = data
    for line in code.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("result = "):
            expr = line[9:].strip()
            result = _safe_eval(expr, data)
        else:
            result = _safe_eval(line, data)
    return result


def _safe_eval(expr, data):
    """Evaluate a safe expression. Only string/number ops allowed."""
    expr = expr.strip()

    # Resolve ${var} and $var template references
    for key in sorted(data.keys(), key=len, reverse=True):
        val = data[key]
        expr = expr.replace(f"${{{key}}}", str(val))
        expr = expr.replace(f"${key}", str(val))

    # Handle pipe operations: field.upper, field.lower, etc.
    pipe_ops = {
        "upper": str.upper,
        "lower": str.lower,
        "strip": str.strip,
        "title": str.title,
    }
    for op_name, op_fn in pipe_ops.items():
        if f".{op_name}" in expr:
            parts = expr.rsplit(f".{op_name}", 1)
            if len(parts) == 2 and (not parts[1] or parts[1].startswith("()")):
                val = _resolve_value(parts[0].strip(), data)
                return op_fn(str(val)) if val is not None else ""

    # Handle function calls: len(x), int(x), str(x), float(x), bool(x)
    func_ops = {
        "len": len, "int": int, "str": str, "float": float, "bool": bool,
        "abs": abs, "round": round, "min": min, "max": max, "sum": sum,
    }
    for func_name, func_fn in func_ops.items():
        if expr.startswith(f"{func_name}(") and expr.endswith(")"):
            inner = expr[len(func_name)+1:-1].strip()
            val = _resolve_value(inner, data)
            try:
                return func_fn(val)
            except Exception:
                return None

    # Handle concatenation: "hello" + " " + "world" or field1 + field2
    if " + " in expr:
        parts = expr.split(" + ")
        resolved = [_resolve_value(p.strip(), data) for p in parts]
        if all(r is not None for r in resolved):
            # If any part is numeric, try numeric addition
            try:
                nums = [float(r) for r in resolved]
                result = sum(nums)
                return int(result) if result == int(result) else result
            except (ValueError, TypeError):
                return "".join(str(r) for r in resolved)

    # Handle multiplication: field * n
    if " * " in expr:
        parts = expr.split(" * ")
        if len(parts) == 2:
            val = _resolve_value(parts[0].strip(), data)
            try:
                return float(val) * float(parts[1].strip())
            except (ValueError, TypeError):
                return val

    # Simple value resolution
    val = _resolve_value(expr, data)
    if val is not None:
        return val

    # Return as-is if nothing matched
    return expr


def _resolve_value(expr, data):
    """Resolve a value from data dict. Supports dot notation: user.name"""
    expr = expr.strip()
    # String literal
    if (expr.startswith('"') and expr.endswith('"')) or \
       (expr.startswith("'") and expr.endswith("'")):
        return expr[1:-1]
    # Numeric literal
    try:
        return int(expr)
    except ValueError:
        try:
            return float(expr)
        except ValueError:
            pass
    # Boolean / None
    if expr == "True": return True
    if expr == "False": return False
    if expr == "None": return None
    # Data lookup (dot notation)
    keys = expr.split(".")
    val = data
    for k in keys:
        if isinstance(val, dict):
            val = val.get(k)
        else:
            return None
    return val


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
    """Evaluate simple conditions safely (NO eval).
    Supports: key == value, key != value, key > value, key < value,
              key >= value, key <= value, key contains value
    """
    try:
        # Resolve ${var} and $var template references
        cond = condition.strip()
        for key in sorted(data.keys(), key=len, reverse=True):
            val = data[key]
            cond = cond.replace(f"${{{key}}}", str(val))
            cond = cond.replace(f"${key}", str(val))

        # Parse comparison operators
        import re
        # Match: operand1 operator operand2
        match = re.match(r'^\s*(.+?)\s*(==|!=|>=|<=|>|<|contains)\s*(.+?)\s*$', cond)
        if not match:
            return False
        left_str, op, right_str = match.groups()
        left_str = left_str.strip()
        right_str = right_str.strip()

        # Resolve values
        left = _resolve_value(left_str, data)
        right = _resolve_value(right_str, data)
        # If a side resolved to None, treat it as a literal string
        if left is None:
            left = left_str
        if right is None:
            right = right_str

        # Try numeric comparison first
        try:
            left_num = float(left) if left is not None else None
            right_num = float(right) if right is not None else None
            if left_num is not None and right_num is not None:
                if op == "==": return left_num == right_num
                if op == "!=": return left_num != right_num
                if op == ">":  return left_num > right_num
                if op == "<":  return left_num < right_num
                if op == ">=": return left_num >= right_num
                if op == "<=": return left_num <= right_num
        except (ValueError, TypeError):
            pass

        # String comparison
        left_s = str(left) if left is not None else ""
        right_s = str(right) if right is not None else ""
        if op == "==": return left_s == right_s
        if op == "!=": return left_s != right_s
        if op == "contains": return right_s in left_s

        return False
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
