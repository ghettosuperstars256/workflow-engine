#!/usr/bin/env python3
"""
Prime Garrison — Zapier Integration Layer v1.0
Catch Zapier webhooks and route to workflow engine.

Two-way integration:
1. RECEIVE: Zapier triggers → our webhook endpoint → run workflows
2. SEND: Our Zapier pushes data TO Zapier (for apps we can't access directly)
"""

import os
import sys
import hmac
import hashlib
import json
import logging
from fastapi import APIRouter, Request, HTTPException, Header
from fastapi.responses import JSONResponse

logger = logging.getLogger("zapier-integration")
router = APIRouter(prefix="/api/zapier", tags=["zapier"])

# Zapier webhook secret (set when creating Zapier catch hooks)
ZAPIER_WEBHOOK_SECRET = os.environ.get("ZAPIER_WEBHOOK_SECRET", "")

# ──→ This function is called by our workflow when it needs to PUSH data to Zapier
def push_to_zapier(zapier_hook_url, payload):
    """Send data TO a Zapier catch hook."""
    import requests
    resp = requests.post(zapier_hook_url, json=payload, timeout=15)
    if resp.status_code in (200, 201):
        logger.info(f"Zapier push OK: {zapier_hook_url}")
        return {"ok": True}
    else:
        logger.error(f"Zapier push failed: {resp.status_code}")
        return {"ok": False, "error": resp.text[:200]}


# ──→ Receive webhooks FROM Zapier
@router.post("/webhook/{hook_id}")
async def receive_zapier_webhook(hook_id: str, request: Request):
    """
    Catch Zapier webhooks. Zapier POSTs to this URL when a trigger fires.
    We route to the workflow engine.
    """
    body = await request.body()
    payload = await request.json()

    logger.info(f"Zapier webhook: {hook_id} → {list(payload.keys())}")

    # Verify signature if secret is configured
    if ZAPIER_WEBHOOK_SECRET:
        sig = request.headers.get("X-Zapier-Signature", "")
        expected = hmac.new(
            ZAPIER_WEBHOOK_SECRET.encode(),
            body,
            hashlib.sha256
        ).hexdigest()
        if sig != expected:
            raise HTTPException(status_code=403, detail="Invalid signature")

    # Route to workflow engine
    try:
        sys.path.insert(0, "/opt/data/projects/workflow-engine")
        import engine
        result = engine.run_workflow_from_webhook(hook_id, payload)
        return JSONResponse({"status": "ok", "result": result})
    except FileNotFoundError:
        return JSONResponse({"status": "ok", "message": f"No workflow for {hook_id}", "data": payload})
    except Exception as e:
        logger.error(f"Workflow error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status=500)


@router.get("/zap-list")
async def list_zapier_hooks():
    """List active Zapier integrations."""
    return {
        "zapier_free_plan": {
            "tasks_per_month": 100,
            "multi_step_zaps": False,
            "filters": False,
            "recommendation": "Use Zapier for: Google Forms → Our webhook, Typeform → Our webhook, Calendly → Our webhook. Keep within 100 tasks/mo."
        },
        "recommended_zaps": [
            {
                "name": "New Google Form → Create Lead",
                "trigger": "Google Forms - New Response",
                "action": "Webhook → POST to /api/zapier/webhook/google-form-lead",
                "cost": "1 task per form submission"
            },
            {
                "name": "Typeform → Create Lead",
                "trigger": "Typeform - New Entry",
                "action": "Webhook → POST to /api/zapier/webhook/typeform-lead",
                "cost": "1 task per form entry"
            },
            {
                "name": "Calendly → Client Onboarding",
                "trigger": "Calendly - New Event",
                "action": "Webhook → POST to /api/zapier/webhook/calendly-booking",
                "cost": "1 task per booking"
            },
            {
                "name": "Stripe → Record Payment",
                "trigger": "Stripe - New Payment",
                "action": "Webhook → POST to /api/zapier/webhook/stripe-payment",
                "cost": "1 task per payment"
            }
        ],
        "zapier_alternatives": {
            "make_com": "1,000 ops/month free, better value than Zapier",
            "n8n_self_hosted": "Unlimited, self-hosted, works with our stack. No fees.",
            "pipedream": "100 compute hours/month free, good for dev workflows",
            "our_workflow_engine": "Already built. FREE. No limits. YAML-defined."
        }
    }
