#!/usr/bin/env python3
"""
Prime Garrison — Apify Integration Layer v1.0
Runs Apify actors via API for SEO and lead generation.

Apify Free Tier: $5/month credit (~5,000 actor runs)
Strategy: Use actors for expensive tasks, do simple stuff locally.

Actors we use:
1. google-search — SERP data (backup when Serper is rate-limited)
2. website-content-crawler — Full site content extraction
3. google-maps — Local business discovery (Uganda businesses)
4. email-extractor — Find emails from websites
5. ownez/scraper — SEO metadata extraction
"""

import os
import json
import time
import logging
import requests

logger = logging.getLogger("apify-integration")

APIFY_API_KEY = os.environ.get("APIFY_API_KEY", "")
APIFY_BASE = "https://api.apify.com/v2"


# ─── Run Actors ────────────────────────────────────────────
def run_actor(actor_id, input_data, wait=True, timeout=120):
    """Run an Apify actor and return results."""
    if not APIFY_API_KEY:
        logger.warning("APIFY_API_KEY not set")
        return None

    # Start actor run
    resp = requests.post(
        f"{APFY_BASE}/acts/{actor_id}/runs",
        params={"token": APIFY_API_KEY, "waitForFinish": 60 if wait else None},
        json=input_data,
        timeout=30
    )
    if resp.status_code not in (200, 201):
        logger.error(f"Actor start failed: {resp.status_code} {resp.text[:200]}")
        return None

    run_data = resp.json()
    run_id = run_data["data"]["id"]

    if not wait:
        return {"run_id": run_id, "status": "started"}

    # Wait for completion
    start = time.time()
    while time.time() - start < timeout:
        time.sleep(5)
        status_resp = requests.get(
            f"{APIFY_BASE}/acts/{actor_id}/runs/{run_id}",
            params={"token": APIFY_API_KEY},
            timeout=10
        )
        status = status_resp.json()["data"]["status"]
        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            logger.error(f"Actor {actor_id} {status}")
            return None

    # Fetch results
    dataset_id = status_resp.json()["data"]["defaultDatasetId"]
    result_resp = requests.get(
        f"{APIFY_BASE}/datasets/{dataset_id}/items",
        params={"token": APIFY_API_KEY, "format": "json"},
        timeout=30
    )
    results = result_resp.json()
    logger.info(f"Actor {actor_id} complete: {len(results)} items")
    return results


# ─── SEO-Specific Actors ──────────────────────────────────
def serp_analysis(query, num_results=10):
    """Google SERP analysis via Apify (Apify Store actor: apify/google-search-scraper)."""
    # Actor: apify/google-search-scraper
    results = run_actor(
        "apify/google-search-scraper",
        {
            "queries": query,
            "resultsPerPage": num_results,
            "maxPagesPerQuery": 1,
            "countryCode": "ug",  # Uganda
        }
    )
    return results or []


def crawl_website_content(url, max_pages=50):
    """Crawl full website content (Actor: apify/website-content-crawler)."""
    results = run_actor(
        "apify/website-content-crawler",
        {
            "startUrls": [{"url": url}],
            "maxCrawlPages": max_pages,
            "maxCrawlDepth": 3,
            "saveScreenshots": "fullPage",
            "crawlerType": "cheerio",
        }
    )
    return results or []


def discover_local_businesses(search_term, location="Kampala, Uganda"):
    """Find local businesses via Google Maps (Actor: apify/google-maps-scraper)."""
    results = run_actor(
        "apify/google-maps-scraper",
        {
            "searchTerms": [search_term],
            "location": location,
            "maxCrawledPlaces": 50,
            "language": "en",
            "countryCode": "ug",
        },
    )
    return results or []


def extract_emails_from_website(url):
    """Find emails on a website (Actor: lukaskrivka/email-extractor)."""
    results = run_actor(
        "lukaskrivka/email-extractor",
        {"startUrl": url}
    )
    return results or []


def seo_metadata_extractor(url):
    """Extract SEO metadata from a website (Actor: dtrungtin/actor-seo-checker)."""
    results = run_actor(
        "dtrungtin/actor-seo-checker",
        {"startUrl": url, "proxy": {"useApifyProxy": True}},
        timeout=180
    )
    return results or []


# ─── Batch Operations ──────────────────────────────────────
def batch_discover_and_audit(search_terms, location="Kampala, Uganda"):
    """Full pipeline: discover businesses → extract websites → run SEO audits."""
    all_businesses = []

    for term in search_terms:
        logger.info(f"Discovering: {term} in {location}")
        businesses = discover_local_businesses(term, location)

        for biz in businesses[:10]:  # Limit to 10 per search
            website = biz.get("website", "")
            if website:
                # Extract emails
                emails = extract_emails_from_website(website)
                biz["emails"] = [e.get("email") for e in (emails or []) if e.get("email")]

                # Run SEO audit
                seo = seo_metadata_extractor(website)
                if seo:
                    biz["seo_data"] = seo[0]

            all_businesses.append(biz)
            time.sleep(2)  # Rate limit

    return all_businesses


# ─── FastAPI Router ────────────────────────────────────────
from fastapi import APIRouter

apify_router = APIRouter(prefix="/api/apify", tags=["apify"])


@apify_router.post("/serp")
async def apify_serp(request: dict):
    """Run SERP analysis."""
    query = request.get("query", "")
    results = serp_analysis(query)
    return {"status": "ok", "results": results}


@apify_router.post("/crawl")
async def apify_crawl(request: dict):
    """Crawl website content."""
    url = request.get("url", "")
    results = crawl_website_content(url)
    return {"status": "ok", "results": results}


@apify_router.post("/local-businesses")
async def apify_local(request: dict):
    """Discover local businesses."""
    term = request.get("term", "restaurant")
    location = request.get("location", "Kampala, Uganda")
    results = discover_local_businesses(term, location)
    return {"status": "ok", "results": results}


@apify_router.post("/email-extractor")
async def apify_emails(request: dict):
    """Extract emails from website."""
    url = request.get("url", "")
    results = extract_emails_from_website(url)
    return {"status": "ok", "results": results}


@apify_router.post("/seo-check")
async def apify_seo(request: dict):
    """Run SEO check on a website."""
    url = request.get("url", "")
    results = seo_metadata_extractor(url)
    return {"status": "ok", "results": results}
