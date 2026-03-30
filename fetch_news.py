"""
Yemen News RSS Aggregator
Fetches from multiple RSS sources, translates Arabic to English,
categorizes stories, and outputs to docs/yemen_news.json
"""

import os
import json
import re
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import feedparser
from deep_translator import GoogleTranslator

# ── Configuration ─────────────────────────────────────────────────────────────

COUNTRY = "yemen"
OUTPUT_DIR = Path("docs")
OUTPUT_FILE = OUTPUT_DIR / f"{COUNTRY}_news.json"
MAX_AGE_DAYS = 7
MAX_PER_CATEGORY = 20

CATEGORIES = ["Diplomacy", "Military", "Energy", "Economy", "Local Events"]

# Category keyword mapping (checked against lowercased title + description)
CATEGORY_KEYWORDS = {
    "Diplomacy": [
        "diplomat", "peace", "negotiat", "ceasefire", "talks", "agreement",
        "envoy", "united nations", "un ", "sanction", "treaty", "ambassador",
        "foreign minister", "delegation", "accord", "truce", "mediat",
        "international", "saudi", "oman", "iran", "us ", "usa", "europe",
        "coalition", "houthi negotiat", "political solution",
    ],
    "Military": [
        "attack", "airstrike", "air strike", "missile", "drone", "uav",
        "military", "armed forces", "troops", "soldier", "weapon", "bomb",
        "explosion", "houthi", "front", "operation", "battle", "combat",
        "shelling", "artillery", "navy", "warship", "red sea", "ballistic",
        "intercept", "defense", "offensive", "advance", "clashes", "killed",
        "wounded", "casualties",
    ],
    "Energy": [
        "oil", "gas", "fuel", "energy", "electricity", "power plant",
        "pipeline", "petroleum", "lng", "refinery", "generator", "barrel",
        "opec", "hydrocarbon", "renewabl", "solar", "port fuel",
    ],
    "Economy": [
        "economy", "economic", "gdp", "inflation", "currency", "rial",
        "trade", "import", "export", "bank", "finance", "budget", "debt",
        "aid", "humanitarian aid", "food", "price", "market", "poverty",
        "unemployment", "investment", "revenue", "fiscal", "salary", "wage",
        "port", "shipping", "commerce",
    ],
    "Local Events": [
        "local", "city", "province", "governorate", "hospital", "school",
        "citizen", "village", "district", "sanaa", "aden", "taiz", "hodeidah",
        "marib", "hadramout", "weather", "flood", "earthquake", "drought",
        "protest", "demonstration", "community", "resident", "displaced",
        "refugee", "camp", "water", "health", "education", "culture",
        "festival", "election", "governor", "mayor",
    ],
}

RSS_SOURCES = [
    {
        "name": "Saba Net",
        "url": "https://saba.ye/en/showrss",
        "lang": "en",   # publishes in English; may include Arabic headlines
    },
    {
        "name": "Yemen Watch",
        "url": "https://yemenwatch.com/feed",
        "lang": "en",
    },
    {
        "name": "Yeni Yemen",
        "url": "https://yeniyemen.net/en/rsslink",
        "lang": "en",
    },
    {
        "name": "Yemen Children Platform",
        "url": "https://yemenschildren.net/en/rss/latest-posts",
        "lang": "en",
    },
    {
        "name": "Yemenat",
        "url": "https://yemenat.net/feed",
        "lang": "ar",   # Arabic — will be translated
    },
]

# ── Helpers ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

translator = GoogleTranslator(source="auto", target="en")


def story_id(url: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()[:12]


def safe_translate(text: str) -> str:
    """Translate text to English; return original if translation fails."""
    if not text or not text.strip():
        return text
    try:
        result = translator.translate(text)
        return result if result else text
    except Exception as exc:
        log.warning("Translation failed (%s): %s", exc, text[:60])
        return text


def parse_date(entry) -> datetime | None:
    """Return a timezone-aware datetime from a feedparser entry, or None."""
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text or "").strip()


def classify(title: str, description: str) -> str:
    """Return the best-matching category or 'Local Events' as default."""
    combined = (title + " " + description).lower()
    scores = {cat: 0 for cat in CATEGORIES}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in combined:
                scores[cat] += 1
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "Local Events"


def is_yemen_focused(title: str, description: str) -> bool:
    """Light relevance gate — keep stories where Yemen is the primary subject."""
    text = (title + " " + description).lower()
    yemen_terms = ["yemen", "yemeni", "sanaa", "aden", "houthi", "hadramout",
                   "marib", "taiz", "hodeidah", "ansarallah"]
    return any(t in text for t in yemen_terms)


# ── Core logic ────────────────────────────────────────────────────────────────

def fetch_entries() -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    stories: list[dict] = []

    for source in RSS_SOURCES:
        log.info("Fetching %s  →  %s", source["name"], source["url"])
        try:
            feed = feedparser.parse(
                source["url"],
                request_headers={
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; YemenNewsBot/1.0; "
                        "+https://stratagemdrive.github.io/yemen-local/)"
                    )
                },
            )
        except Exception as exc:
            log.error("Failed to fetch %s: %s", source["name"], exc)
            continue

        if feed.bozo and not feed.entries:
            log.warning("Bad feed from %s: %s", source["name"], feed.bozo_exception)
            continue

        for entry in feed.entries:
            pub_dt = parse_date(entry)

            # Skip entries outside the 7-day window
            if pub_dt and pub_dt < cutoff:
                continue

            raw_title = strip_html(getattr(entry, "title", "") or "")
            raw_desc = strip_html(
                getattr(entry, "summary", "")
                or getattr(entry, "description", "")
                or ""
            )
            url = getattr(entry, "link", "") or ""

            # Translate if Arabic source
            if source["lang"] == "ar":
                title = safe_translate(raw_title)
                desc = safe_translate(raw_desc[:500])  # limit to avoid rate limits
            else:
                title = raw_title
                desc = raw_desc

            if not title or not url:
                continue

            if not is_yemen_focused(title, desc):
                continue

            category = classify(title, desc)
            pub_str = pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ") if pub_dt else None

            stories.append(
                {
                    "_id": story_id(url),
                    "title": title,
                    "source": source["name"],
                    "url": url,
                    "published_date": pub_str,
                    "category": category,
                }
            )

    return stories


def load_existing() -> dict:
    if OUTPUT_FILE.exists():
        try:
            with OUTPUT_FILE.open(encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            log.warning("Could not load existing JSON: %s", exc)
    return {cat: [] for cat in CATEGORIES}


def merge(existing: dict, fresh: list[dict]) -> dict:
    """
    Merge fresh stories into the existing store per category.
    - Deduplicate by URL hash.
    - Fresh stories replace oldest entries when cap is exceeded.
    - Drop entries older than MAX_AGE_DAYS.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    result = {cat: [] for cat in CATEGORIES}

    # Seed with existing, pruning stale entries
    for cat in CATEGORIES:
        for item in existing.get(cat, []):
            pub = item.get("published_date")
            if pub:
                try:
                    dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                    if dt < cutoff:
                        continue
                except Exception:
                    pass
            result[cat].append(item)

    # Index existing by id for deduplication
    seen: dict[str, str] = {}  # id -> category
    for cat, items in result.items():
        for item in items:
            seen[item["_id"]] = cat

    # Add fresh stories
    for story in fresh:
        sid = story["_id"]
        cat = story["category"]
        if sid in seen:
            continue  # already stored
        result[cat].append(story)
        seen[sid] = cat

    # Enforce per-category cap: sort by date desc, trim oldest
    for cat in CATEGORIES:
        items = result[cat]
        items.sort(
            key=lambda x: x.get("published_date") or "",
            reverse=True,
        )
        result[cat] = items[:MAX_PER_CATEGORY]

    return result


def save(data: dict) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "country": COUNTRY,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "categories": CATEGORIES,
        "stories": data,
    }
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info("Saved → %s", OUTPUT_FILE)
    for cat in CATEGORIES:
        log.info("  %-16s %d stories", cat, len(data.get(cat, [])))


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=== Yemen News Fetch ===")
    fresh = fetch_entries()
    log.info("Fetched %d fresh candidates", len(fresh))
    existing = load_existing()
    merged = merge(existing, fresh)
    save(merged)
    log.info("Done.")


if __name__ == "__main__":
    main()
