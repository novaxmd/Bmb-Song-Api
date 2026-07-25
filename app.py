"""
Bmb Song Backend API — v9  (HuggingFace Spaces · 16 GB RAM · 2 vCPU)
=======================================================================

CHANGES IN v9
─────────────────────────────────────────────────────────────────────
REMOVED
  • Download feature (yt-dlp / download_bp.py) — fully stripped
  • Last.fm — removed; it requires a paid plan for geo charts

ADDED — FREE TRENDING SOURCE
  • Spotify Web API (Client Credentials, 100% free, no card needed)
      – Featured playlists + top tracks by country
      – Set SPOTIFY_CLIENT_ID + SPOTIFY_CLIENT_SECRET env vars
      – Token auto-refreshed every hour; falls back gracefully if unset
      – New endpoint: GET /spotify/trending?country=IN&limit=50

10-HOUR CHARTS & TRENDING CACHE
  • _cache_charts: dedicated TTLCache (TTL = 36 000 s / 10 h)
  • On first request for a new country → fetch + cache
  • All subsequent requests for that country → served from cache instantly
  • Disk write-through (diskcache) → survives across uvicorn worker
    restarts within the same OS process
  • Per-country asyncio.Lock prevents thundering-herd: if 50 users
    request "IN" charts simultaneously, only ONE fetch fires; the rest
    wait for the result

APPLE MUSIC PROXY
  • Dedicated `_get_apple_client()` always routes through proxy
    (when PROXY_USERNAME / PROXY_PASSWORD are set)

SESSION / CONCURRENCY SAFETY
  • Per-country asyncio.Lock (charts + trending separately)
  • Global Semaphore caps concurrent external fetches to 20
  • ThreadPoolExecutor stays at 8 workers (2 vCPU, I/O-bound YTM calls)
  • All mutable shared state protected by threading.Lock or asyncio.Lock

LOGGING
  • RequestIDMiddleware tags every request with a short UUID prefix
  • Each external fetch logs source, country, count, latency
  • HTTPException handler logs path + status + detail
  • Full tracebacks logged at DEBUG level for unexpected exceptions
  • Structured format:  HH:MM:SS  LEVEL  [req-id] message
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import re
import time
import threading
import traceback
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import diskcache
import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.middleware.base import BaseHTTPMiddleware
from ytmusicapi import YTMusic

# ─────────────────────────────────────────────────────────────────────────────
#  Logging  — request-ID aware
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("Bmb Song")

# ─────────────────────────────────────────────────────────────────────────────
#  Environment / proxy config
# ─────────────────────────────────────────────────────────────────────────────

_PROXY_HOST          = "p.webshare.io"
_PROXY_PORT          = "80"
_PROXY_USER          = os.environ.get("PROXY_USERNAME", "")
_PROXY_PASS          = os.environ.get("PROXY_PASSWORD", "")
_SPOTIFY_CLIENT_ID   = os.environ.get("SPOTIFY_CLIENT_ID", "")
_SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
DOCS_PASSWORD        = os.environ.get("DOCS_PASSWORD", "prakamya05")


def _build_proxies() -> dict | None:
    if not _PROXY_USER or not _PROXY_PASS:
        return None
    proxy_url = f"http://{_PROXY_USER}:{_PROXY_PASS}@{_PROXY_HOST}:{_PROXY_PORT}"
    return {"http": proxy_url, "https": proxy_url}


_PROXIES  = _build_proxies()
_PROXY_URL = (
    f"http://{_PROXY_USER}:{_PROXY_PASS}@{_PROXY_HOST}:{_PROXY_PORT}"
    if _PROXY_USER and _PROXY_PASS else None
)

# ─────────────────────────────────────────────────────────────────────────────
#  Docs Auth Middleware
# ─────────────────────────────────────────────────────────────────────────────

class DocsAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path in ("/docs", "/openapi.json", "/redoc"):
            auth = request.headers.get("Authorization", "")
            if auth.startswith("Basic "):
                try:
                    decoded  = base64.b64decode(auth[6:]).decode()
                    _, password = decoded.split(":", 1)
                    if password == DOCS_PASSWORD:
                        return await call_next(request)
                except Exception:
                    pass
            return Response(
                "Unauthorized", status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="Bmb Song Docs"'},
            )
        return await call_next(request)

# ─────────────────────────────────────────────────────────────────────────────
#  Request-ID Middleware  — adds X-Request-ID header + per-request log line
# ─────────────────────────────────────────────────────────────────────────────

class RequestIDMiddleware(BaseHTTPMiddleware):
    """Stamps every request with a short ID; logs method / path / status / ms."""
    async def dispatch(self, request: Request, call_next):
        rid = uuid.uuid4().hex[:8]
        request.state.request_id = rid
        t0 = time.monotonic()
        try:
            response = await call_next(request)
        except Exception as exc:
            elapsed = (time.monotonic() - t0) * 1000
            log.error("[%s] %s %s → 500 (%.1f ms) — unhandled: %s",
                      rid, request.method, request.url.path, elapsed, exc)
            log.debug("[%s] traceback:\n%s", rid, traceback.format_exc())
            raise
        elapsed = (time.monotonic() - t0) * 1000
        lvl = logging.WARNING if response.status_code >= 400 else logging.INFO
        log.log(lvl, "[%s] %s %s → %d (%.1f ms)",
                rid, request.method, request.url.path,
                response.status_code, elapsed)
        response.headers["X-Request-ID"] = rid
        return response

# ─────────────────────────────────────────────────────────────────────────────
#  Circuit breaker  (per external source)
# ─────────────────────────────────────────────────────────────────────────────

_CB_THRESHOLD  = 5
_CB_RESET_SECS = 60


class CircuitBreaker:
    def __init__(self, name: str):
        self.name       = name
        self._failures  = 0
        self._opened_at = 0.0
        self._lock      = threading.Lock()

    @property
    def available(self) -> bool:
        with self._lock:
            if self._failures < _CB_THRESHOLD:
                return True
            if time.monotonic() - self._opened_at > _CB_RESET_SECS:
                return True
            return False

    def success(self):
        with self._lock:
            self._failures  = 0
            self._opened_at = 0.0

    def failure(self):
        with self._lock:
            self._failures += 1
            if self._failures >= _CB_THRESHOLD:
                self._opened_at = time.monotonic()
                log.warning("Circuit breaker OPEN: %s", self.name)


_cb_ytm     = CircuitBreaker("ytmusicapi")
_cb_itunes  = CircuitBreaker("itunes")
_cb_deezer  = CircuitBreaker("deezer")
_cb_spotify = CircuitBreaker("spotify")

# ─────────────────────────────────────────────────────────────────────────────
#  Two-level cache  (L1 in-memory LRU + L2 disk)
# ─────────────────────────────────────────────────────────────────────────────

_DISK_CACHE_DIR  = os.environ.get("CACHE_DIR", "/tmp/ytm_cache")
_DISK_CACHE_SIZE = 600 * 1024 * 1024   # 600 MB (16 GB headroom)

_disk_cache = diskcache.Cache(
    _DISK_CACHE_DIR,
    size_limit=_DISK_CACHE_SIZE,
    eviction_policy="least-recently-used",
    statistics=True,
)

_DISK_TTL_SHORT  = 300
_DISK_TTL_MEDIUM = 1_800
_DISK_TTL_LONG   = 21_600
_DISK_TTL_CHARTS = 36_000   # 10 h — matches in-memory chart TTL


class TTLCache:
    """Thread-safe LRU in-memory cache with per-entry TTL + disk write-through."""

    def __init__(self, maxsize: int = 256, ttl: int = 300, disk_ttl: int = 0):
        self._store:   OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._maxsize  = maxsize
        self._ttl      = ttl
        self._disk_ttl = disk_ttl
        self._lock     = threading.Lock()

    def get(self, key: str) -> Any:
        with self._lock:
            if key in self._store:
                value, expires = self._store[key]
                if time.monotonic() <= expires:
                    self._store.move_to_end(key)
                    return value
                del self._store[key]
        if self._disk_ttl:
            try:
                value = _disk_cache.get(key)
                if value is not None:
                    self.set(key, value)
                    return value
            except Exception:
                pass
        return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        mem_ttl = ttl or self._ttl
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = (value, time.monotonic() + mem_ttl)
            while len(self._store) > self._maxsize:
                self._store.popitem(last=False)
        if self._disk_ttl:
            try:
                _disk_cache.set(key, value, expire=self._disk_ttl)
            except Exception:
                pass

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)
        try:
            _disk_cache.delete(key)
        except Exception:
            pass

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)


# 16 GB optimised — generous sizes
_cache_short  = TTLCache(maxsize=192,  ttl=120,    disk_ttl=_DISK_TTL_SHORT)
_cache_medium = TTLCache(maxsize=288,  ttl=600,    disk_ttl=_DISK_TTL_MEDIUM)
_cache_long   = TTLCache(maxsize=480,  ttl=3_600,  disk_ttl=_DISK_TTL_LONG)
#  ↓ Charts & trending: one fetch per country every 10 hours
_cache_charts = TTLCache(maxsize=500,  ttl=36_000, disk_ttl=_DISK_TTL_CHARTS)

# ─────────────────────────────────────────────────────────────────────────────
#  Shared async HTTP clients
#  • _get_http_client()   — general purpose (uses proxy when configured)
#  • _get_apple_client()  — Apple Music RSS; explicitly routes via proxy
# ─────────────────────────────────────────────────────────────────────────────

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_http_client:  httpx.AsyncClient | None = None
_apple_client: httpx.AsyncClient | None = None


def _make_client(proxy: str | None = None) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(20.0),
        limits=httpx.Limits(max_connections=60, max_keepalive_connections=30),
        follow_redirects=True,
        headers={
            "User-Agent":      _BROWSER_UA,
            "Accept":          "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        },
        **({"proxy": proxy} if proxy else {}),
    )


def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = _make_client(_PROXY_URL)
    return _http_client


def _get_apple_client() -> httpx.AsyncClient:
    """Always routes through proxy (when configured) for Apple Music RSS."""
    global _apple_client
    if _apple_client is None or _apple_client.is_closed:
        _apple_client = _make_client(_PROXY_URL)   # proxy if set, plain if not
    return _apple_client

# ─────────────────────────────────────────────────────────────────────────────
#  Global concurrency gate — caps simultaneous outbound fetches
# ─────────────────────────────────────────────────────────────────────────────

# Created lazily in async context (needs a running event loop)
_fetch_semaphore: asyncio.Semaphore | None = None


def _get_semaphore() -> asyncio.Semaphore:
    global _fetch_semaphore
    if _fetch_semaphore is None:
        _fetch_semaphore = asyncio.Semaphore(20)
    return _fetch_semaphore

# ─────────────────────────────────────────────────────────────────────────────
#  Per-country fetch locks — prevents thundering herd on cache miss
#  One lock dict for charts, one for trending (they fetch independently)
# ─────────────────────────────────────────────────────────────────────────────

_chart_locks:    Dict[str, asyncio.Lock] = {}
_trending_locks: Dict[str, asyncio.Lock] = {}


def _chart_lock(country: str) -> asyncio.Lock:
    key = country.upper()
    if key not in _chart_locks:
        _chart_locks[key] = asyncio.Lock()
    return _chart_locks[key]


def _trending_lock(country: str) -> asyncio.Lock:
    key = country.upper()
    if key not in _trending_locks:
        _trending_locks[key] = asyncio.Lock()
    return _trending_locks[key]

# ─────────────────────────────────────────────────────────────────────────────
#  Rate limiter
# ─────────────────────────────────────────────────────────────────────────────

limiter = Limiter(key_func=get_remote_address, default_limits=["300/minute"])

# ─────────────────────────────────────────────────────────────────────────────
#  Lifespan
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("═══ Bmb Song API v9 starting ═══")
    log.info("Proxy: %s", "enabled" if _PROXY_URL else "disabled")
    log.info("Spotify: %s", "configured" if _SPOTIFY_CLIENT_ID else "not configured")
    asyncio.create_task(_background_warmup())
    asyncio.create_task(_auto_refresh_charts())
    yield
    log.info("═══ Bmb Song API v9 shutting down ═══")
    for client in (_http_client, _apple_client):
        if client and not client.is_closed:
            await client.aclose()
    _disk_cache.close()

# ─────────────────────────────────────────────────────────────────────────────
#  App
# ─────────────────────────────────────────────────────────────────────────────

_START_TIME = time.monotonic()

app = FastAPI(
    title="Bmb Song API — v9",
    description=(
        "ytmusicapi + Apple Music + Deezer + Spotify. "
        "Per-country 10-hour chart cache. 2-vCPU / 16 GB optimised."
    ),
    version="9.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(RequestIDMiddleware)
app.add_middleware(DocsAuthMiddleware)
app.add_middleware(GZipMiddleware, minimum_size=300)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)

# 8 workers — 2 vCPU, ytmusicapi is I/O-bound
_executor = ThreadPoolExecutor(max_workers=8)


async def run(fn, *args, **kwargs):
    loop = asyncio.get_running_loop()
    def _safe():
        try:
            return fn(*args, **kwargs)
        except StopIteration as exc:
            raise RuntimeError(f"ytmusicapi StopIteration: {exc}") from exc
    return await loop.run_in_executor(_executor, _safe)

# ─────────────────────────────────────────────────────────────────────────────
#  Per-locale YTMusic instance pool  (LRU, capped at 20)
# ─────────────────────────────────────────────────────────────────────────────

_ytm_pool:    Dict[str, YTMusic] = {}
_ytm_pool_od: OrderedDict        = OrderedDict()
_ytm_lock     = threading.Lock()
_YTM_POOL_MAX = 20


def get_ytm(country: str = "ZZ", language: str = "en") -> YTMusic:
    country  = (country  or "ZZ").upper().strip()
    language = (language or "en").lower().strip()
    key = f"{country}:{language}"

    with _ytm_lock:
        if key in _ytm_pool:
            _ytm_pool_od.move_to_end(key, last=True)
            return _ytm_pool[key]
        if len(_ytm_pool) >= _YTM_POOL_MAX:
            oldest_key, _ = _ytm_pool_od.popitem(last=False)
            _ytm_pool.pop(oldest_key, None)
        try:
            location = country if country != "ZZ" else ""
            instance = YTMusic(language=language, location=location, proxies=_PROXIES)
        except Exception:
            try:
                instance = YTMusic(proxies=_PROXIES)
            except Exception as exc:
                raise RuntimeError(f"YTMusic init failed: {exc}") from exc
        _ytm_pool[key]    = instance
        _ytm_pool_od[key] = True
        log.info("YTMusic instance created: locale=%s", key)
        return instance

# ─────────────────────────────────────────────────────────────────────────────
#  Up-Next store
# ─────────────────────────────────────────────────────────────────────────────

_upnext_store: OrderedDict[str, Dict] = OrderedDict()
_upnext_lock  = threading.Lock()
_UPNEXT_TTL   = 7_200
_UPNEXT_MAX   = 100

# ─────────────────────────────────────────────────────────────────────────────
#  Thumbnail helpers
# ─────────────────────────────────────────────────────────────────────────────

_YT_QUALITY_RANK: Dict[str, int] = {
    "maxresdefault": 100, "sddefault": 70, "0": 65,
    "hqdefault": 50,      "mqdefault": 30, "2": 20,
    "1": 15,              "3": 10,         "default": 5,
}
_LH3_SIZE_RE = re.compile(r"=(w\d+(-h\d+)?|h\d+|s\d+)(-[a-zA-Z0-9_\-]*)*$")


def upgrade_thumbnail_url(url: str) -> str:
    if not url:
        return url
    try:
        if "lh3.googleusercontent.com" in url:
            url = _LH3_SIZE_RE.sub("", url)
            return url + "=w576-h576-l90-rj"
        if "i.ytimg.com/vi/" in url:
            url = re.sub(
                r"/(maxresdefault|sddefault|hqdefault|mqdefault|default|[0-3])\.jpg",
                "/maxresdefault.jpg", url,
            )
            return url
    except Exception:
        pass
    return url


def _thumb_score(t: Any) -> int:
    if isinstance(t, str):
        url, w, h = t, 0, 0
    else:
        url = t.get("url", "")
        w   = int(t.get("width",  0) or 0)
        h   = int(t.get("height", 0) or 0)
    if w > 0 and h > 0:
        return w * h
    m = re.search(r"=w(\d+)", url)
    if m:
        side = int(m.group(1))
        return side * side
    try:
        fname = url.rsplit("/", 1)[-1].split("?")[0].split(".")[0]
        rank  = _YT_QUALITY_RANK.get(fname)
        if rank:
            return rank * 10_000
    except Exception:
        pass
    return 0


def best_thumbnails_list(raw: Any) -> list:
    if not raw:
        return []
    if isinstance(raw, str):
        raw = [{"url": raw, "width": 0, "height": 0}]
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    cleaned = []
    for t in raw:
        if isinstance(t, str):
            t = {"url": t, "width": 0, "height": 0}
        url = t.get("url", "") if isinstance(t, dict) else ""
        if not url:
            continue
        upgraded = upgrade_thumbnail_url(url)
        cleaned.append({
            "url":    upgraded,
            "width":  int(t.get("width",  0) or 0),
            "height": int(t.get("height", 0) or 0),
        })
    if not cleaned:
        return []
    cleaned.sort(key=_thumb_score, reverse=True)
    return cleaned

# ─────────────────────────────────────────────────────────────────────────────
#  Normalizers
# ─────────────────────────────────────────────────────────────────────────────

def _norm_artists(raw: Any) -> list:
    if not raw:
        return []
    if isinstance(raw, str):
        return [{"name": raw, "id": ""}]
    if isinstance(raw, dict):
        return [{"name": raw.get("name", "") or raw.get("artist", ""), "id": raw.get("id", "")}]
    if isinstance(raw, list):
        out = []
        for a in raw:
            if isinstance(a, dict):
                out.append({"name": a.get("name", "") or a.get("artist", ""), "id": a.get("id", "") or a.get("browseId", "")})
            elif isinstance(a, str):
                out.append({"name": a, "id": ""})
        return out
    return []


def norm_track(t: dict) -> dict:
    raw = t.get("thumbnails") or t.get("thumbnail") or []
    if isinstance(raw, str):
        raw = [{"url": raw, "width": 0, "height": 0}]
    thumbs     = best_thumbnails_list(raw)
    album      = t.get("album")
    album_name = album.get("name", "") if isinstance(album, dict) else (album or "")
    return {
        "videoId":    t.get("videoId", ""),
        "title":      t.get("title", ""),
        "artists":    _norm_artists(t.get("artists") or t.get("artist")),
        "album":      album_name,
        "duration":   t.get("duration", ""),
        "thumbnails": thumbs,
        "thumbnail":  thumbs[0]["url"] if thumbs else "",
        "isExplicit": t.get("isExplicit", False),
        "year":       t.get("year", ""),
        "source":     t.get("source", "ytm"),
    }


def norm_artist_result(a: dict) -> dict:
    thumbs = best_thumbnails_list(a.get("thumbnails") or [])
    return {
        "browseId":    a.get("browseId", "") or a.get("channelId", ""),
        "name":        a.get("artist", "") or a.get("name", "") or a.get("title", ""),
        "subscribers": a.get("subscribers", ""),
        "thumbnails":  thumbs,
        "thumbnail":   thumbs[0]["url"] if thumbs else "",
    }


def norm_album_result(a: dict) -> dict:
    thumbs = best_thumbnails_list(a.get("thumbnails") or [])
    return {
        "browseId":   a.get("browseId", ""),
        "title":      a.get("title", ""),
        "artists":    _norm_artists(a.get("artists")),
        "year":       a.get("year", ""),
        "type":       a.get("type", "Album"),
        "thumbnails": thumbs,
        "thumbnail":  thumbs[0]["url"] if thumbs else "",
    }


def norm_playlist_result(p: dict) -> dict:
    thumbs = best_thumbnails_list(p.get("thumbnails") or [])
    return {
        "browseId":   p.get("browseId", "") or p.get("playlistId", ""),
        "title":      p.get("title", ""),
        "author":     p.get("author", ""),
        "itemCount":  p.get("itemCount", ""),
        "thumbnails": thumbs,
        "thumbnail":  thumbs[0]["url"] if thumbs else "",
    }


def norm_podcast_result(p: dict) -> dict:
    thumbs    = best_thumbnails_list(p.get("thumbnails") or [])
    browse_id = p.get("browseId") or p.get("podcastId") or p.get("channelId") or ""
    author    = p.get("author", "") or ", ".join(
        a.get("name", "") for a in _norm_artists(p.get("artists"))
    )
    return {
        "browseId":   browse_id,
        "title":      p.get("title", ""),
        "author":     author,
        "thumbnails": thumbs,
        "thumbnail":  thumbs[0]["url"] if thumbs else "",
    }


def norm_search_results(raw: list, filter_type: str | None) -> list:
    if not isinstance(raw, list):
        return []
    out = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        rt = (item.get("resultType") or filter_type or "").lower()
        if rt in ("song", "songs", "video", "videos"):
            n = norm_track(item)
            n["resultType"] = "video" if "video" in rt else "song"
            out.append(n)
        elif rt in ("artist", "artists"):
            n = norm_artist_result(item); n["resultType"] = "artist"; out.append(n)
        elif rt in ("album", "albums", "single", "singles", "ep"):
            n = norm_album_result(item); n["resultType"] = "album"; out.append(n)
        elif rt in ("playlist", "playlists"):
            n = norm_playlist_result(item); n["resultType"] = "playlist"; out.append(n)
        elif rt in ("podcast", "podcasts", "episode", "episodes"):
            n = norm_podcast_result(item); n["resultType"] = "podcast"; out.append(n)
        else:
            thumbs = best_thumbnails_list(item.get("thumbnails") or [])
            item["thumbnails"] = thumbs
            item["thumbnail"]  = thumbs[0]["url"] if thumbs else ""
            out.append(item)
    return out


def _normalise_home(raw: list) -> list:
    shelves = []
    for shelf in raw:
        if not isinstance(shelf, dict):
            continue
        contents = []
        for item in (shelf.get("contents") or []):
            if not isinstance(item, dict):
                continue
            if item.get("videoId"):
                contents.append(norm_track(item))
            else:
                thumbs = best_thumbnails_list(item.get("thumbnails") or [])
                item["thumbnails"] = thumbs
                item["thumbnail"]  = thumbs[0]["url"] if thumbs else ""
                contents.append(item)
        if contents:
            shelves.append({"title": shelf.get("title", "For You"), "contents": contents})
    return shelves


def _extract_chart_section(section: Any) -> list:
    if not section:
        return []
    items = section if isinstance(section, list) else (
        section.get("items") or section.get("results") or []
    )
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        thumbs = best_thumbnails_list(item.get("thumbnails") or [])
        item["thumbnails"] = thumbs
        item["thumbnail"]  = thumbs[0]["url"] if thumbs else ""
        out.append(item)
    return out


def _normalise_charts(raw: dict, country: str) -> dict:
    return {
        "country":  country,
        "songs":    [norm_track(t) for t in _extract_chart_section(raw.get("songs"))],
        "videos":   [norm_track(t) for t in _extract_chart_section(raw.get("videos"))
                     if t.get("videoId")],
        "artists":  [norm_artist_result(a) for a in _extract_chart_section(raw.get("artists"))],
        "trending": [norm_track(t) for t in _extract_chart_section(raw.get("trending"))
                     if t.get("videoId")],
    }


def _flatten_mood_categories(raw: Any) -> list:
    categories: list = []
    if isinstance(raw, dict):
        for section_title, items in raw.items():
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    thumbs = best_thumbnails_list(item.get("thumbnails") or [])
                    categories.append({
                        "title":      item.get("title", ""),
                        "params":     item.get("params", ""),
                        "section":    section_title,
                        "thumbnails": thumbs,
                        "thumbnail":  thumbs[0]["url"] if thumbs else "",
                    })
    elif isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            thumbs = best_thumbnails_list(item.get("thumbnails") or [])
            categories.append({
                "title":      item.get("title", ""),
                "params":     item.get("params", ""),
                "thumbnails": thumbs,
                "thumbnail":  thumbs[0]["url"] if thumbs else "",
            })
    return categories

# ─────────────────────────────────────────────────────────────────────────────
#  External API helpers — Apple Music
# ─────────────────────────────────────────────────────────────────────────────

_ITUNES_COUNTRY_FALLBACK = "us"


async def _fetch_itunes_trending(country: str, limit: int = 50) -> list:
    """Apple Music RSS top songs — country-aware, free, uses dedicated proxy client."""
    if not _cb_itunes.available:
        log.debug("Apple Music circuit breaker open — skipping fetch")
        return []
    c   = country.lower() if country and country not in ("ZZ", "zz") else _ITUNES_COUNTRY_FALLBACK
    url = f"https://rss.appleitunes.apple.com/api/v1/{c}/music/most-played/{limit}/songs/json"
    t0  = time.monotonic()
    try:
        async with _get_semaphore():
            resp = await _get_apple_client().get(url)
        resp.raise_for_status()
        results = resp.json().get("feed", {}).get("results") or []
        tracks  = []
        for i, item in enumerate(results):
            art_url = re.sub(r"\d+x\d+bb", "600x600bb", item.get("artworkUrl100", ""))
            tracks.append({
                "videoId":    "",
                "title":      item.get("name", ""),
                "artists":    [{"name": item.get("artistName", ""), "id": ""}],
                "album":      item.get("collectionName", ""),
                "duration":   "",
                "thumbnails": [{"url": art_url, "width": 600, "height": 600}] if art_url else [],
                "thumbnail":  art_url,
                "isExplicit": item.get("contentAdvisoryRating", "") == "Explicit",
                "year":       item.get("releaseDate", "")[:4],
                "rank":       i + 1,
                "source":     "apple_music",
                "genres":     [g.get("name", "") for g in item.get("genres", []) if isinstance(g, dict)],
                "url":        item.get("url", ""),
            })
        elapsed = (time.monotonic() - t0) * 1000
        _cb_itunes.success()
        log.info("Apple Music: %d tracks for country=%s (%.0f ms)", len(tracks), c, elapsed)
        return tracks
    except Exception as exc:
        _cb_itunes.failure()
        log.warning("Apple Music fetch FAILED country=%s: %s", c, exc)
        log.debug("Apple Music traceback:\n%s", traceback.format_exc())
        return []


async def _fetch_itunes_top_albums(country: str, limit: int = 20) -> list:
    """Apple Music RSS top albums."""
    c   = country.lower() if country and country not in ("ZZ", "zz") else "us"
    url = f"https://rss.appleitunes.apple.com/api/v1/{c}/music/most-played/{limit}/albums/json"
    try:
        async with _get_semaphore():
            resp = await _get_apple_client().get(url)
        resp.raise_for_status()
        results = resp.json().get("feed", {}).get("results") or []
        albums  = []
        for i, item in enumerate(results):
            art_url = re.sub(r"\d+x\d+bb", "600x600bb", item.get("artworkUrl100", ""))
            albums.append({
                "browseId":  "",
                "title":     item.get("name", ""),
                "artists":   [{"name": item.get("artistName", ""), "id": ""}],
                "year":      item.get("releaseDate", "")[:4],
                "thumbnail": art_url,
                "thumbnails":[{"url": art_url, "width": 600, "height": 600}] if art_url else [],
                "rank":      i + 1,
                "source":    "apple_music",
                "url":       item.get("url", ""),
            })
        return albums
    except Exception as exc:
        log.warning("Apple Music albums FAILED: %s", exc)
        return []

# ─────────────────────────────────────────────────────────────────────────────
#  External API helpers — Deezer
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_deezer_charts(limit: int = 50) -> dict:
    """Deezer global charts — free, no API key needed."""
    if not _cb_deezer.available:
        log.debug("Deezer circuit breaker open — skipping fetch")
        return {}
    t0 = time.monotonic()
    try:
        async with _get_semaphore():
            resp = await _get_http_client().get(
                "https://api.deezer.com/chart/0/tracks",
                params={"limit": limit},
            )
        resp.raise_for_status()
        data   = resp.json()
        tracks = []
        for i, item in enumerate(data.get("data") or []):
            art = (item.get("album", {}).get("cover_xl") or
                   item.get("album", {}).get("cover_big") or
                   item.get("album", {}).get("cover_medium", ""))
            tracks.append({
                "videoId":    "",
                "title":      item.get("title", ""),
                "artists":    [{"name": item.get("artist", {}).get("name", ""), "id": ""}],
                "album":      item.get("album", {}).get("title", ""),
                "duration":   str(item.get("duration", "")),
                "thumbnails": [{"url": art, "width": 500, "height": 500}] if art else [],
                "thumbnail":  art,
                "isExplicit": item.get("explicit_lyrics", False),
                "year":       "",
                "rank":       i + 1,
                "source":     "deezer",
                "deezerId":   str(item.get("id", "")),
                "preview":    item.get("preview", ""),
            })
        _cb_deezer.success()
        albums:  list = []
        artists: list = []
        for t_list, endpoint in [(albums, "albums"), (artists, "artists")]:
            try:
                async with _get_semaphore():
                    r2 = await _get_http_client().get(
                        f"https://api.deezer.com/chart/0/{endpoint}",
                        params={"limit": 20},
                    )
                r2.raise_for_status()
                for item in (r2.json().get("data") or []):
                    pic = (item.get("cover_xl") or item.get("cover_big") or
                           item.get("picture_xl") or item.get("picture_big") or
                           item.get("picture_medium", ""))
                    t_list.append({
                        "title":     item.get("title") or item.get("name", ""),
                        "artist":    item.get("artist", {}).get("name", "") if endpoint == "albums" else "",
                        "thumbnail": pic,
                        "source":    "deezer",
                        "deezerId":  str(item.get("id", "")),
                        "url":       item.get("link", ""),
                    })
            except Exception as exc:
                log.debug("Deezer %s fetch skipped: %s", endpoint, exc)
        elapsed = (time.monotonic() - t0) * 1000
        log.info("Deezer: %d chart tracks (%.0f ms)", len(tracks), elapsed)
        return {"tracks": tracks, "albums": albums, "artists": artists}
    except Exception as exc:
        _cb_deezer.failure()
        log.warning("Deezer charts FAILED: %s", exc)
        log.debug("Deezer traceback:\n%s", traceback.format_exc())
        return {}


async def _fetch_deezer_genres() -> list:
    """Deezer genre list."""
    if not _cb_deezer.available:
        return []
    try:
        async with _get_semaphore():
            resp = await _get_http_client().get("https://api.deezer.com/genre")
        resp.raise_for_status()
        genres = []
        for g in (resp.json().get("data") or []):
            pic = g.get("picture_xl") or g.get("picture_big") or g.get("picture_medium", "")
            genres.append({
                "id":        g.get("id"),
                "title":     g.get("name", ""),
                "thumbnail": pic,
                "thumbnails": [{"url": pic, "width": 500, "height": 500}] if pic else [],
                "source":    "deezer",
            })
        _cb_deezer.success()
        return genres
    except Exception as exc:
        _cb_deezer.failure()
        log.warning("Deezer genres FAILED: %s", exc)
        return []


async def _fetch_deezer_editorial_playlists(limit: int = 20) -> list:
    """Deezer editorial playlists (global)."""
    if not _cb_deezer.available:
        return []
    try:
        async with _get_semaphore():
            resp = await _get_http_client().get(
                "https://api.deezer.com/editorial/0/charts",
                params={"limit": limit},
            )
        resp.raise_for_status()
        data      = resp.json()
        playlists = []
        for item in (data.get("playlists", {}).get("data") or []):
            pic = item.get("picture_xl") or item.get("picture_big") or item.get("picture_medium", "")
            playlists.append({
                "browseId":   str(item.get("id", "")),
                "title":      item.get("title", ""),
                "subtitle":   item.get("description", ""),
                "thumbnail":  pic,
                "thumbnails": [{"url": pic, "width": 500, "height": 500}] if pic else [],
                "source":     "deezer",
            })
        _cb_deezer.success()
        return playlists
    except Exception as exc:
        _cb_deezer.failure()
        log.warning("Deezer editorial FAILED: %s", exc)
        return []

# ─────────────────────────────────────────────────────────────────────────────
#  External API helpers — Spotify  (Client Credentials, 100% free)
# ─────────────────────────────────────────────────────────────────────────────

# ── Spotify Android-client token state ───────────────────────────────────────
# We simulate the Spotify Android app.  Two separate tokens are maintained:
#   _spotify_client_token  — short-lived token from clienttoken.spotify.com
#                            (proves we are a known Spotify client; no secret)
#   _spotify_access_token  — Bearer token from accounts.spotify.com
#                            (used in Authorization header; needs client_secret)
# Both are refreshed automatically; all state protected by a threading.Lock.
# ─────────────────────────────────────────────────────────────────────────────

import secrets as _secrets

_SPOTIFY_DEVICE_ID = _secrets.token_hex(16)   # stable per-process, random across restarts

# Android Spotify app version strings (update periodically if Spotify changes them)
_SPOTIFY_ANDROID_VERSION    = "8.6.96.470"
_SPOTIFY_ANDROID_OS_VERSION = "30"            # Android 11

_spotify_access_token:        str   = ""
_spotify_access_expires:      float = 0.0
_spotify_client_token:        str   = ""
_spotify_client_token_expires: float = 0.0
_spotify_token_lock                  = threading.Lock()


async def _refresh_spotify_client_token() -> str:
    """
    Fetch a Spotify client-token via clienttoken.spotify.com.
    This endpoint accepts just the client_id (no secret) and returns a short-lived
    client-token that proves the caller is a known Spotify client application.
    Used identically by the real Spotify Android / iOS apps.
    """
    global _spotify_client_token, _spotify_client_token_expires
    if not _SPOTIFY_CLIENT_ID:
        return ""
    payload = {
        "client_data": {
            "client_version": _SPOTIFY_ANDROID_VERSION,
            "client_id":      _SPOTIFY_CLIENT_ID,
            "js_sdk_data": {
                "device_brand":   "Google",
                "device_model":   "sdk_gphone64_x86_64",
                "os":             "android",
                "os_version":     _SPOTIFY_ANDROID_OS_VERSION,
                "device_id":      _SPOTIFY_DEVICE_ID,
                "device_type":    "smartphone",
            },
        }
    }
    try:
        async with _get_semaphore():
            resp = await _get_http_client().post(
                "https://clienttoken.spotify.com/v1/clienttoken",
                json=payload,
                headers={"Accept": "application/json"},
            )
        resp.raise_for_status()
        data = resp.json()
        token = (
            data.get("granted_token", {}).get("token") or
            data.get("client_token") or ""
        )
        ttl = int(
            data.get("granted_token", {}).get("expires_after_seconds") or
            data.get("refresh_after_seconds") or
            1_800
        )
        with _spotify_token_lock:
            _spotify_client_token         = token
            _spotify_client_token_expires = time.monotonic() + ttl - 30
        log.info("Spotify: client-token refreshed (ttl=%ds)", ttl)
        return token
    except Exception as exc:
        log.warning("Spotify client-token refresh FAILED: %s", exc)
        log.debug("Spotify client-token traceback:\n%s", traceback.format_exc())
        return ""


async def _get_spotify_client_token() -> str:
    with _spotify_token_lock:
        if _spotify_client_token and time.monotonic() < _spotify_client_token_expires:
            return _spotify_client_token
    return await _refresh_spotify_client_token()


async def _refresh_spotify_access_token() -> str:
    """
    Fetch a Spotify Bearer access token via client_credentials grant.
    Uses the provided SPOTIFY_CLIENT_ID + SPOTIFY_CLIENT_SECRET (Android/iOS app creds).
    Does NOT require Spotify Premium — gives access to all public catalog endpoints.
    """
    global _spotify_access_token, _spotify_access_expires
    if not _SPOTIFY_CLIENT_ID or not _SPOTIFY_CLIENT_SECRET:
        return ""
    creds = base64.b64encode(
        f"{_SPOTIFY_CLIENT_ID}:{_SPOTIFY_CLIENT_SECRET}".encode()
    ).decode()
    try:
        async with _get_semaphore():
            resp = await _get_http_client().post(
                "https://accounts.spotify.com/api/token",
                headers={
                    "Authorization": f"Basic {creds}",
                    "Content-Type":  "application/x-www-form-urlencoded",
                },
                content=b"grant_type=client_credentials",
            )
        resp.raise_for_status()
        data = resp.json()
        with _spotify_token_lock:
            _spotify_access_token   = data["access_token"]
            _spotify_access_expires = time.monotonic() + data.get("expires_in", 3_600) - 30
        log.info("Spotify: access token refreshed (expires in %ds)", data.get("expires_in", 3_600))
        return _spotify_access_token
    except Exception as exc:
        log.warning("Spotify access token refresh FAILED: %s", exc)
        log.debug("Spotify token traceback:\n%s", traceback.format_exc())
        return ""


async def _get_spotify_tokens() -> tuple[str, str]:
    """Return (access_token, client_token). Either may be empty on failure."""
    with _spotify_token_lock:
        access_ok  = _spotify_access_token  and time.monotonic() < _spotify_access_expires
        client_ok  = _spotify_client_token  and time.monotonic() < _spotify_client_token_expires

    access_task  = (asyncio.sleep(0) if access_ok  else _refresh_spotify_access_token())
    client_task  = (asyncio.sleep(0) if client_ok  else _refresh_spotify_client_token())
    await asyncio.gather(access_task, client_task, return_exceptions=True)

    with _spotify_token_lock:
        return _spotify_access_token, _spotify_client_token


def _spotify_headers(access_token: str, client_token: str) -> dict:
    """Build the full set of headers the Spotify Android app sends."""
    h = {
        "Authorization":    f"Bearer {access_token}",
        "Accept":           "application/json",
        "Accept-Language":  "en",
        "app-platform":     "Android",
        "spotify-app-version": _SPOTIFY_ANDROID_VERSION,
        "User-Agent": (
            f"Spotify/{_SPOTIFY_ANDROID_VERSION} Android/{_SPOTIFY_ANDROID_OS_VERSION} "
            "(Google sdk_gphone64_x86_64)"
        ),
    }
    if client_token:
        h["client-token"] = client_token
    return h


async def _fetch_spotify_trending(country: str, limit: int = 50) -> list:
    """
    Fetch country trending tracks via the Spotify Android client approach:
      1. GET /v1/browse/featured-playlists  — country-aware editorial playlists
      2. GET /v1/browse/new-releases         — new albums/singles for the country
      3. Pull tracks from the top playlists using Android client headers.

    Falls back gracefully to [] when credentials are absent or the circuit is open.
    No Spotify Premium required.
    """
    if not _SPOTIFY_CLIENT_ID or not _SPOTIFY_CLIENT_SECRET:
        return []
    if not _cb_spotify.available:
        log.debug("Spotify circuit breaker open — skipping fetch")
        return []

    c = country.upper() if country and country not in ("ZZ", "zz") else "US"
    access_token, client_token = await _get_spotify_tokens()
    if not access_token:
        log.warning("Spotify: no access token available, skipping trending for %s", c)
        return []

    headers = _spotify_headers(access_token, client_token)
    t0      = time.monotonic()

    try:
        # ── 1. Featured playlists (country-localised) ──────────────────────
        async with _get_semaphore():
            fp_resp = await _get_http_client().get(
                "https://api.spotify.com/v1/browse/featured-playlists",
                headers=headers,
                params={"country": c, "limit": 5},
            )
        fp_resp.raise_for_status()
        playlists = fp_resp.json().get("playlists", {}).get("items") or []
        log.debug("Spotify: %d featured playlists for %s", len(playlists), c)

        tracks: list = []
        per_playlist  = max((limit // max(len(playlists), 1)) + 5, 15)

        for playlist in playlists[:4]:
            pid = playlist.get("id", "")
            if not pid:
                continue
            try:
                async with _get_semaphore():
                    tr_resp = await _get_http_client().get(
                        f"https://api.spotify.com/v1/playlists/{pid}/tracks",
                        headers=headers,
                        params={
                            "limit":  per_playlist,
                            "market": c,
                            "fields": (
                                "items(track("
                                "id,name,artists(name,id),album(name,images),"
                                "duration_ms,explicit,popularity))"
                            ),
                        },
                    )
                tr_resp.raise_for_status()
                for item in (tr_resp.json().get("items") or []):
                    t = item.get("track")
                    if not t or not isinstance(t, dict) or not t.get("id"):
                        continue
                    album  = t.get("album") or {}
                    images = album.get("images") or []
                    # pick largest image (Spotify orders by descending width)
                    art = images[0]["url"] if images else ""
                    tracks.append({
                        "videoId":    "",
                        "title":      t.get("name", ""),
                        "artists":    [
                            {"name": a.get("name", ""), "id": a.get("id", "")}
                            for a in (t.get("artists") or [])
                        ],
                        "album":      album.get("name", ""),
                        "duration":   str(int(t.get("duration_ms", 0)) // 1_000),
                        "thumbnails": [{"url": art, "width": 640, "height": 640}] if art else [],
                        "thumbnail":  art,
                        "isExplicit": t.get("explicit", False),
                        "year":       "",
                        "rank":       len(tracks) + 1,
                        "source":     "spotify",
                        "spotifyId":  t.get("id", ""),
                        "popularity": t.get("popularity", 0),
                    })
            except Exception as exc:
                log.debug("Spotify playlist %s track fetch skipped: %s", pid, exc)

        # ── 2. New releases as supplemental source ─────────────────────────
        if len(tracks) < limit // 2:
            try:
                async with _get_semaphore():
                    nr_resp = await _get_http_client().get(
                        "https://api.spotify.com/v1/browse/new-releases",
                        headers=headers,
                        params={"country": c, "limit": 10},
                    )
                nr_resp.raise_for_status()
                for album in (nr_resp.json().get("albums", {}).get("items") or []):
                    images = album.get("images") or []
                    art    = images[0]["url"] if images else ""
                    tracks.append({
                        "videoId":    "",
                        "title":      album.get("name", ""),
                        "artists":    [
                            {"name": a.get("name", ""), "id": a.get("id", "")}
                            for a in (album.get("artists") or [])
                        ],
                        "album":      album.get("name", ""),
                        "duration":   "",
                        "thumbnails": [{"url": art, "width": 640, "height": 640}] if art else [],
                        "thumbnail":  art,
                        "isExplicit": False,
                        "year":       (album.get("release_date") or "")[:4],
                        "rank":       len(tracks) + 1,
                        "source":     "spotify",
                        "spotifyId":  album.get("id", ""),
                        "popularity": 0,
                    })
            except Exception as exc:
                log.debug("Spotify new-releases skipped: %s", exc)

        elapsed = (time.monotonic() - t0) * 1000
        _cb_spotify.success()
        log.info("Spotify: %d tracks for country=%s (%.0f ms)", len(tracks), c, elapsed)
        return tracks[:limit]

    except Exception as exc:
        _cb_spotify.failure()
        log.warning("Spotify trending FAILED country=%s: %s", c, exc)
        log.debug("Spotify traceback:\n%s", traceback.format_exc())
        return []

# ─────────────────────────────────────────────────────────────────────────────
#  Cache-Control header helper
# ─────────────────────────────────────────────────────────────────────────────

def _cc(seconds: int) -> dict:
    return {"Cache-Control": f"public, max-age={seconds}, stale-while-revalidate=60"}

# ─────────────────────────────────────────────────────────────────────────────
#  Track deduplication
# ─────────────────────────────────────────────────────────────────────────────

def _deduplicate_tracks(tracks: list) -> list:
    seen, out = set(), []
    for t in tracks:
        title  = (t.get("title", "") or "").lower().strip()
        artist = ""
        arts   = t.get("artists", [])
        if arts and isinstance(arts[0], dict):
            artist = (arts[0].get("name", "") or "").lower().strip()
        key = f"{title}|{artist}"
        if key and key not in seen:
            seen.add(key)
            out.append(t)
    return out

# ─────────────────────────────────────────────────────────────────────────────
#  Chart merging
# ─────────────────────────────────────────────────────────────────────────────

def _merge_charts(
    country:  str,
    ytm:      dict,
    apple:    Any,
    deezer:   Any,
    spotify:  Any,
) -> dict:
    """Merge chart data from all sources. Apple Music is primary when YTM is thin."""
    apple_tracks   = apple   if isinstance(apple,   list) else []
    deezer_data    = deezer  if isinstance(deezer,  dict) else {}
    spotify_tracks = spotify if isinstance(spotify, list) else []
    deezer_tracks  = deezer_data.get("tracks", [])

    ytm_songs    = ytm.get("songs",    [])
    ytm_videos   = ytm.get("videos",   [])
    ytm_artists  = ytm.get("artists",  [])
    ytm_trending = ytm.get("trending", [])

    primary_songs = apple_tracks if len(apple_tracks) >= 5 else (ytm_songs or apple_tracks)

    return {
        "country":         country,
        "songs":           primary_songs,
        "ytm_songs":       ytm_songs,
        "videos":          ytm_videos,
        "artists":         ytm_artists,
        "trending":        ytm_trending,
        "apple_music_top": apple_tracks,
        "deezer_top":      deezer_tracks,
        "deezer_albums":   deezer_data.get("albums", []),
        "deezer_artists":  deezer_data.get("artists", []),
        "spotify_top":     spotify_tracks,
        "cached_at":       int(time.time()),
        "sources_used": {
            "ytm":     len(ytm_songs)      > 0,
            "apple":   len(apple_tracks)   > 0,
            "deezer":  len(deezer_tracks)  > 0,
            "spotify": len(spotify_tracks) > 0,
        },
    }

# ─────────────────────────────────────────────────────────────────────────────
#  Background warm-up & auto-refresh
# ─────────────────────────────────────────────────────────────────────────────

async def _do_fetch_charts(country: str, language: str) -> dict:
    """Core fetch function: YTM + Apple + Deezer + Spotify in parallel."""
    log.info("Charts fetch: country=%s lang=%s", country, language)
    ytm_task    = (run(get_ytm(country, language).get_charts, country)
                   if _cb_ytm.available else asyncio.sleep(0))
    apple_task  = _fetch_itunes_trending(country, 50)
    deezer_task = _fetch_deezer_charts(50)
    spotify_task = _fetch_spotify_trending(country, 50)

    ytm_raw, apple_tracks, deezer_data, spotify_tracks = await asyncio.gather(
        ytm_task, apple_task, deezer_task, spotify_task,
        return_exceptions=True,
    )

    ytm_result: dict = {}
    if isinstance(ytm_raw, Exception):
        _cb_ytm.failure()
        log.warning("YTMusic charts FAILED country=%s: %s", country, ytm_raw)
    elif isinstance(ytm_raw, dict) and ytm_raw:
        ytm_result = _normalise_charts(ytm_raw, country)
        _cb_ytm.success()

    result = _merge_charts(country, ytm_result, apple_tracks, deezer_data, spotify_tracks)
    key    = f"charts10h:{country}:{language}"
    _cache_charts.set(key, result)
    log.info("Charts cached: country=%s (songs=%d, apple=%d, spotify=%d)",
             country,
             len(result.get("ytm_songs", [])),
             len(result.get("apple_music_top", [])),
             len(result.get("spotify_top", [])))
    return result


async def _warm_home(country: str, language: str) -> None:
    try:
        raw     = await run(get_ytm(country, language).get_home, 6)
        shelves = _normalise_home(raw or [])
        if shelves:
            _cache_medium.set(f"home:{country}:{language}:6", shelves)
    except Exception as exc:
        log.debug("_warm_home country=%s: %s", country, exc)


async def _warm_moods(country: str, language: str) -> None:
    try:
        raw        = await run(get_ytm(country, language).get_mood_categories)
        categories = _flatten_mood_categories(raw)
        if categories:
            _cache_medium.set(f"mood_categories:{country}:{language}", categories)
    except Exception as exc:
        log.debug("_warm_moods country=%s: %s", country, exc)


async def _background_warmup():
    """Pre-populate caches for common locales shortly after startup."""
    await asyncio.sleep(5)
    log.info("Background warm-up starting …")
    warm_targets = [("ZZ", "en"), ("IN", "en"), ("US", "en"), ("GB", "en")]
    for country, lang in warm_targets:
        try:
            await _do_fetch_charts(country, lang)
        except Exception as exc:
            log.warning("Warm-up charts %s failed: %s", country, exc)
        try:
            await _warm_home(country, lang)
        except Exception:
            pass
        await asyncio.sleep(1)   # small stagger to avoid burst
    try:
        await _warm_moods("ZZ", "en")
    except Exception:
        pass
    log.info("Background warm-up complete.")


async def _auto_refresh_charts():
    """Re-fetch all known countries' charts once every 10 hours."""
    await asyncio.sleep(120)    # let warm-up finish first
    while True:
        await asyncio.sleep(36_000)    # 10 hours
        log.info("Auto-refresh: refreshing all cached chart countries …")
        # Refresh every country already known in the charts cache
        # (we track them via disk cache keys)
        refreshed = 0
        try:
            for k in list(_disk_cache.iterkeys()):
                sk = str(k)
                if not sk.startswith("charts10h:"):
                    continue
                parts = sk.split(":")
                if len(parts) == 3:
                    _, c, lang = parts
                    try:
                        await _do_fetch_charts(c, lang)
                        refreshed += 1
                    except Exception as exc:
                        log.warning("Auto-refresh failed for %s/%s: %s", c, lang, exc)
                    await asyncio.sleep(2)
        except Exception as exc:
            log.warning("Auto-refresh loop error: %s", exc)
        log.info("Auto-refresh complete: %d countries refreshed.", refreshed)

# ─────────────────────────────────────────────────────────────────────────────
#  Routes — meta
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def root():
    return {"name": "Bmb Song API", "version": "9.0.0", "status": "ok", "docs": "/docs"}


@app.get("/health", tags=["meta"])
async def health():
    hits, misses = _disk_cache.stats()
    return {
        "status":           "ok",
        "version":          "9.0.0",
        "uptime_seconds":   round(time.monotonic() - _START_TIME),
        "ytm_instances":    len(_ytm_pool),
        "proxy_enabled":    _PROXIES is not None,
        "spotify_enabled":  bool(_SPOTIFY_CLIENT_ID),
        "circuit_breakers": {
            "ytm":     "open" if not _cb_ytm.available     else "closed",
            "itunes":  "open" if not _cb_itunes.available  else "closed",
            "deezer":  "open" if not _cb_deezer.available  else "closed",
            "spotify": "open" if not _cb_spotify.available else "closed",
        },
        "chart_countries_cached": sum(
            1 for k in (_disk_cache.iterkeys() if True else [])
            if str(k).startswith("charts10h:")
        ),
        "disk_cache": {
            "hits":    hits,
            "misses":  misses,
            "size_mb": round(_disk_cache.volume() / 1_048_576, 1),
        },
    }


@app.get("/cache_stats", tags=["meta"])
async def cache_stats():
    hits, misses = _disk_cache.stats()
    return {
        "memory": {
            "short":  len(_cache_short),
            "medium": len(_cache_medium),
            "long":   len(_cache_long),
            "charts": len(_cache_charts),
        },
        "disk": {
            "entries":  len(_disk_cache),
            "size_mb":  round(_disk_cache.volume() / 1_048_576, 1),
            "hits":     hits,
            "misses":   misses,
            "hit_rate": f"{hits/(hits+misses)*100:.1f}%" if (hits + misses) > 0 else "n/a",
        },
        "upnext":   len(_upnext_store),
        "ytm_pool": len(_ytm_pool),
    }


@app.delete("/cache", tags=["meta"])
async def clear_cache(key_prefix: str = ""):
    cleared = 0
    if key_prefix:
        for k in list(_disk_cache.iterkeys()):
            if str(k).startswith(key_prefix):
                _disk_cache.delete(k)
                cleared += 1
    else:
        _disk_cache.clear()
        cleared = -1
    log.info("Cache cleared: prefix=%r entries=%s", key_prefix or "(all)", cleared)
    return {"cleared": cleared if cleared >= 0 else "all"}

# ─────────────────────────────────────────────────────────────────────────────
#  Search
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/search", tags=["search"])
@limiter.limit("40/minute")
async def search(
    request:  Request,
    response: Response,
    query:    str,
    filter:   Optional[str] = Query(None),
    scope:    Optional[str] = Query(None),
    limit:    int  = Query(20, ge=1, le=50),
    offset:   int  = Query(0, ge=0),
    ignore_spelling: bool = False,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
):
    fetch_limit = min(offset + limit, 50)
    cache_key   = f"search:{query}:{filter}:{fetch_limit}:{country}:{language}"
    cached      = _cache_short.get(cache_key)

    if cached is None:
        if not _cb_ytm.available:
            raise HTTPException(503, detail="YTMusic source temporarily unavailable")
        try:
            raw    = await run(
                get_ytm(country, language).search,
                query, filter, scope, fetch_limit, ignore_spelling,
            )
            cached = norm_search_results(raw or [], filter)
            _cache_short.set(cache_key, cached)
            _cb_ytm.success()
        except Exception as e:
            _cb_ytm.failure()
            log.error("Search failed q=%r: %s", query, e)
            raise HTTPException(500, detail=str(e))

    response.headers.update(_cc(120))
    return cached[offset:offset + limit]


@app.get("/search_suggestions", tags=["search"])
@limiter.limit("40/minute")
async def search_suggestions(
    request:  Request,
    response: Response,
    query:    str,
    detailed: bool = False,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
):
    cache_key = f"suggestions:{query}:{detailed}:{country}:{language}"
    cached    = _cache_short.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(60))
        return cached
    try:
        data = await run(get_ytm(country, language).get_search_suggestions, query, detailed)
        _cache_short.set(cache_key, data or [])
        response.headers.update(_cc(60))
        return data or []
    except Exception as e:
        log.warning("Search suggestions failed q=%r: %s", query, e)
        raise HTTPException(500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
#  Home feed
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/home", tags=["discovery"])
@limiter.limit("30/minute")
async def get_home(
    request:  Request,
    response: Response,
    limit:    int = Query(6, ge=1, le=15),
    country:  str = Query("ZZ"),
    language: str = Query("en"),
):
    cache_key = f"home:{country}:{language}:{limit}"
    cached    = _cache_medium.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(300))
        return cached

    try:
        raw     = await run(get_ytm(country, language).get_home, limit)
        shelves = _normalise_home(raw or [])
    except Exception as exc:
        log.warning("Home YTM failed country=%s: %s", country, exc)
        shelves = []

    # Fallback: synthesise home from Apple Music + Spotify when YTM is empty
    if not shelves:
        apple_task   = _fetch_itunes_trending(country, 20)
        spotify_task = _fetch_spotify_trending(country, 20)
        apple_tracks, spotify_tracks = await asyncio.gather(
            apple_task, spotify_task, return_exceptions=True
        )
        if isinstance(apple_tracks, list) and apple_tracks:
            shelves.append({"title": "Top Songs", "contents": apple_tracks[:20]})
        if isinstance(spotify_tracks, list) and spotify_tracks:
            shelves.append({"title": "Trending Worldwide", "contents": spotify_tracks[:20]})

    _cache_medium.set(cache_key, shelves)
    response.headers.update(_cc(300))
    return shelves

# ─────────────────────────────────────────────────────────────────────────────
#  Charts  — 10-hour per-country cache with stampede protection
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/charts", tags=["discovery"])
@limiter.limit("30/minute")
async def get_charts(
    request:  Request,
    response: Response,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
    sources:  str = Query("all", description="all | ytm | apple | deezer | spotify"),
):
    """
    Country-aware charts from all sources.
    Cached per-country for 10 hours. A new country triggers one fetch;
    all concurrent callers wait on a per-country lock for the result.
    """
    cache_key = f"charts10h:{country}:{language}"
    cached    = _cache_charts.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(36_000))
        return cached

    # Per-country lock: only one fetch in-flight at a time
    async with _chart_lock(country):
        # Double-check after acquiring lock
        cached = _cache_charts.get(cache_key)
        if cached is not None:
            response.headers.update(_cc(36_000))
            return cached

        try:
            result = await _do_fetch_charts(country, language)
        except Exception as exc:
            log.error("Charts fetch error country=%s: %s", country, exc)
            log.debug("Charts traceback:\n%s", traceback.format_exc())
            raise HTTPException(500, detail=f"Charts fetch failed: {exc}")

    response.headers.update(_cc(36_000))
    return result

# ─────────────────────────────────────────────────────────────────────────────
#  Trending  — 10-hour per-country cache with stampede protection
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/trending", tags=["discovery"])
@limiter.limit("30/minute")
async def get_trending(
    request:  Request,
    response: Response,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
    limit:    int = Query(25, ge=1, le=100),
    sources:  str = Query("all", description="all | ytm | apple | deezer | spotify"),
):
    """
    Trending songs merged from Apple Music + Deezer + Spotify (+YTM when warm).
    Cached per-country for 10 hours. First request per country triggers one fetch.
    """
    cache_key = f"trending10h:{country}:{language}"
    cached    = _cache_charts.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(36_000))
        # Return slice of limit from cached merged list
        out = dict(cached)
        out["merged"] = out.get("merged", [])[:limit]
        return out

    async with _trending_lock(country):
        cached = _cache_charts.get(cache_key)
        if cached is not None:
            response.headers.update(_cc(36_000))
            out = dict(cached)
            out["merged"] = out.get("merged", [])[:limit]
            return out

        log.info("Trending fetch: country=%s lang=%s", country, language)

        use_apple   = sources in ("all", "apple")
        use_deezer  = sources in ("all", "deezer")
        use_spotify = sources in ("all", "spotify")
        use_ytm     = sources in ("all", "ytm")

        apple_task   = _fetch_itunes_trending(country, 50)  if use_apple   else asyncio.sleep(0)
        deezer_task  = _fetch_deezer_charts(50)             if use_deezer  else asyncio.sleep(0)
        spotify_task = _fetch_spotify_trending(country, 50) if use_spotify else asyncio.sleep(0)

        # Use YTM from charts cache if already warm
        ytm_trending: list = []
        if use_ytm and _cb_ytm.available:
            chart_cached = _cache_charts.get(f"charts10h:{country}:{language}")
            if chart_cached:
                ytm_trending = (chart_cached.get("trending") or
                                chart_cached.get("ytm_songs") or [])

        apple_tracks, deezer_data, spotify_tracks = await asyncio.gather(
            apple_task, deezer_task, spotify_task,
            return_exceptions=True,
        )
        apple_tracks   = apple_tracks   if isinstance(apple_tracks,   list) else []
        deezer_tracks  = (deezer_data.get("tracks", [])
                          if isinstance(deezer_data, dict) else [])
        spotify_tracks = spotify_tracks if isinstance(spotify_tracks, list) else []

        merged = _deduplicate_tracks(
            apple_tracks[:50] + spotify_tracks[:50] + ytm_trending[:50]
        )

        result = {
            "country":      country,
            "trending":     ytm_trending[:50],
            "apple_top":    apple_tracks[:50],
            "deezer_top":   deezer_tracks[:50],
            "spotify_top":  spotify_tracks[:50],
            "merged":       merged,
            "cached_at":    int(time.time()),
            "sources_used": {
                "ytm":     len(ytm_trending)   > 0,
                "apple":   len(apple_tracks)   > 0,
                "deezer":  len(deezer_tracks)  > 0,
                "spotify": len(spotify_tracks) > 0,
            },
        }
        _cache_charts.set(cache_key, result)
        log.info("Trending cached: country=%s merged=%d", country, len(merged))

    out = dict(result)
    out["merged"] = out["merged"][:limit]
    response.headers.update(_cc(36_000))
    return out

# ─────────────────────────────────────────────────────────────────────────────
#  Apple Music direct endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/apple_music/top_songs", tags=["apple_music"])
@limiter.limit("30/minute")
async def apple_music_top_songs(
    request:  Request,
    response: Response,
    country:  str = Query("us"),
    limit:    int = Query(50, ge=1, le=100),
):
    cache_key = f"am_songs:{country}:{limit}"
    cached    = _cache_charts.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(36_000))
        return cached
    tracks = await _fetch_itunes_trending(country, limit)
    if not tracks:
        raise HTTPException(503, detail="Apple Music RSS unavailable")
    result = {"country": country, "tracks": tracks, "count": len(tracks)}
    _cache_charts.set(cache_key, result)
    response.headers.update(_cc(36_000))
    return result


@app.get("/apple_music/top_albums", tags=["apple_music"])
@limiter.limit("30/minute")
async def apple_music_top_albums(
    request:  Request,
    response: Response,
    country:  str = Query("us"),
    limit:    int = Query(20, ge=1, le=50),
):
    cache_key = f"am_albums:{country}:{limit}"
    cached    = _cache_medium.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(1_800))
        return cached
    albums = await _fetch_itunes_top_albums(country, limit)
    if not albums:
        raise HTTPException(503, detail="Apple Music RSS unavailable")
    result = {"country": country, "albums": albums, "count": len(albums)}
    _cache_medium.set(cache_key, result)
    response.headers.update(_cc(1_800))
    return result

# ─────────────────────────────────────────────────────────────────────────────
#  Deezer direct endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/deezer/charts", tags=["deezer"])
@limiter.limit("30/minute")
async def deezer_charts_endpoint(
    request:  Request,
    response: Response,
    limit:    int = Query(50, ge=1, le=100),
):
    cache_key = f"deezer_charts:{limit}"
    cached    = _cache_charts.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(36_000))
        return cached
    data = await _fetch_deezer_charts(limit)
    if not data:
        raise HTTPException(503, detail="Deezer unavailable")
    _cache_charts.set(cache_key, data)
    response.headers.update(_cc(36_000))
    return data

# ─────────────────────────────────────────────────────────────────────────────
#  Spotify direct endpoint
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/spotify/trending", tags=["spotify"])
@limiter.limit("30/minute")
async def spotify_trending(
    request:  Request,
    response: Response,
    country:  str = Query("US"),
    limit:    int = Query(50, ge=1, le=100),
):
    """Spotify featured playlists trending tracks by country (free, no login needed)."""
    if not _SPOTIFY_CLIENT_ID:
        raise HTTPException(503, detail="Spotify not configured (set SPOTIFY_CLIENT_ID + SPOTIFY_CLIENT_SECRET)")
    cache_key = f"spotify_trending:{country}:{limit}"
    cached    = _cache_charts.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(36_000))
        return cached
    tracks = await _fetch_spotify_trending(country, limit)
    if not tracks:
        raise HTTPException(503, detail="Spotify trending unavailable")
    result = {"country": country, "tracks": tracks, "count": len(tracks)}
    _cache_charts.set(cache_key, result)
    response.headers.update(_cc(36_000))
    return result

# ─────────────────────────────────────────────────────────────────────────────
#  Song metadata
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/song/{video_id}", tags=["song"])
@limiter.limit("60/minute")
async def get_song(
    request:  Request,
    response: Response,
    video_id: str,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
):
    cache_key = f"song:{video_id}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(3_600))
        return cached
    try:
        data = await run(get_ytm(country, language).get_song, video_id)
        raw  = (
            data.get("thumbnail", {}).get("thumbnails") or
            data.get("thumbnails") or []
        )
        thumbs = best_thumbnails_list(raw)
        data["thumbnails"] = thumbs
        data["thumbnail"]  = thumbs[0]["url"] if thumbs else ""
        _cache_long.set(cache_key, data)
        response.headers.update(_cc(3_600))
        return data
    except Exception as e:
        log.error("get_song %s: %s", video_id, e)
        raise HTTPException(500, detail=str(e))



@app.get("/video_info/{video_id}", tags=["song"])
@limiter.limit("120/minute")
async def get_video_info(
    request:  Request,
    response: Response,
    video_id: str,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
):
    """
    Lightweight metadata endpoint used by the frontend when a song is played
    via a raw YouTube link (no title/thumbnail in URL params).
    Returns: videoId, title, artist, thumbnail, duration_seconds, thumbnails[].
    Cached for 1 hour (same as /stream).
    """
    cache_key = f"video_info:{video_id}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(3_600))
        return cached
    try:
        song_data = await run(get_ytm(country, language).get_song, video_id)
        vd = song_data.get("videoDetails", {})

        raw_thumbs = (
            vd.get("thumbnail", {}).get("thumbnails") or
            song_data.get("thumbnail", {}).get("thumbnails") or
            song_data.get("thumbnails") or []
        )
        thumbs = best_thumbnails_list(raw_thumbs)

        result = {
            "videoId":          video_id,
            "title":            vd.get("title", ""),
            "artist":           vd.get("author", ""),
            "channelId":        vd.get("channelId", ""),
            "duration_seconds": int(vd.get("lengthSeconds") or 0),
            "views":            int(vd.get("viewCount") or 0),
            "thumbnails":       thumbs,
            "thumbnail":        thumbs[0]["url"] if thumbs else "",
            "isLive":           bool(vd.get("isLiveContent", False)),
        }
        _cache_long.set(cache_key, result)
        response.headers.update(_cc(3_600))
        return result
    except HTTPException:
        raise
    except Exception as e:
        log.error("get_video_info %s: %s", video_id, e)
        raise HTTPException(500, detail=str(e))

@app.get("/stream/{video_id}", tags=["song"])
@limiter.limit("60/minute")
async def get_stream(
    request:  Request,
    response: Response,
    video_id: str,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
):
    cache_key = f"stream:{video_id}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(3_600))
        return cached
    try:
        song_data = await run(get_ytm(country, language).get_song, video_id)
        vd = song_data.get("videoDetails", {})
        if not vd:
            raise HTTPException(404, "Video not found")
        raw    = (vd.get("thumbnail", {}).get("thumbnails") or
                  song_data.get("thumbnail", {}).get("thumbnails") or [])
        thumbs = best_thumbnails_list(raw)
        result = {
            "video_id":          video_id,
            "videoId":           video_id,
            "url":               f"https://www.youtube.com/watch?v={video_id}",
            "audio_url":         f"https://www.youtube.com/watch?v={video_id}",
            "stream_url":        f"https://www.youtube.com/watch?v={video_id}",
            "title":             vd.get("title", ""),
            "artist":            vd.get("author", ""),
            "channel_id":        vd.get("channelId", ""),
            "duration_seconds":  int(vd.get("lengthSeconds") or 0),
            "views":             int(vd.get("viewCount") or 0),
            "keywords":          vd.get("keywords", [])[:10],
            "is_live":           vd.get("isLiveContent", False),
            "thumbnails":        thumbs,
            "thumbnail":         thumbs[0]["url"] if thumbs else "",
        }
        _cache_long.set(cache_key, result)
        response.headers.update(_cc(3_600))
        return result
    except HTTPException:
        raise
    except Exception as e:
        log.error("get_stream %s: %s", video_id, e)
        raise HTTPException(500, detail=str(e))


@app.get("/now_playing/{video_id}", tags=["song"])
@limiter.limit("60/minute")
async def now_playing(
    request:       Request,
    response:      Response,
    video_id:      str,
    related_limit: int = Query(10, ge=1, le=30),
    country:       str = Query("ZZ"),
    language:      str = Query("en"),
):
    cache_key = f"now_playing:{video_id}:{related_limit}:{country}:{language}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(1_800))
        return cached

    ytm = get_ytm(country, language)
    song_data, watch_data = await asyncio.gather(
        run(ytm.get_song, video_id),
        run(ytm.get_watch_playlist, video_id, None, related_limit + 1),
        return_exceptions=True,
    )

    stream: dict = {}
    if isinstance(song_data, dict):
        vd     = song_data.get("videoDetails", {})
        raw    = (vd.get("thumbnail", {}).get("thumbnails") or
                  song_data.get("thumbnail", {}).get("thumbnails") or [])
        thumbs = best_thumbnails_list(raw)
        stream = {
            "videoId":          video_id,
            "url":              f"https://www.youtube.com/watch?v={video_id}",
            "audio_url":        f"https://www.youtube.com/watch?v={video_id}",
            "title":            vd.get("title", ""),
            "artist":           vd.get("author", ""),
            "duration_seconds": int(vd.get("lengthSeconds") or 0),
            "thumbnails":       thumbs,
            "thumbnail":        thumbs[0]["url"] if thumbs else "",
        }

    related: list = []
    if isinstance(watch_data, dict):
        tracks_raw = watch_data.get("tracks") or []
        if tracks_raw and tracks_raw[0].get("videoId") == video_id:
            tracks_raw = tracks_raw[1:]
        related = [norm_track(t) for t in tracks_raw[:related_limit] if t.get("videoId")]

    result = {"videoId": video_id, "stream": stream, "related": related}
    _cache_long.set(cache_key, result, ttl=1_800)
    response.headers.update(_cc(1_800))
    return result


@app.get("/related_songs/{video_id}", tags=["song"])
@limiter.limit("30/minute")
async def get_related_songs(
    request:   Request,
    response:  Response,
    video_id:  str,
    limit:     int = Query(15, ge=1, le=50),
    country:   str = Query("ZZ"),
    language:  str = Query("en"),
):
    cache_key = f"related:{video_id}:{limit}:{country}:{language}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(1_800))
        return cached
    try:
        raw        = await run(get_ytm(country, language).get_watch_playlist, video_id, None, limit + 1)
        tracks_raw = raw.get("tracks") or []
        if tracks_raw and tracks_raw[0].get("videoId") == video_id:
            tracks_raw = tracks_raw[1:]
        tracks = [norm_track(t) for t in tracks_raw[:limit] if t.get("videoId")]
        result = {"videoId": video_id, "tracks": tracks, "count": len(tracks)}
        _cache_long.set(cache_key, result, ttl=1_800)
        response.headers.update(_cc(1_800))
        return result
    except Exception as e:
        log.error("related_songs %s: %s", video_id, e)
        raise HTTPException(500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
#  Up-Next
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/upnext/{video_id}", tags=["song"])
@limiter.limit("30/minute")
async def get_upnext(
    request:       Request,
    response:      Response,
    video_id:      str,
    limit:         int  = Query(20, ge=5, le=50),
    force_refresh: bool = Query(False),
    country:       str = Query("ZZ"),
    language:      str = Query("en"),
):
    store_key = f"{video_id}:{country}:{language}"
    now       = time.time()
    with _upnext_lock:
        existing = _upnext_store.get(store_key)
    if existing and not force_refresh:
        if now - existing.get("created_at", 0) < _UPNEXT_TTL:
            response.headers.update(_cc(300))
            return existing
    try:
        raw        = await run(get_ytm(country, language).get_watch_playlist, video_id, None, limit)
        tracks_raw = raw.get("tracks") or []
        if tracks_raw and tracks_raw[0].get("videoId") == video_id:
            tracks_raw = tracks_raw[1:]
        tracks = [norm_track(t) for t in tracks_raw if t.get("videoId")]
        queue  = {
            "origin_video_id": video_id,
            "tracks":          tracks,
            "count":           len(tracks),
            "created_at":      now,
            "country":         country,
        }
        with _upnext_lock:
            _upnext_store[store_key] = queue
            _upnext_store.move_to_end(store_key)
            while len(_upnext_store) > _UPNEXT_MAX:
                _upnext_store.popitem(last=False)
        response.headers.update(_cc(300))
        return queue
    except Exception as e:
        log.error("upnext %s: %s", video_id, e)
        raise HTTPException(500, detail=str(e))


@app.delete("/upnext/{video_id}", tags=["song"])
async def reset_upnext(video_id: str, country: str = Query("ZZ"), language: str = Query("en")):
    store_key = f"{video_id}:{country}:{language}"
    with _upnext_lock:
        _upnext_store.pop(store_key, None)
    return {"cleared": store_key}

# ─────────────────────────────────────────────────────────────────────────────
#  Artist
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/artist/{artist_id}", tags=["artist"])
@limiter.limit("30/minute")
async def get_artist(
    request:   Request,
    response:  Response,
    artist_id: str,
    country:   str = Query("ZZ"),
    language:  str = Query("en"),
):
    cache_key = f"artist:{artist_id}:{country}:{language}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(3_600))
        return cached
    try:
        data = await run(get_ytm(country, language).get_artist, artist_id)
        if not data:
            raise HTTPException(404, "Artist not found")
        data["thumbnails"] = best_thumbnails_list(data.get("thumbnails") or [])
        data["thumbnail"]  = data["thumbnails"][0]["url"] if data["thumbnails"] else ""
        _cache_long.set(cache_key, data)
        response.headers.update(_cc(3_600))
        return data
    except HTTPException:
        raise
    except Exception as e:
        log.error("get_artist %s: %s", artist_id, e)
        raise HTTPException(500, detail=str(e))


@app.get("/artist/{artist_id}/songs", tags=["artist"])
@limiter.limit("20/minute")
async def get_artist_songs(
    request:   Request,
    response:  Response,
    artist_id: str,
    limit:     int = Query(20, ge=1, le=100),
    country:   str = Query("ZZ"),
    language:  str = Query("en"),
):
    cache_key = f"artist_songs:{artist_id}:{limit}:{country}:{language}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(600))
        return cached
    try:
        ytm         = get_ytm(country, language)
        artist_data = await run(ytm.get_artist, artist_id)
        if not artist_data:
            raise HTTPException(404, "Artist not found")
        artist_name   = artist_data.get("name", "")
        songs_section = artist_data.get("songs", {})
        all_tracks: list  = []
        albums_info: list = []

        if isinstance(songs_section, dict) and songs_section.get("browseId"):
            try:
                songs_raw = await run(ytm.get_artist_songs, artist_id, songs_section.get("params"))
                all_tracks.extend(songs_raw or [])
            except Exception:
                all_tracks.extend(songs_section.get("results") or [])
        else:
            all_tracks.extend(songs_section.get("results") or [])

        for section_key in ("albums", "singles"):
            section = artist_data.get(section_key, {})
            if not isinstance(section, dict):
                continue
            entries = section.get("results") or section.get("items") or []
            for entry in entries[:3]:
                thumbs = best_thumbnails_list(entry.get("thumbnails") or [])
                albums_info.append({
                    "browseId":   entry.get("browseId", ""),
                    "title":      entry.get("title", ""),
                    "year":       entry.get("year", ""),
                    "type":       entry.get("type", "Album"),
                    "thumbnails": thumbs,
                    "thumbnail":  thumbs[0]["url"] if thumbs else "",
                    "trackCount": len(entry.get("tracks") or []),
                })
                for t in entry.get("tracks") or []:
                    if isinstance(t, dict) and t.get("videoId"):
                        if not t.get("album"):
                            t = dict(t)
                            t["album"] = {"name": entry.get("title", "")}
                        all_tracks.append(t)

        seen, deduped = set(), []
        for t in all_tracks:
            vid = t.get("videoId", "")
            if vid and vid not in seen:
                seen.add(vid)
                deduped.append(norm_track(t))

        result = {
            "artistId": artist_id,
            "name":     artist_name,
            "songs":    deduped[:limit],
            "total":    len(deduped),
            "albums":   albums_info,
        }
        _cache_long.set(cache_key, result)
        response.headers.update(_cc(600))
        return result
    except HTTPException:
        raise
    except Exception as e:
        log.error("artist_songs %s: %s", artist_id, e)
        raise HTTPException(500, detail=str(e))


@app.get("/artist/{artist_id}/albums", tags=["artist"])
@limiter.limit("20/minute")
async def get_artist_albums(
    request:   Request,
    response:  Response,
    artist_id: str,
    country:   str = Query("ZZ"),
    language:  str = Query("en"),
):
    cache_key = f"artist_albums:{artist_id}:{country}:{language}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(600))
        return cached
    try:
        ytm         = get_ytm(country, language)
        artist_data = await run(ytm.get_artist, artist_id)
        if not artist_data:
            raise HTTPException(404, "Artist not found")
        artist_name = artist_data.get("name", "")
        channel_id  = artist_data.get("channelId", artist_id)
        album_entries: list = []
        for section_key in ("albums", "singles"):
            section = artist_data.get(section_key, {})
            if not isinstance(section, dict):
                continue
            params = section.get("params")
            if params and channel_id:
                try:
                    more = await run(ytm.get_artist_albums, channel_id, params)
                    album_entries.extend(
                        more if isinstance(more, list) else
                        (more.get("results") or more.get("items") or [])
                    )
                except Exception:
                    album_entries.extend(section.get("results") or section.get("items") or [])
            else:
                album_entries.extend(section.get("results") or section.get("items") or [])

        async def _album_detail(entry: dict) -> dict:
            bid    = entry.get("browseId")
            thumbs = best_thumbnails_list(entry.get("thumbnails") or [])
            base   = {
                "browseId": bid or "", "title": entry.get("title", ""),
                "year":     entry.get("year", ""), "type": entry.get("type", "Album"),
                "thumbnails": thumbs, "thumbnail": thumbs[0]["url"] if thumbs else "",
                "tracks": [],
            }
            if not bid:
                return base
            try:
                d = await run(ytm.get_album, bid)
                base["tracks"] = [norm_track(t) for t in (d.get("tracks") or []) if t.get("videoId")]
            except Exception:
                pass
            return base

        albums_out: list = []
        for i in range(0, len(album_entries), 5):
            batch   = album_entries[i:i+5]
            results = await asyncio.gather(*[_album_detail(e) for e in batch])
            albums_out.extend(results)

        result = {
            "artistId":    artist_id,
            "name":        artist_name,
            "albums":      albums_out,
            "totalAlbums": len(albums_out),
            "totalTracks": sum(len(a["tracks"]) for a in albums_out),
        }
        _cache_long.set(cache_key, result)
        response.headers.update(_cc(600))
        return result
    except HTTPException:
        raise
    except Exception as e:
        log.error("artist_albums %s: %s", artist_id, e)
        raise HTTPException(500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
#  Album
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/album/{album_id}", tags=["album"])
@limiter.limit("30/minute")
async def get_album(
    request:  Request,
    response: Response,
    album_id: str,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
):
    cache_key = f"album:{album_id}:{country}:{language}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(3_600))
        return cached
    try:
        data = await run(get_ytm(country, language).get_album, album_id)
        if not data:
            raise HTTPException(404, "Album not found")
        data["thumbnails"] = best_thumbnails_list(data.get("thumbnails") or [])
        data["thumbnail"]  = data["thumbnails"][0]["url"] if data["thumbnails"] else ""
        data["tracks"]     = [norm_track(t) for t in (data.get("tracks") or [])]
        _cache_long.set(cache_key, data)
        response.headers.update(_cc(3_600))
        return data
    except HTTPException:
        raise
    except Exception as e:
        log.error("get_album %s: %s", album_id, e)
        raise HTTPException(500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
#  Playlist
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/playlist/{playlist_id}", tags=["playlist"])
@limiter.limit("30/minute")
async def get_playlist(
    request:      Request,
    response:     Response,
    playlist_id:  str,
    limit:        int  = Query(100, ge=1, le=500),
    related:      bool = False,
    suggestions_limit: int = 0,
    country:      str = Query("ZZ"),
    language:     str = Query("en"),
):
    clean_id  = playlist_id[2:] if playlist_id.startswith("VL") else playlist_id
    cache_key = f"playlist:{clean_id}:{limit}:{country}:{language}"
    cached    = _cache_medium.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(300))
        return cached
    try:
        data = await run(
            get_ytm(country, language).get_playlist,
            clean_id, limit, related, suggestions_limit,
        )
        if not data:
            raise HTTPException(404, "Playlist not found")
        data["tracks"]     = [norm_track(t) for t in (data.get("tracks") or []) if isinstance(t, dict)]
        data["thumbnails"] = best_thumbnails_list(data.get("thumbnails") or [])
        data["thumbnail"]  = data["thumbnails"][0]["url"] if data["thumbnails"] else ""
        _cache_medium.set(cache_key, data)
        response.headers.update(_cc(300))
        return data
    except HTTPException:
        raise
    except Exception as e:
        log.error("get_playlist %s: %s", playlist_id, e)
        raise HTTPException(500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
#  Podcast
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/podcast/{podcast_id}", tags=["podcast"])
@limiter.limit("20/minute")
async def get_podcast(
    request:    Request,
    response:   Response,
    podcast_id: str,
    limit:      int = Query(50, ge=1, le=200),
    country:    str = Query("ZZ"),
    language:   str = Query("en"),
):
    clean_id = (podcast_id[2:]
                if podcast_id.startswith("VL") and not podcast_id.startswith("VLM")
                else podcast_id)
    cache_key = f"podcast:{clean_id}:{limit}:{country}:{language}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(1_800))
        return cached

    ytm          = get_ytm(country, language)
    episodes_raw: list = []
    meta: dict         = {}

    for fn, *fn_args in [
        (ytm.get_podcast,  clean_id),
        (ytm.get_playlist, clean_id, limit),
        (ytm.get_podcast,  podcast_id),
        (ytm.get_playlist, podcast_id, limit),
    ]:
        if episodes_raw:
            break
        try:
            data = await run(fn, *fn_args)
            if isinstance(data, dict) and data:
                if not meta:
                    meta = data
                episodes_raw = data.get("episodes") or data.get("tracks") or []
        except Exception:
            pass

    if not meta and not episodes_raw:
        raise HTTPException(404, "Podcast not found")

    def _norm_ep(ep: dict) -> dict:
        raw_t = ep.get("thumbnails") or ep.get("thumbnail") or []
        if isinstance(raw_t, str):
            raw_t = [{"url": raw_t, "width": 0}]
        thumbs = best_thumbnails_list(raw_t)
        dur    = ep.get("duration") or ep.get("durationSeconds") or ""
        if isinstance(dur, int) and dur > 0:
            m, s = divmod(dur, 60)
            h, m = divmod(m, 60)
            dur  = f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"
        artists = ep.get("artists") or ep.get("author") or ""
        if isinstance(artists, list):
            artists = ", ".join(
                a.get("name", "") if isinstance(a, dict) else str(a) for a in artists
            )
        return {
            "videoId":    ep.get("videoId", "") or ep.get("id", ""),
            "title":      ep.get("title", ""),
            "author":     artists,
            "duration":   dur,
            "date":       ep.get("date", "") or ep.get("publishedAt", ""),
            "thumbnails": thumbs,
            "thumbnail":  thumbs[0]["url"] if thumbs else "",
            "description":ep.get("description", ""),
        }

    thumbs_p = best_thumbnails_list(meta.get("thumbnails") or [])
    result   = {
        "podcastId":  clean_id,
        "title":      meta.get("title", ""),
        "author":     meta.get("author", ""),
        "description":meta.get("description", ""),
        "thumbnails": thumbs_p,
        "thumbnail":  thumbs_p[0]["url"] if thumbs_p else "",
        "episodes":   [_norm_ep(ep) for ep in episodes_raw[:limit] if isinstance(ep, dict)],
        "count":      min(len(episodes_raw), limit),
    }
    _cache_long.set(cache_key, result, ttl=1_800)
    response.headers.update(_cc(1_800))
    return result

# ─────────────────────────────────────────────────────────────────────────────
#  Watch playlist / lyrics
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/watch/{video_id}", tags=["song"])
@limiter.limit("30/minute")
async def get_watch_playlist(
    request:   Request,
    response:  Response,
    video_id:  str,
    limit:     int = Query(25, ge=1, le=50),
    country:   str = Query("ZZ"),
    language:  str = Query("en"),
):
    cache_key = f"watch:{video_id}:{limit}:{country}:{language}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(1_800))
        return cached
    try:
        raw        = await run(get_ytm(country, language).get_watch_playlist, video_id, None, limit)
        tracks_raw = raw.get("tracks") or []
        tracks     = [norm_track(t) for t in tracks_raw if isinstance(t, dict) and t.get("videoId")]
        result     = {
            "videoId":  video_id,
            "tracks":   tracks,
            "count":    len(tracks),
            "lyrics":   raw.get("lyrics") or raw.get("lyricsId") or "",
            "related":  raw.get("related") or "",
        }
        _cache_long.set(cache_key, result, ttl=1_800)
        response.headers.update(_cc(1_800))
        return result
    except Exception as e:
        log.error("watch %s: %s", video_id, e)
        raise HTTPException(500, detail=str(e))


@app.get("/lyrics/{browse_id}", tags=["song"])
@limiter.limit("30/minute")
async def get_lyrics(
    request:   Request,
    response:  Response,
    browse_id: str,
    country:   str = Query("ZZ"),
    language:  str = Query("en"),
):
    cache_key = f"lyrics:{browse_id}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(86_400))
        return cached
    try:
        data = await run(get_ytm(country, language).get_lyrics, browse_id)
        _cache_long.set(cache_key, data or {})
        response.headers.update(_cc(86_400))
        return data or {}
    except Exception as e:
        log.warning("get_lyrics %s: %s", browse_id, e)
        raise HTTPException(500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
#  Genres / featured playlists
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/genres", tags=["discovery"])
@limiter.limit("30/minute")
async def get_genres(
    request:  Request,
    response: Response,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
    limit:    int = Query(50, ge=1, le=200),
):
    cache_key = f"genres:{country}:{language}:{limit}"
    cached    = _cache_medium.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(600))
        return cached

    ytm_task    = run(get_ytm(country, language).get_charts, country)
    deezer_task = _fetch_deezer_editorial_playlists(20)

    ytm_charts_raw, deezer_playlists = await asyncio.gather(
        ytm_task, deezer_task, return_exceptions=True
    )

    playlists: list = []
    if not isinstance(ytm_charts_raw, Exception) and isinstance(ytm_charts_raw, dict):
        for sk in ("playlists", "genres", "moods", "trending"):
            section = ytm_charts_raw.get(sk, {})
            items   = section if isinstance(section, list) else (
                section.get("items") or section.get("results") or []
            )
            for item in items:
                if not isinstance(item, dict):
                    continue
                bid = item.get("browseId") or item.get("playlistId")
                if bid and item.get("title"):
                    thumbs = best_thumbnails_list(item.get("thumbnails") or [])
                    playlists.append({
                        "browseId":   bid,
                        "title":      item.get("title", ""),
                        "subtitle":   item.get("subtitle", "") or item.get("author", ""),
                        "thumbnails": thumbs,
                        "thumbnail":  thumbs[0]["url"] if thumbs else "",
                        "source":     "ytm",
                    })

    try:
        cats         = await run(get_ytm(country, language).get_mood_categories)
        first_params = None
        if isinstance(cats, dict):
            for items in cats.values():
                if isinstance(items, list) and items:
                    p = items[0].get("params")
                    if p:
                        first_params = p
                        break
        elif isinstance(cats, list) and cats:
            first_params = cats[0].get("params")
        if first_params:
            mood_raw = await run(get_ytm(country, language).get_mood_playlists, first_params)
            for section in (mood_raw or []):
                if not isinstance(section, dict):
                    continue
                for item in (section.get("contents") or section.get("playlists") or []):
                    if not isinstance(item, dict):
                        continue
                    bid = item.get("playlistId") or item.get("browseId")
                    if bid:
                        thumbs = best_thumbnails_list(item.get("thumbnails") or [])
                        playlists.append({
                            "browseId":   bid,
                            "title":      item.get("title", ""),
                            "subtitle":   item.get("subtitle", ""),
                            "thumbnails": thumbs,
                            "thumbnail":  thumbs[0]["url"] if thumbs else "",
                            "source":     "ytm",
                        })
    except Exception:
        pass

    if not isinstance(deezer_playlists, Exception):
        playlists.extend(deezer_playlists)

    seen, deduped = set(), []
    for p in playlists:
        bid = p.get("browseId", "")
        if bid and bid not in seen:
            seen.add(bid)
            deduped.append(p)

    result = deduped[:limit]
    _cache_medium.set(cache_key, result)
    response.headers.update(_cc(600))
    return result

# ─────────────────────────────────────────────────────────────────────────────
#  Mood categories & playlists
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/mood_categories", tags=["discovery"])
@limiter.limit("30/minute")
async def get_mood_categories(
    request:  Request,
    response: Response,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
):
    cache_key = f"mood_categories:{country}:{language}"
    cached    = _cache_medium.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(600))
        return cached
    try:
        ytm_task    = run(get_ytm(country, language).get_mood_categories)
        deezer_task = _fetch_deezer_genres()
        ytm_raw, deezer_genres = await asyncio.gather(ytm_task, deezer_task, return_exceptions=True)

        categories: list = []
        if not isinstance(ytm_raw, Exception):
            categories = _flatten_mood_categories(ytm_raw)

        existing_titles = {c["title"].lower() for c in categories}
        if not isinstance(deezer_genres, Exception):
            for g in deezer_genres:
                if g.get("title", "").lower() not in existing_titles:
                    g["section"] = "Genres"
                    categories.append(g)
                    existing_titles.add(g["title"].lower())

        _cache_medium.set(cache_key, categories)
        response.headers.update(_cc(600))
        return categories
    except Exception as e:
        log.error("mood_categories country=%s: %s", country, e)
        raise HTTPException(500, detail=str(e))


@app.get("/mood_playlists/{params}", tags=["discovery"])
@limiter.limit("30/minute")
async def get_mood_playlists(
    request:  Request,
    response: Response,
    params:   str,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
):
    cache_key = f"mood_playlists:{params}:{country}:{language}"
    cached    = _cache_medium.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(600))
        return cached
    try:
        raw       = await run(get_ytm(country, language).get_mood_playlists, params)
        playlists: list = []
        for section in (raw if isinstance(raw, list) else []):
            if not isinstance(section, dict):
                continue
            contents = section.get("contents") or section.get("playlists") or []
            if isinstance(contents, list):
                for item in contents:
                    if not isinstance(item, dict):
                        continue
                    thumbs = best_thumbnails_list(item.get("thumbnails") or [])
                    playlists.append({
                        "browseId":   item.get("playlistId") or item.get("browseId", ""),
                        "title":      item.get("title", ""),
                        "subtitle":   item.get("subtitle", ""),
                        "thumbnails": thumbs,
                        "thumbnail":  thumbs[0]["url"] if thumbs else "",
                    })
            else:
                thumbs = best_thumbnails_list(section.get("thumbnails") or [])
                playlists.append({
                    "browseId":   section.get("playlistId") or section.get("browseId", ""),
                    "title":      section.get("title", ""),
                    "subtitle":   section.get("subtitle", ""),
                    "thumbnails": thumbs,
                    "thumbnail":  thumbs[0]["url"] if thumbs else "",
                })
        _cache_medium.set(cache_key, playlists)
        response.headers.update(_cc(600))
        return playlists
    except Exception as e:
        log.error("mood_playlists params=%s: %s", params, e)
        raise HTTPException(500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
#  Explore
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/explore", tags=["discovery"])
@limiter.limit("30/minute")
async def get_explore(
    request:  Request,
    response: Response,
    country:  str = Query("ZZ"),
    language: str = Query("en"),
):
    cache_key = f"explore:{country}:{language}"
    cached    = _cache_medium.get(cache_key)
    if cached is not None:
        response.headers.update(_cc(600))
        return cached
    try:
        data = await run(get_ytm(country, language).get_explore)
        _cache_medium.set(cache_key, data)
        response.headers.update(_cc(600))
        return data
    except Exception as e:
        log.error("get_explore country=%s: %s", country, e)
        raise HTTPException(500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
#  User
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/user/{channel_id}", tags=["user"])
async def get_user(
    response:   Response,
    channel_id: str,
    country:    str = Query("ZZ"),
    language:   str = Query("en"),
):
    cache_key = f"user:{channel_id}:{country}:{language}"
    cached    = _cache_long.get(cache_key)
    if cached is not None:
        return cached
    try:
        data = await run(get_ytm(country, language).get_user, channel_id)
        _cache_long.set(cache_key, data)
        return data
    except Exception as e:
        log.error("get_user %s: %s", channel_id, e)
        raise HTTPException(500, detail=str(e))


@app.get("/user_playlists/{channel_id}", tags=["user"])
async def get_user_playlists(
    response:   Response,
    channel_id: str,
    params:     Optional[str] = None,
    country:    str = Query("ZZ"),
    language:   str = Query("en"),
):
    try:
        data = await run(get_ytm(country, language).get_user_playlists, channel_id, params)
        return data
    except Exception as e:
        log.error("user_playlists %s: %s", channel_id, e)
        raise HTTPException(500, detail=str(e))
