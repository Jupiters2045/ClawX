#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║   MARKETPLACE INTELLIGENCE ENGINE — Core Abstraction Layer       ║
║   v4.0 · Adapter Pattern · Self-Monitoring · Multi-Platform      ║
║                                                                  ║
║   Architecture:                                                  ║
║     MarketplaceConnector (Abstract)                              ║
║       ├── ShopeeConnector                                        ║
║       ├── TokopediaConnector  (stub — ready to implement)        ║
║       └── TikTokConnector     (stub — ready to implement)        ║
║                                                                  ║
║   Philosophy:                                                    ║
║     "Saat satu endpoint mati, engine tetap hidup."               ║
║     Undocumented API = moving target. Kita yang bergerak lebih   ║
║     cepat dari mereka yang mengubahnya.                          ║
╚══════════════════════════════════════════════════════════════════╝

INSTALL:
  pip install httpx[http2] rich typer anthropic python-dotenv

USAGE:
  python marketplace_core.py health-check
  python marketplace_core.py health-check --marketplace shopee --watch
  python marketplace_core.py search "tumbler" --marketplace shopee --ai
  python marketplace_core.py search "tumbler" --marketplace all
  python marketplace_core.py schema-diff --since 7
  python marketplace_core.py dashboard
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import sqlite3
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import httpx
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.live import Live
from rich.progress import (
    BarColumn, Progress, SpinnerColumn,
    TextColumn, TimeElapsedColumn,
)
import typer

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

console = Console()
app = typer.Typer(
    help="🌐 Multi-Marketplace Intelligence Engine",
    rich_markup_mode="rich",
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA CONTRACTS — Platform-agnostic normalized schema
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class NormalizedProduct:
    """
    Single source of truth — semua platform dipaksa ke shape ini.
    Tidak peduli Shopee atau Tokopedia atau TikTok,
    output selalu sama. Ini yang membuat engine tetap hidup
    meski backend berubah.
    """
    platform:          str
    item_id:           str
    shop_id:           str
    name:              str
    price_min:         float
    price_max:         float
    sold:              int
    rating_star:       float
    rating_count:      int
    stock:             int
    liked_count:       int
    view_count:        int
    shop_name:         str
    shop_location:     str
    is_official:       bool
    url:               str
    image_url:         str
    # Computed — engine fills these
    revenue_estimate:  float = field(default=0.0)
    opportunity_score: float = field(default=0.0)
    crawled_at:        str   = field(default_factory=lambda: datetime.now().isoformat())

    def __post_init__(self):
        self.revenue_estimate = self.sold * self.price_min


@dataclass
class EndpointHealthResult:
    """Result dari satu endpoint health check."""
    platform:      str
    endpoint_name: str
    endpoint_url:  str
    status:        str          # OK | SCHEMA_CHANGED | TIMEOUT | HTTP_ERROR | DEAD
    latency_ms:    float
    schema_hash:   str
    schema_sample: dict
    error:         Optional[str] = None
    checked_at:    str = field(default_factory=lambda: datetime.now().isoformat())

    @property
    def is_healthy(self) -> bool:
        return self.status == "OK"

    @property
    def status_icon(self) -> str:
        icons = {
            "OK":             "✅",
            "SCHEMA_CHANGED": "⚠️ ",
            "TIMEOUT":        "⏱️ ",
            "HTTP_ERROR":     "🔴",
            "DEAD":           "💀",
        }
        return icons.get(self.status, "❓")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ABSTRACT BASE — Kontrak yang harus dipenuhi semua connector
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MarketplaceConnector(ABC):
    """
    Seperti soket listrik — bentuknya standar,
    apapun yang dicolok harus ikuti bentuk itu.

    ShopeeConnector, TokopediaConnector, TikTokConnector
    semua harus implement method di bawah ini.
    Engine tidak tahu dan tidak peduli siapa yang jalan —
    engine hanya panggil kontrak ini.
    """

    platform_name: str = "abstract"
    base_url:      str = ""

    # Setiap subclass mendefinisikan endpoint mereka sendiri
    # Format: {"endpoint_name": "/path/to/endpoint"}
    ENDPOINTS: dict[str, str] = {}

    # Minimal keys yang HARUS ada di response
    # Kalau keys ini hilang → schema changed alert
    REQUIRED_SCHEMA_KEYS: dict[str, list[str]] = {}

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    @abstractmethod
    def build_headers(self) -> dict:
        """Return platform-specific request headers."""
        ...

    @abstractmethod
    async def search(
        self,
        keyword: str,
        limit: int = 30,
        sort_by: str = "sales",
    ) -> list[NormalizedProduct]:
        """
        Search products by keyword.
        MUST return list[NormalizedProduct] — never platform-specific shape.
        """
        ...

    @abstractmethod
    async def get_product_detail(
        self,
        item_id: str,
        shop_id: str,
    ) -> Optional[NormalizedProduct]:
        """Fetch single product detail. Return None if not found."""
        ...

    @abstractmethod
    async def health_check_endpoints(
        self,
        client: httpx.AsyncClient,
    ) -> list[EndpointHealthResult]:
        """
        Test semua endpoint platform ini.
        Harus return list[EndpointHealthResult] dengan status yang akurat.
        """
        ...

    # ── Shared utility — semua subclass boleh pakai ──────

    async def _fetch(
        self,
        client: httpx.AsyncClient,
        path: str,
        params: dict,
        retries: int = 3,
    ) -> Optional[dict]:
        url = self.base_url + path
        for attempt in range(retries):
            try:
                await asyncio.sleep(random.uniform(0.3, 1.1))
                r = await client.get(
                    url, params=params,
                    headers=self.build_headers(),
                    timeout=12,
                )
                r.raise_for_status()
                return r.json()
            except httpx.TimeoutException:
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
            except (httpx.HTTPStatusError, json.JSONDecodeError):
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(1.5)
        return None

    @staticmethod
    def _schema_hash(data: dict) -> str:
        """Fingerprint dari top-level keys + tipe data — deteksi perubahan schema."""
        shape = {k: type(v).__name__ for k, v in data.items()}
        return hashlib.md5(json.dumps(shape, sort_keys=True).encode()).hexdigest()[:12]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SHOPEE CONNECTOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ShopeeConnector(MarketplaceConnector):

    platform_name = "shopee"
    base_url      = "https://shopee.co.id"

    ENDPOINTS = {
        "search":      "/api/v4/search/search_items",
        "item_detail": "/api/v4/item/get",
        "shop_detail": "/api/v4/shop/get_shop_detail",
        "shop_items":  "/api/v4/shop/get_shop_itemids",
        "ratings":     "/api/v4/item/get_ratings",
        "keyword_rec": "/api/v4/search/get_keyword_rec",
    }

    # Keys yang wajib ada — kalau hilang = schema changed
    REQUIRED_SCHEMA_KEYS = {
        "search":      ["items", "total_count"],
        "item_detail": ["item"],
        "shop_detail": ["data"],
        "ratings":     ["data"],
    }

    _USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    ]

    def build_headers(self) -> dict:
        return {
            "User-Agent":       random.choice(self._USER_AGENTS),
            "Accept":           "application/json",
            "Accept-Language":  "id-ID,id;q=0.9,en-US;q=0.8",
            "Accept-Encoding":  "gzip, deflate, br",
            "Referer":          "https://shopee.co.id/",
            "Origin":           "https://shopee.co.id",
            "X-Api-Source":     "pc",
            "X-Requested-With": "XMLHttpRequest",
            "sec-fetch-dest":   "empty",
            "sec-fetch-mode":   "cors",
            "sec-fetch-site":   "same-origin",
        }

    def _parse_raw(self, raw: dict) -> Optional[NormalizedProduct]:
        info   = raw.get("item_basic", raw)
        rating = info.get("item_rating", {})
        iid    = info.get("itemid", 0)
        sid    = info.get("shopid", 0)
        if not iid or not sid:
            return None
        price_min = info.get("price", 0) / 100000
        price_max = info.get("price_max", info.get("price", 0)) / 100000

        return NormalizedProduct(
            platform     = "shopee",
            item_id      = str(iid),
            shop_id      = str(sid),
            name         = info.get("name", "")[:120],
            price_min    = price_min,
            price_max    = price_max,
            sold         = info.get("sold", 0),
            rating_star  = rating.get("rating_star", 0.0),
            rating_count = sum(rating.get("rating_count", [0] * 6)),
            stock        = info.get("stock", 0),
            liked_count  = info.get("liked_count", 0),
            view_count   = info.get("view_count", 0),
            shop_name    = info.get("shop_name", ""),
            shop_location= info.get("shop_location", ""),
            is_official  = bool(info.get("is_official_shop", False)),
            url          = f"https://shopee.co.id/product/{sid}/{iid}",
            image_url    = f"https://cf.shopee.co.id/file/{info.get('image','')}",
        )

    async def search(
        self,
        keyword: str,
        limit: int = 30,
        sort_by: str = "sales",
    ) -> list[NormalizedProduct]:
        sort_map = {"sales": "sales", "price": "price", "rating": "rating", "new": "ctime"}
        results  = []
        limits   = httpx.Limits(max_connections=8, max_keepalive_connections=4)

        async with httpx.AsyncClient(http2=True, limits=limits, follow_redirects=True) as client:
            for offset in range(0, limit, 60):
                batch = min(60, limit - offset)
                params = {
                    "by":        sort_map.get(sort_by, "sales"),
                    "keyword":   keyword,
                    "limit":     batch,
                    "newest":    offset,
                    "order":     "desc",
                    "page_type": "search",
                    "scenario":  "PAGE_GLOBAL_SEARCH",
                    "version":   2,
                    "matchtype": 0,
                }
                try:
                    data = await self._fetch(client, self.ENDPOINTS["search"], params)
                except Exception:
                    break

                if not data:
                    break
                for raw in data.get("items", []):
                    p = self._parse_raw(raw)
                    if p:
                        results.append(p)
                if len(data.get("items", [])) < batch:
                    break

        return results

    async def get_product_detail(
        self,
        item_id: str,
        shop_id: str,
    ) -> Optional[NormalizedProduct]:
        async with httpx.AsyncClient(http2=True, follow_redirects=True) as client:
            try:
                data = await self._fetch(
                    client, self.ENDPOINTS["item_detail"],
                    {"itemid": int(item_id), "shopid": int(shop_id)},
                )
            except Exception:
                return None

        if not data or "item" not in data:
            return None
        return self._parse_raw({"item_basic": data["item"]})

    async def health_check_endpoints(
        self,
        client: httpx.AsyncClient,
    ) -> list[EndpointHealthResult]:
        """
        Test setiap endpoint dengan probe request minimal.
        Bandingkan schema hash dengan snapshot terakhir.
        """
        results = []

        probe_params = {
            "search": {
                "keyword": "test", "limit": 1, "newest": 0,
                "by": "sales", "order": "desc",
                "page_type": "search", "scenario": "PAGE_GLOBAL_SEARCH",
                "version": 2,
            },
            "item_detail": {"itemid": 1, "shopid": 1},
            "shop_detail":  {"shopid": 1},
            "ratings":      {"itemid": 1, "shopid": 1, "offset": 0, "limit": 1, "filter": 0, "type": 0},
        }

        for name, path in self.ENDPOINTS.items():
            if name not in probe_params:
                continue

            url     = self.base_url + path
            t_start = time.monotonic()
            status  = "OK"
            schema_hash   = ""
            schema_sample = {}
            error_msg     = None

            try:
                resp = await client.get(
                    url,
                    params   = probe_params[name],
                    headers  = self.build_headers(),
                    timeout  = 10,
                )
                latency_ms = (time.monotonic() - t_start) * 1000

                if resp.status_code >= 500:
                    status    = "DEAD"
                    error_msg = f"HTTP {resp.status_code}"
                elif resp.status_code >= 400:
                    status    = "HTTP_ERROR"
                    error_msg = f"HTTP {resp.status_code}"
                else:
                    try:
                        data          = resp.json()
                        schema_sample = {k: type(v).__name__ for k, v in data.items()}
                        schema_hash   = self._schema_hash(data)

                        # Check required keys
                        required = self.REQUIRED_SCHEMA_KEYS.get(name, [])
                        missing  = [k for k in required if k not in data]
                        if missing:
                            status    = "SCHEMA_CHANGED"
                            error_msg = f"Missing keys: {missing}"
                    except json.JSONDecodeError:
                        status    = "HTTP_ERROR"
                        error_msg = "Response is not JSON"

            except httpx.TimeoutException:
                latency_ms = (time.monotonic() - t_start) * 1000
                status     = "TIMEOUT"
                error_msg  = f"Timeout after {latency_ms:.0f}ms"
            except Exception as e:
                latency_ms = (time.monotonic() - t_start) * 1000
                status     = "DEAD"
                error_msg  = str(e)[:100]

            results.append(EndpointHealthResult(
                platform      = self.platform_name,
                endpoint_name = name,
                endpoint_url  = url,
                status        = status,
                latency_ms    = round(latency_ms, 1),
                schema_hash   = schema_hash,
                schema_sample = schema_sample,
                error         = error_msg,
            ))

        return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOKOPEDIA CONNECTOR (Stub — arsitektur siap, impl menyusul)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TokopediaConnector(MarketplaceConnector):
    """
    Tokopedia menggunakan GraphQL — berbeda total dari Shopee REST.
    Tapi engine tidak peduli: output tetap list[NormalizedProduct].
    Ini kekuatan adapter pattern.
    """
    platform_name = "tokopedia"
    base_url      = "https://gql.tokopedia.com"

    ENDPOINTS = {
        "search": "/graphql/SearchProductQueryV4",
        "pdp":    "/graphql/PDPGetLayoutQuery",
    }

    REQUIRED_SCHEMA_KEYS = {
        "search": ["data"],
    }

    def build_headers(self) -> dict:
        return {
            "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Content-Type": "application/json",
            "Referer":      "https://www.tokopedia.com/",
            "Origin":       "https://www.tokopedia.com",
            "X-Source":     "tokopedia-lite",
            "X-Tkpd-Lite-Service": "zeus",
        }

    async def search(self, keyword: str, limit: int = 30, sort_by: str = "sales") -> list[NormalizedProduct]:
        # GraphQL payload untuk Tokopedia search
        # Stub: return empty sampai diimplementasi penuh
        # Pattern GraphQL Tokopedia sudah didokumentasi di berbagai repo publik
        console.print("[dim yellow]  Tokopedia connector: stub — implementasi GraphQL menyusul[/dim yellow]")
        return []

    async def get_product_detail(self, item_id: str, shop_id: str) -> Optional[NormalizedProduct]:
        return None

    async def health_check_endpoints(self, client: httpx.AsyncClient) -> list[EndpointHealthResult]:
        """Health check Tokopedia GraphQL endpoint."""
        t_start = time.monotonic()
        url     = self.base_url + self.ENDPOINTS["search"]

        # Minimal GraphQL probe
        payload = json.dumps([{"operationName": "SearchProductQueryV4",
                               "variables": {"params": "q=test&rows=1"},
                               "query": "query SearchProductQueryV4($params: String) { ace_search_product_v4(params: $params) { status } }"}])
        try:
            resp = await client.post(
                url, content=payload,
                headers=self.build_headers(), timeout=10,
            )
            latency_ms = (time.monotonic() - t_start) * 1000
            status = "OK" if resp.status_code < 400 else "HTTP_ERROR"
        except httpx.TimeoutException:
            latency_ms = (time.monotonic() - t_start) * 1000
            status     = "TIMEOUT"
        except Exception as e:
            latency_ms = (time.monotonic() - t_start) * 1000
            status     = "DEAD"

        return [EndpointHealthResult(
            platform      = "tokopedia",
            endpoint_name = "graphql_search",
            endpoint_url  = url,
            status        = status,
            latency_ms    = round(latency_ms, 1),
            schema_hash   = "",
            schema_sample = {},
        )]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TIKTOK SHOP CONNECTOR (Stub)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TikTokShopConnector(MarketplaceConnector):
    """
    TikTok Shop Indonesia menggunakan endpoint berbeda dari TikTok global.
    Semua data affiliate dan produk tersedia via public search.
    """
    platform_name = "tiktok_shop"
    base_url      = "https://affiliate.tiktok.com"

    ENDPOINTS = {
        "product_search": "/api/v1/product/search",
    }

    REQUIRED_SCHEMA_KEYS = {
        "product_search": ["data", "status_code"],
    }

    def build_headers(self) -> dict:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer":    "https://affiliate.tiktok.com/",
            "Accept":     "application/json, text/plain, */*",
        }

    async def search(self, keyword: str, limit: int = 30, sort_by: str = "sales") -> list[NormalizedProduct]:
        console.print("[dim yellow]  TikTok Shop connector: stub — endpoint mapping menyusul[/dim yellow]")
        return []

    async def get_product_detail(self, item_id: str, shop_id: str) -> Optional[NormalizedProduct]:
        return None

    async def health_check_endpoints(self, client: httpx.AsyncClient) -> list[EndpointHealthResult]:
        t_start = time.monotonic()
        url     = self.base_url + self.ENDPOINTS["product_search"]
        try:
            resp       = await client.get(url, headers=self.build_headers(), timeout=8)
            latency_ms = (time.monotonic() - t_start) * 1000
            status     = "OK" if resp.status_code < 500 else "HTTP_ERROR"
        except Exception:
            latency_ms = (time.monotonic() - t_start) * 1000
            status     = "DEAD"

        return [EndpointHealthResult(
            platform      = "tiktok_shop",
            endpoint_name = "product_search",
            endpoint_url  = url,
            status        = status,
            latency_ms    = round(latency_ms, 1),
            schema_hash   = "",
            schema_sample = {},
        )]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONNECTOR REGISTRY — Engine tidak hardcode platform apapun
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

REGISTRY: dict[str, type[MarketplaceConnector]] = {
    "shopee":      ShopeeConnector,
    "tokopedia":   TokopediaConnector,
    "tiktok_shop": TikTokShopConnector,
}

def get_connector(name: str) -> MarketplaceConnector:
    cls = REGISTRY.get(name.lower())
    if not cls:
        raise ValueError(f"Unknown marketplace: {name}. Available: {list(REGISTRY.keys())}")
    return cls()

def get_all_connectors() -> list[MarketplaceConnector]:
    return [cls() for cls in REGISTRY.values()]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SCORING ENGINE — Platform-agnostic, works on NormalizedProduct
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def compute_scores(products: list[NormalizedProduct]) -> list[NormalizedProduct]:
    if not products:
        return products

    max_rev     = max(p.revenue_estimate for p in products) or 1
    num_sellers = len(set(p.shop_id for p in products))

    for p in products:
        rev_score    = (p.revenue_estimate / max_rev) * 40
        comp_score   = max(0.0, 25 - (num_sellers / len(products)) * 25)
        rating_score = (p.rating_star / 5.0) * 20 if p.rating_star > 0 else 0
        like_ratio   = min((p.liked_count / (p.sold or 1)), 1.0)
        growth_score = like_ratio * 15
        p.opportunity_score = round(rev_score + comp_score + rating_score + growth_score, 1)

    return sorted(products, key=lambda x: x.opportunity_score, reverse=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HEALTH CHECK DATABASE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

HEALTH_DB = "health_monitor.db"

def init_health_db() -> sqlite3.Connection:
    conn = sqlite3.connect(HEALTH_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS endpoint_health (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            platform       TEXT,
            endpoint_name  TEXT,
            endpoint_url   TEXT,
            status         TEXT,
            latency_ms     REAL,
            schema_hash    TEXT,
            schema_sample  TEXT,
            error          TEXT,
            checked_at     TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_snapshots (
            platform       TEXT,
            endpoint_name  TEXT,
            schema_hash    TEXT,
            schema_sample  TEXT,
            first_seen     TEXT,
            last_seen      TEXT,
            PRIMARY KEY (platform, endpoint_name, schema_hash)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            platform    TEXT,
            endpoint    TEXT,
            alert_type  TEXT,
            message     TEXT,
            old_hash    TEXT,
            new_hash    TEXT,
            alerted_at  TEXT
        )
    """)
    conn.commit()
    return conn


def save_health_result(conn: sqlite3.Connection, r: EndpointHealthResult):
    conn.execute("""
        INSERT INTO endpoint_health
        (platform, endpoint_name, endpoint_url, status, latency_ms,
         schema_hash, schema_sample, error, checked_at)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        r.platform, r.endpoint_name, r.endpoint_url,
        r.status, r.latency_ms, r.schema_hash,
        json.dumps(r.schema_sample), r.error, r.checked_at,
    ))
    conn.commit()


def detect_schema_change(
    conn: sqlite3.Connection,
    r: EndpointHealthResult,
) -> Optional[dict]:
    """
    Bandingkan schema hash sekarang dengan snapshot terakhir.
    Return dict berisi info perubahan jika ada, None jika tidak ada.
    """
    if not r.schema_hash:
        return None

    row = conn.execute("""
        SELECT schema_hash, schema_sample, first_seen
        FROM schema_snapshots
        WHERE platform = ? AND endpoint_name = ?
        ORDER BY first_seen DESC LIMIT 1
    """, (r.platform, r.endpoint_name)).fetchone()

    now = datetime.now().isoformat()

    if not row:
        # Pertama kali — simpan sebagai baseline
        conn.execute("""
            INSERT OR REPLACE INTO schema_snapshots
            (platform, endpoint_name, schema_hash, schema_sample, first_seen, last_seen)
            VALUES (?,?,?,?,?,?)
        """, (r.platform, r.endpoint_name, r.schema_hash,
              json.dumps(r.schema_sample), now, now))
        conn.commit()
        return None  # No change (it's new)

    prev_hash   = row[0]
    prev_sample = json.loads(row[1])
    first_seen  = row[2]

    if prev_hash != r.schema_hash:
        # Schema berubah! Hitung diff
        prev_keys  = set(prev_sample.keys())
        curr_keys  = set(r.schema_sample.keys())
        added      = curr_keys - prev_keys
        removed    = prev_keys - curr_keys
        type_changed = {
            k for k in prev_keys & curr_keys
            if prev_sample[k] != r.schema_sample.get(k)
        }

        # Update snapshot
        conn.execute("""
            INSERT OR REPLACE INTO schema_snapshots
            (platform, endpoint_name, schema_hash, schema_sample, first_seen, last_seen)
            VALUES (?,?,?,?,?,?)
        """, (r.platform, r.endpoint_name, r.schema_hash,
              json.dumps(r.schema_sample), now, now))

        # Log alert
        change_info = {
            "platform":     r.platform,
            "endpoint":     r.endpoint_name,
            "old_hash":     prev_hash,
            "new_hash":     r.schema_hash,
            "keys_added":   list(added),
            "keys_removed": list(removed),
            "type_changed": list(type_changed),
            "baseline_since": first_seen,
        }
        conn.execute("""
            INSERT INTO alerts (platform, endpoint, alert_type, message, old_hash, new_hash, alerted_at)
            VALUES (?,?,?,?,?,?,?)
        """, (
            r.platform, r.endpoint_name, "SCHEMA_CHANGED",
            json.dumps(change_info), prev_hash, r.schema_hash, now,
        ))
        conn.commit()
        return change_info

    # Update last_seen
    conn.execute("""
        UPDATE schema_snapshots SET last_seen = ?
        WHERE platform = ? AND endpoint_name = ?
    """, (now, r.platform, r.endpoint_name))
    conn.commit()
    return None


def get_recent_alerts(conn: sqlite3.Connection, days: int = 7) -> list[dict]:
    since = (datetime.now() - timedelta(days=days)).isoformat()
    rows  = conn.execute("""
        SELECT platform, endpoint, alert_type, message, alerted_at
        FROM alerts WHERE alerted_at > ?
        ORDER BY alerted_at DESC
    """, (since,)).fetchall()
    return [
        {"platform": r[0], "endpoint": r[1], "type": r[2],
         "message": json.loads(r[3]), "at": r[4]}
        for r in rows
    ]


def get_uptime_stats(conn: sqlite3.Connection, hours: int = 24) -> dict:
    """Hitung uptime % tiap endpoint dalam N jam terakhir."""
    since = (datetime.now() - timedelta(hours=hours)).isoformat()
    rows  = conn.execute("""
        SELECT platform, endpoint_name, status, COUNT(*) as cnt
        FROM endpoint_health
        WHERE checked_at > ?
        GROUP BY platform, endpoint_name, status
    """, (since,)).fetchall()

    stats: dict[str, dict] = {}
    for platform, endpoint, status, cnt in rows:
        key = f"{platform}/{endpoint}"
        if key not in stats:
            stats[key] = {"total": 0, "ok": 0, "platform": platform, "endpoint": endpoint}
        stats[key]["total"] += cnt
        if status == "OK":
            stats[key]["ok"] += cnt

    for key in stats:
        t = stats[key]["total"]
        stats[key]["uptime_pct"] = round(stats[key]["ok"] / t * 100, 1) if t else 0.0
        stats[key]["avg_latency"] = 0.0  # filled below

    # Avg latency
    lat_rows = conn.execute("""
        SELECT platform, endpoint_name, AVG(latency_ms)
        FROM endpoint_health
        WHERE checked_at > ? AND status = 'OK'
        GROUP BY platform, endpoint_name
    """, (since,)).fetchall()
    for platform, endpoint, avg_lat in lat_rows:
        key = f"{platform}/{endpoint}"
        if key in stats:
            stats[key]["avg_latency"] = round(avg_lat, 1)

    return stats


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TELEGRAM + AI ALERT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def send_telegram(text: str):
    token   = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
                timeout=10,
            )
        except Exception:
            pass


async def alert_schema_change(change: dict, result: EndpointHealthResult):
    """Kirim alert schema change ke Telegram + generate AI diagnosis."""
    lines = [
        f"⚠️ *SCHEMA CHANGED DETECTED*",
        f"Platform: `{change['platform']}`",
        f"Endpoint: `{change['endpoint']}`",
        f"Hash: `{change['old_hash'][:8]}` → `{change['new_hash'][:8]}`",
    ]
    if change.get("keys_added"):
        lines.append(f"✅ Keys ADDED: `{', '.join(change['keys_added'])}`")
    if change.get("keys_removed"):
        lines.append(f"❌ Keys REMOVED: `{', '.join(change['keys_removed'])}`")
    if change.get("type_changed"):
        lines.append(f"🔄 Types CHANGED: `{', '.join(change['type_changed'])}`")
    lines.append(f"Baseline since: `{change['baseline_since'][:10]}`")
    lines.append(f"\n_Auto-detected by Health Monitor_")

    msg = "\n".join(lines)
    await send_telegram(msg)

    # Claude AI diagnosis
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return

    try:
        import anthropic
        ac = anthropic.Anthropic(api_key=api_key)

        prompt = (
            f"Kamu engineer yang expert di reverse-engineering marketplace APIs Indonesia.\n\n"
            f"Terdeteksi perubahan schema pada endpoint:\n"
            f"Platform: {change['platform']}\n"
            f"Endpoint: {change['endpoint']}\n"
            f"Keys hilang: {change.get('keys_removed', [])}\n"
            f"Keys baru: {change.get('keys_added', [])}\n"
            f"Tipe berubah: {change.get('type_changed', [])}\n\n"
            f"Berikan dalam format:\n"
            f"1. DAMPAK (apakah parsing akan break? field mana?)\n"
            f"2. KEMUNGKINAN PENYEBAB (update API, A/B test, versioning?)\n"
            f"3. FIX CEPAT (patch parsing code dalam 1-2 langkah)\n"
            f"Bahasa Indonesia. Padat dan teknikal."
        )
        msg_obj = ac.messages.create(
            model      = "claude-sonnet-4-6",
            max_tokens = 350,
            messages   = [{"role": "user", "content": prompt}],
        )
        diagnosis = msg_obj.content[0].text
        await send_telegram(f"🤖 *AI Diagnosis:*\n\n{diagnosis[:900]}")
    except Exception:
        pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DISPLAY HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def display_health_results(
    results: list[EndpointHealthResult],
    changes: list[Optional[dict]],
):
    table = Table(
        title="[bold cyan]🩺 Endpoint Health Check[/bold cyan]",
        box=box.ROUNDED,
        border_style="dim",
        expand=True,
    )
    table.add_column("Platform",    style="cyan",  width=12)
    table.add_column("Endpoint",    style="white", width=18)
    table.add_column("Status",      width=16,      justify="center")
    table.add_column("Latency",     width=10,      justify="right", style="green")
    table.add_column("Schema Hash", width=14,      style="dim")
    table.add_column("Schema Δ",    width=10,      justify="center")

    for r, change in zip(results, changes):
        status_text = f"{r.status_icon} {r.status}"
        status_style = {
            "OK":             "green",
            "SCHEMA_CHANGED": "yellow",
            "TIMEOUT":        "yellow",
            "HTTP_ERROR":     "red",
            "DEAD":           "bold red",
        }.get(r.status, "white")

        change_str = ""
        if change:
            removed = change.get("keys_removed", [])
            added   = change.get("keys_added", [])
            change_str = (
                f"[red]-{','.join(removed[:2])}[/red] " if removed else ""
            ) + (
                f"[green]+{','.join(added[:2])}[/green]" if added else ""
            )
            if not change_str:
                change_str = "[yellow]type Δ[/yellow]"
        else:
            change_str = "[dim]–[/dim]"

        error_suffix = f"\n  [dim red]{r.error}[/dim red]" if r.error else ""

        table.add_row(
            r.platform,
            r.endpoint_name,
            f"[{status_style}]{status_text}[/{status_style}]{error_suffix}",
            f"{r.latency_ms:.0f}ms",
            r.schema_hash or "[dim]–[/dim]",
            change_str,
        )

    console.print(table)


def display_products(products: list[NormalizedProduct], title: str = "Results"):
    table = Table(
        title=f"[bold cyan]{title}[/bold cyan]",
        box=box.ROUNDED,
        border_style="dim",
        expand=True,
    )
    table.add_column("Platform", style="cyan",  width=10)
    table.add_column("#",        style="dim",   width=3, justify="right")
    table.add_column("Produk",   style="white", max_width=33)
    table.add_column("Harga",    style="green", width=16, justify="right")
    table.add_column("Terjual",  style="cyan",  width=9,  justify="right")
    table.add_column("Rev Est",  style="yellow bold", width=12, justify="right")
    table.add_column("⭐",       width=5,  justify="center")
    table.add_column("Skor",     style="magenta bold", width=7, justify="center")

    for i, p in enumerate(products[:25], 1):
        rev = p.revenue_estimate
        rev_str = (f"Rp {rev/1e9:.1f}M" if rev >= 1e9
                   else f"Rp {rev/1e6:.1f}jt" if rev >= 1e6
                   else f"Rp {rev/1e3:.0f}rb")
        price_str = (f"Rp {p.price_min:,.0f}" if p.price_min == p.price_max
                     else f"Rp {p.price_min:,.0f}–{p.price_max:,.0f}")
        name = p.name[:33] + "…" if len(p.name) > 33 else p.name
        score_color = ("green" if p.opportunity_score >= 70
                       else "yellow" if p.opportunity_score >= 50
                       else "red")

        table.add_row(
            p.platform,
            str(i),
            name + (" 🏅" if p.is_official else ""),
            price_str,
            f"{p.sold:,}",
            rev_str,
            f"{p.rating_star:.1f}",
            f"[{score_color}]{p.opportunity_score}[/{score_color}]",
        )

    console.print(table)
    total_rev = sum(p.revenue_estimate for p in products)
    sellers   = len(set(p.shop_id for p in products))
    console.print(Panel(
        f"[bold]Total:[/bold] {len(products)} produk  │  "
        f"[bold]Seller:[/bold] {sellers}  │  "
        f"[bold]Est. Rev:[/bold] [yellow]Rp {total_rev/1e6:.1f}jt[/yellow]",
        border_style="cyan",
    ))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.command(name="health-check")
def health_check_cmd(
    marketplace: str = typer.Option("all", "--marketplace", "-m",
                                    help="shopee | tokopedia | tiktok_shop | all"),
    watch: bool  = typer.Option(False,  "--watch", "-w",
                                 help="Loop terus, check tiap --interval detik"),
    interval: int= typer.Option(3600,   "--interval", "-i",
                                 help="Interval detik untuk watch mode"),
    notify: bool = typer.Option(True,   "--notify/--no-notify",
                                 help="Kirim Telegram alert jika ada masalah"),
    ai_diag: bool= typer.Option(False,  "--ai",
                                 help="Claude AI diagnosis jika schema berubah"),
):
    """
    🩺 Health check semua endpoint marketplace.
    Deteksi schema change, latency spike, dan dead endpoints.
    Jalankan tiap hari via cron/n8n untuk self-monitoring.
    """
    conn       = init_health_db()
    connectors = (
        get_all_connectors()
        if marketplace == "all"
        else [get_connector(marketplace)]
    )

    async def _run_once():
        all_results: list[EndpointHealthResult] = []
        all_changes: list[Optional[dict]]        = []

        limits = httpx.Limits(max_connections=6, max_keepalive_connections=3)
        async with httpx.AsyncClient(http2=True, follow_redirects=True, limits=limits) as client:
            for connector in connectors:
                console.print(f"\n[bold]Checking [cyan]{connector.platform_name}[/cyan]...[/bold]")
                with Progress(SpinnerColumn(), TextColumn("{task.description}"),
                              TimeElapsedColumn(), console=console, transient=True) as prog:
                    task = prog.add_task(f"  {connector.platform_name}", total=None)
                    results = await connector.health_check_endpoints(client)
                    prog.update(task, completed=True)

                changes = []
                for r in results:
                    save_health_result(conn, r)
                    change = detect_schema_change(conn, r)
                    changes.append(change)

                    if change and notify:
                        if ai_diag:
                            await alert_schema_change(change, r)
                        else:
                            msg = (
                                f"⚠️ SCHEMA CHANGED: {r.platform}/{r.endpoint_name}\n"
                                f"Hash: {change['old_hash'][:8]} → {change['new_hash'][:8]}\n"
                                f"Removed: {change.get('keys_removed', [])}\n"
                                f"Added: {change.get('keys_added', [])}"
                            )
                            await send_telegram(msg)
                    elif r.status in ("DEAD", "TIMEOUT") and notify:
                        await send_telegram(
                            f"🔴 ENDPOINT DOWN: {r.platform}/{r.endpoint_name}\n"
                            f"Status: {r.status} | Error: {r.error}"
                        )

                all_results.extend(results)
                all_changes.extend(changes)

        display_health_results(all_results, all_changes)

        # Summary
        ok_count   = sum(1 for r in all_results if r.is_healthy)
        fail_count = len(all_results) - ok_count
        change_count = sum(1 for c in all_changes if c)

        summary_color = "green" if fail_count == 0 else "red"
        console.print(Panel(
            f"[bold]Checked:[/bold] {len(all_results)} endpoints  │  "
            f"[green]OK: {ok_count}[/green]  │  "
            f"[{summary_color}]Failed: {fail_count}[/{summary_color}]  │  "
            f"[yellow]Schema Changes: {change_count}[/yellow]  │  "
            f"[dim]{datetime.now().strftime('%H:%M:%S %d/%m/%Y')}[/dim]",
            title="🩺 Health Summary",
            border_style=summary_color,
        ))

        return fail_count, change_count

    if not watch:
        asyncio.run(_run_once())
    else:
        run = 0
        while True:
            run += 1
            console.rule(f"[dim]Run #{run} — {datetime.now().strftime('%H:%M:%S')}[/dim]")
            fail, changes = asyncio.run(_run_once())
            next_run = datetime.now() + timedelta(seconds=interval)
            console.print(
                f"[dim]💤 Next check: {next_run.strftime('%H:%M:%S')} "
                f"(in {interval//60}m)[/dim]"
            )
            time.sleep(interval)


@app.command()
def search(
    keyword:     str           = typer.Argument(..., help="Keyword produk"),
    marketplace: str           = typer.Option("shopee", "--marketplace", "-m",
                                              help="shopee | tokopedia | tiktok_shop | all"),
    limit:       int           = typer.Option(30,  "--limit", "-l"),
    sort:        str           = typer.Option("sales", "--sort", "-s",
                                              help="sales | price | rating | new"),
    ai:          bool          = typer.Option(False, "--ai", help="Claude AI insight"),
    output:      Optional[str] = typer.Option(None,  "--output", "-o"),
):
    """🔍 Search produk lintas marketplace — output selalu format yang sama."""

    async def _run():
        connectors = (
            get_all_connectors() if marketplace == "all"
            else [get_connector(marketplace)]
        )

        all_products: list[NormalizedProduct] = []
        for connector in connectors:
            console.print(f"[dim]→ Searching {connector.platform_name}...[/dim]")
            with console.status(f"[cyan]{connector.platform_name}[/cyan]"):
                try:
                    products = await connector.search(keyword, limit, sort)
                    all_products.extend(products)
                    console.print(f"  [green]✓[/green] {connector.platform_name}: {len(products)} produk")
                except Exception as e:
                    console.print(f"  [red]✗[/red] {connector.platform_name}: {e}")

        if not all_products:
            console.print("[yellow]Tidak ada produk ditemukan.[/yellow]")
            return

        all_products = compute_scores(all_products)
        display_products(all_products, f'🔍 "{keyword}" — {marketplace}')

        if ai:
            api_key = os.getenv("ANTHROPIC_API_KEY")
            if api_key:
                import anthropic
                ac = anthropic.Anthropic(api_key=api_key)
                top5 = "\n".join([
                    f"- [{p.platform}] {p.name[:50]} | Terjual: {p.sold:,} "
                    f"| Rev: Rp{p.revenue_estimate/1e6:.1f}jt | Skor: {p.opportunity_score}"
                    for p in all_products[:5]
                ])
                prompt = (
                    f'Analis e-commerce Indonesia. Keyword: "{keyword}"\n\n'
                    f"TOP 5:\n{top5}\n\n"
                    f"Berikan insight:\n"
                    f"1. KONDISI PASAR (2 kalimat, spesifik angka)\n"
                    f"2. PELUANG TERSEMBUNYI (1 kalimat)\n"
                    f"3. AKSI KONKRET (1 kalimat)\n"
                    f"Bahasa Indonesia. Padat."
                )
                with console.status("[cyan]Generating AI insight...[/cyan]"):
                    msg = ac.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=350,
                        messages=[{"role": "user", "content": prompt}],
                    )
                console.print(Panel(msg.content[0].text,
                                    title="🤖 AI Market Intelligence",
                                    border_style="cyan", padding=(1, 2)))

        if output:
            data = [asdict(p) for p in all_products]
            suffix = Path(output).suffix.lower()
            if suffix == ".csv":
                import csv
                with open(output, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
            else:
                with open(output, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            console.print(f"[green]✓ Exported → {output}[/green]")

    asyncio.run(_run())


@app.command(name="schema-diff")
def schema_diff_cmd(
    since: int = typer.Option(7, "--since", "-s", help="Tampilkan perubahan N hari terakhir"),
):
    """📋 Lihat riwayat semua schema changes yang terdeteksi."""
    conn    = init_health_db()
    alerts  = get_recent_alerts(conn, days=since)

    if not alerts:
        console.print(Panel(
            f"[green]✅ Tidak ada schema change dalam {since} hari terakhir.[/green]",
            title="📋 Schema Diff History",
            border_style="green",
        ))
        return

    console.print(f"\n[bold yellow]⚠️  {len(alerts)} schema change(s) dalam {since} hari terakhir[/bold yellow]\n")
    for a in alerts:
        m = a["message"]
        console.print(Panel(
            f"[bold]Platform:[/bold]  {a['platform']}\n"
            f"[bold]Endpoint:[/bold]  {a['endpoint']}\n"
            f"[bold]Waktu:[/bold]     {a['at'][:19]}\n"
            f"[bold]Hash:[/bold]      {m.get('old_hash','?')[:8]} → {m.get('new_hash','?')[:8]}\n"
            f"[red]Keys Hilang:[/red]  {m.get('keys_removed', [])}\n"
            f"[green]Keys Baru:[/green]    {m.get('keys_added', [])}\n"
            f"[yellow]Type Berubah:[/yellow] {m.get('type_changed', [])}",
            title=f"[yellow]⚠️  Schema Change — {a['platform']}/{a['endpoint']}[/yellow]",
            border_style="yellow",
        ))


@app.command()
def dashboard(
    hours: int = typer.Option(24, "--hours", "-h", help="Window analisis dalam jam"),
):
    """📊 Dashboard uptime & latency semua endpoint dalam N jam terakhir."""
    conn  = init_health_db()
    stats = get_uptime_stats(conn, hours=hours)

    if not stats:
        console.print("[yellow]Belum ada data health check. Jalankan 'health-check' dulu.[/yellow]")
        return

    table = Table(
        title=f"[bold cyan]📊 Uptime Dashboard — {hours}h window[/bold cyan]",
        box=box.ROUNDED, border_style="dim", expand=True,
    )
    table.add_column("Platform",   style="cyan",  width=14)
    table.add_column("Endpoint",   style="white", width=18)
    table.add_column("Uptime %",   width=10,  justify="right")
    table.add_column("Avg Latency",width=12,  justify="right", style="green")
    table.add_column("Status Bar", width=30)

    for key, s in sorted(stats.items()):
        pct    = s["uptime_pct"]
        color  = "green" if pct >= 99 else "yellow" if pct >= 90 else "red"
        bar_ok = int(pct / 5)
        bar    = f"[green]{'█' * bar_ok}[/green][dim]{'░' * (20 - bar_ok)}[/dim]"

        table.add_row(
            s["platform"],
            s["endpoint"],
            f"[{color}]{pct}%[/{color}]",
            f"{s['avg_latency']:.0f}ms",
            bar,
        )

    console.print(table)

    # Alert summary
    alerts = get_recent_alerts(conn, days=1)
    if alerts:
        console.print(Panel(
            f"[yellow]⚠️  {len(alerts)} schema change(s) dalam 24 jam terakhir[/yellow]\n"
            f"Jalankan [cyan]schema-diff[/cyan] untuk detail.",
            border_style="yellow",
        ))
    else:
        console.print(Panel("[green]✅ Tidak ada schema change dalam 24 jam.[/green]",
                            border_style="green"))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ENTRYPOINT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    console.print(Panel.fit(
        "[bold cyan]MARKETPLACE INTELLIGENCE ENGINE[/bold cyan] [dim]v4.0[/dim]\n"
        "[dim]Adapter Pattern · Self-Monitoring · Multi-Platform[/dim]\n\n"
        "[dim]Commands: health-check | search | schema-diff | dashboard[/dim]\n"
        "[dim]Platforms: shopee | tokopedia | tiktok_shop | all[/dim]",
        border_style="cyan",
    ))
    app()
