#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║   MARKET INTELLIGENCE TELEMETRY PIPELINE                         ║
║   China Supplier × Indonesia Marketplace                         ║
║                                                                  ║
║   Menggabungkan:                                                 ║
║     - Google Trends (demand heatmap per provinsi)                ║
║     - Shopee Search API (supply density, pricing, velocity)      ║
║     - SQLite (historis untuk tren mingguan)                      ║
║     - Telemetry Engine (gap score, Pareto, momentum)             ║
║                                                                  ║
║   Install: pip install requests                                  ║
║                                                                  ║
║   Usage:                                                         ║
║     python telemetry_pipeline.py seed      # Muat data Trends    ║
║     python telemetry_pipeline.py collect   # Ambil data Shopee   ║
║     python telemetry_pipeline.py report    # Tampilkan laporan   ║
║     python telemetry_pipeline.py run       # Semua sekaligus     ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

# ══════════════════════════════════════════════════════════════════
# KONFIGURASI
# ══════════════════════════════════════════════════════════════════

DB_PATH = "telemetry.db"

# Keyword prioritas dari riset Google Trends
# Format: (keyword_shopee, keyword_trends, label_tampilan)
KEYWORD_CONFIG = [
    ("lampu led",           "lampu china",      "Lampu & Pencahayaan"),
    ("sepatu import",       "sepatu china",     "Sepatu Import"),
    ("skincare viral",      "skincare china",   "Skincare China"),
    ("baju import wanita",  "baju china",       "Fashion Wanita Import"),
    ("powerbank",           "powerbank china",  "Powerbank"),
    ("tas import",          "tas china",        "Tas Import"),
]

# Data Google Trends dari riset sesungguhnya (Supermetrics, Apr 2026)
# Format: keyword → {provinsi: interest_score}
TRENDS_REGIONAL_DATA = {
    "lampu china": {
        "Kalimantan Selatan": 100, "Kalimantan Barat": 84, "Kalimantan Timur": 76,
        "Banten": 61, "DKI Jakarta": 61, "Jawa Timur": 53, "Sumatera Utara": 53,
        "Jawa Tengah": 46, "Sumatera Selatan": 46, "Jawa Barat": 46,
        "Kepulauan Riau": 7, "Lainnya": 0,
    },
    "skincare china": {
        "DKI Jakarta": 100, "Bali": 68, "Yogyakarta": 63,
        "Banten": 42, "Sumatera Utara": 42, "Jawa Timur": 36,
        "Jawa Barat": 36, "Jawa Tengah": 26, "Lainnya": 0,
    },
    "sepatu china": {
        "Bangka Belitung": 100, "Kepulauan Riau": 60, "DKI Jakarta": 52,
        "Yogyakarta": 40, "Banten": 36, "Jawa Tengah": 36,
        "Jawa Timur": 34, "Sumatera Utara": 34, "Sulawesi Selatan": 34,
        "Jawa Barat": 32, "Kalimantan Barat": 32, "Kalimantan Timur": 30,
        "Riau": 30, "Sumatera Selatan": 30,
    },
    "baju china": {
        "Bangka Belitung": 100, "Kalimantan Barat": 76, "Kalimantan Utara": 73,
        "Kepulauan Riau": 72, "Kalimantan Tengah": 62, "Sulawesi Utara": 60,
        "Sumatera Utara": 58, "Kalimantan Timur": 56, "Jambi": 55,
        "Bengkulu": 53, "Kalimantan Selatan": 52, "DKI Jakarta": 50,
        "Banten": 48, "Papua": 48, "Riau": 47, "Lampung": 45,
        "Jawa Timur": 44, "Sulawesi Selatan": 44, "Jawa Barat": 43,
        "Jawa Tengah": 37, "Yogyakarta": 36,
    },
    "powerbank china": {
        "DKI Jakarta": 100, "Jawa Barat": 72, "Banten": 65,
        "Jawa Timur": 58, "Jawa Tengah": 45, "Sumatera Utara": 40,
        "Lainnya": 20,
    },
    "tas china": {
        "DKI Jakarta": 100, "Banten": 68, "Jawa Barat": 55,
        "Jawa Timur": 48, "Jawa Tengah": 40, "Sumatera Utara": 35,
        "Bali": 30, "Lainnya": 15,
    },
}

# Interest over time (mingguan, data aktual Apr 2026)
TRENDS_TIME_DATA = {
    "lampu china":     [93, 100, 80, 65, 76, 70, 99, 88, 65, 82, 80, 71, 80, 97, 89, 46, 46],
    "baju china":      [33, 36, 33, 38, 35, 54, 36, 35, 27, 34, 23, 35, 33, 33, 39, 41, 50,
                        53, 36, 47, 49, 56, 50, 50, 48, 53, 100, 59, 61, 37, 49, 46, 50, 45, 61],
    "sepatu china":    [63, 50, 33, 67, 43, 100, 47, 52, 56, 65, 58, 59, 47, 61, 40, 70, 43,
                        45, 45, 49, 48, 56, 44, 43, 33, 35, 51, 54, 67, 68, 62, 65, 61, 39, 44,
                        49, 44, 57, 57, 71, 39, 38, 34, 44, 42, 50, 63, 56, 34, 60, 60, 49, 46, 41, 37],
    "skincare china":  [48, 41, 75, 64, 70, 69, 100, 85, 50, 42, 65, 62, 82, 47, 35, 41, 52, 43, 48, 54, 20, 23, 20],
    "powerbank china": [100, 79, 60, 54, 62, 45, 48, 54, 54, 50, 49, 43, 45, 23, 24],
    "tas china":       [45, 50, 42, 37, 50, 31, 26, 31, 30, 54, 30, 44, 46, 37, 26, 42, 40,
                        27, 39, 40, 41, 29, 39, 34, 47, 33, 27, 47, 48, 29, 37, 30, 28, 41,
                        33, 35, 56, 34, 31],
}


# ══════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS trends_regional (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword     TEXT,
            provinsi    TEXT,
            interest    INTEGER,
            collected   TEXT
        );
        CREATE TABLE IF NOT EXISTS trends_time (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword     TEXT,
            minggu_ke   INTEGER,
            interest    INTEGER,
            collected   TEXT
        );
        CREATE TABLE IF NOT EXISTS shopee_products (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword           TEXT,
            nama              TEXT,
            harga_min         REAL,
            harga_max         REAL,
            terjual           INTEGER,
            rating            REAL,
            jumlah_review     INTEGER,
            stok              INTEGER,
            disukai           INTEGER,
            nama_toko         TEXT,
            lokasi_toko       TEXT,
            official          INTEGER,
            url               TEXT,
            estimasi_revenue  REAL,
            skor_peluang      REAL,
            collected         TEXT
        );
        CREATE TABLE IF NOT EXISTS telemetry_snapshots (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword              TEXT,
            label                TEXT,
            trends_keyword       TEXT,
            demand_score         REAL,
            supply_count         INTEGER,
            gap_score            REAL,
            avg_harga            REAL,
            median_terjual       REAL,
            total_revenue_est    REAL,
            pareto_index         REAL,
            herfindahl_index     REAL,
            official_ratio       REAL,
            avg_love_ratio       REAL,
            avg_convert_ratio    REAL,
            top_lokasi           TEXT,
            geo_arbitrage_score  REAL,
            opportunity_score    REAL,
            collected            TEXT
        );
    """)
    conn.commit()
    return conn


# ══════════════════════════════════════════════════════════════════
# DATA CONTRACTS
# ══════════════════════════════════════════════════════════════════

@dataclass
class Produk:
    keyword:          str
    nama:             str
    harga_min:        float
    harga_max:        float
    terjual:          int
    rating:           float
    jumlah_review:    int
    stok:             int
    disukai:          int
    nama_toko:        str
    lokasi_toko:      str
    official:         bool
    url:              str
    estimasi_revenue: float = 0.0
    skor_peluang:     float = 0.0

    def __post_init__(self):
        self.estimasi_revenue = round(self.terjual * self.harga_min, 0)


@dataclass
class TelemetryResult:
    keyword:             str
    label:               str
    trends_keyword:      str
    demand_score:        float    # Rata-rata Google Trends interest
    supply_count:        int      # Jumlah produk di Shopee
    gap_score:           float    # demand/supply gap (makin tinggi = makin terbuka)
    avg_harga:           float
    median_terjual:      float
    total_revenue_est:   float
    pareto_index:        float    # % revenue dari top 20% produk
    herfindahl_index:    float    # 0=kompetitif, 1=monopoli
    official_ratio:      float    # % produk official
    avg_love_ratio:      float    # disukai/terjual
    avg_convert_ratio:   float    # terjual/view (jika ada)
    top_lokasi:          str      # Provinsi Shopee terbanyak
    geo_arbitrage_score: float    # Trends demand - Shopee supply match
    opportunity_score:   float    # Composite final score


# ══════════════════════════════════════════════════════════════════
# STEP 1: SEED GOOGLE TRENDS DATA
# ══════════════════════════════════════════════════════════════════

def seed_trends(conn: sqlite3.Connection):
    print("\n[1/4] Menyimpan data Google Trends ke database...")
    now = datetime.now().isoformat()

    # Regional data
    for keyword, regions in TRENDS_REGIONAL_DATA.items():
        for provinsi, interest in regions.items():
            conn.execute("""
                INSERT INTO trends_regional (keyword, provinsi, interest, collected)
                VALUES (?, ?, ?, ?)
            """, (keyword, provinsi, interest, now))

    # Time series data
    for keyword, series in TRENDS_TIME_DATA.items():
        for i, interest in enumerate(series):
            conn.execute("""
                INSERT INTO trends_time (keyword, minggu_ke, interest, collected)
                VALUES (?, ?, ?, ?)
            """, (keyword, i + 1, interest, now))

    conn.commit()
    print(f"  ✓ {sum(len(v) for v in TRENDS_REGIONAL_DATA.values())} data regional tersimpan")
    print(f"  ✓ {sum(len(v) for v in TRENDS_TIME_DATA.values())} data time-series tersimpan")


# ══════════════════════════════════════════════════════════════════
# STEP 2: COLLECT SHOPEE DATA
# ══════════════════════════════════════════════════════════════════

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

def _headers():
    return {
        "User-Agent":       random.choice(USER_AGENTS),
        "Accept":           "application/json",
        "Accept-Language":  "id-ID,id;q=0.9",
        "Referer":          "https://shopee.co.id/",
        "Origin":           "https://shopee.co.id",
        "X-Api-Source":     "pc",
        "X-Requested-With": "XMLHttpRequest",
    }

def _parse_produk(keyword: str, raw: dict) -> Optional[Produk]:
    info   = raw.get("item_basic", raw)
    rating = info.get("item_rating", {})
    iid    = info.get("itemid", 0)
    sid    = info.get("shopid", 0)
    if not iid or not sid:
        return None
    harga_min = info.get("price", 0) / 100000
    harga_max = info.get("price_max", info.get("price", 0)) / 100000
    return Produk(
        keyword       = keyword,
        nama          = info.get("name", "")[:80],
        harga_min     = harga_min,
        harga_max     = harga_max,
        terjual       = info.get("sold", 0),
        rating        = rating.get("rating_star", 0.0),
        jumlah_review = sum(rating.get("rating_count", [0] * 6)),
        stok          = info.get("stock", 0),
        disukai       = info.get("liked_count", 0),
        nama_toko     = info.get("shop_name", ""),
        lokasi_toko   = info.get("shop_location", ""),
        official      = bool(info.get("is_official_shop", False)),
        url           = f"https://shopee.co.id/product/{sid}/{iid}",
    )

def collect_shopee(keyword: str, limit: int = 60) -> list[Produk]:
    hasil   = []
    session = requests.Session()
    for offset in range(0, limit, 60):
        batch = min(60, limit - offset)
        params = {
            "by": "sales", "keyword": keyword,
            "limit": batch, "newest": offset, "order": "desc",
            "page_type": "search", "scenario": "PAGE_GLOBAL_SEARCH",
            "version": 2, "matchtype": 0,
        }
        time.sleep(random.uniform(0.8, 1.5))
        try:
            resp = session.get(
                "https://shopee.co.id/api/v4/search/search_items",
                params=params, headers=_headers(), timeout=12,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"    [!] {keyword}: {e}")
            break
        items = data.get("items", [])
        for raw in items:
            p = _parse_produk(keyword, raw)
            if p:
                hasil.append(p)
        if len(items) < batch:
            break
    return hasil

def save_shopee(conn: sqlite3.Connection, produk_list: list[Produk]):
    now = datetime.now().isoformat()
    for p in produk_list:
        conn.execute("""
            INSERT INTO shopee_products
            (keyword, nama, harga_min, harga_max, terjual, rating, jumlah_review,
             stok, disukai, nama_toko, lokasi_toko, official, url,
             estimasi_revenue, skor_peluang, collected)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (p.keyword, p.nama, p.harga_min, p.harga_max, p.terjual, p.rating,
              p.jumlah_review, p.stok, p.disukai, p.nama_toko, p.lokasi_toko,
              int(p.official), p.url, p.estimasi_revenue, p.skor_peluang, now))
    conn.commit()

def step_collect(conn: sqlite3.Connection, limit: int = 60):
    print("\n[2/4] Mengambil data produk dari Shopee...")
    total = 0
    for kw_shopee, kw_trends, label in KEYWORD_CONFIG:
        print(f"  Mencari \"{kw_shopee}\"... ", end="", flush=True)
        produk = collect_shopee(kw_shopee, limit)
        if produk:
            save_shopee(conn, produk)
            print(f"{len(produk)} produk tersimpan")
            total += len(produk)
        else:
            print("tidak ada hasil / error koneksi")
    print(f"  ✓ Total {total} produk tersimpan ke database")


# ══════════════════════════════════════════════════════════════════
# STEP 3: TELEMETRY ENGINE
# ══════════════════════════════════════════════════════════════════

def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    return (s[n // 2] + s[(n - 1) // 2]) / 2

def _pareto_index(revenues: list[float]) -> float:
    """% revenue dari top 20% produk (idealnya ~80% = pasar sehat)."""
    if not revenues:
        return 0.0
    s = sorted(revenues, reverse=True)
    top20 = s[:max(1, len(s) // 5)]
    return round(sum(top20) / max(sum(s), 1) * 100, 1)

def _herfindahl(revenues: list[float]) -> float:
    """0 = sangat kompetitif, 1 = monopoli."""
    total = sum(revenues)
    if not total:
        return 0.0
    return round(sum((r / total) ** 2 for r in revenues), 4)

def _geo_arbitrage(trends_keyword: str, lokasi_toko_list: list[str]) -> tuple[float, str]:
    """
    Bandingkan provinsi dengan demand Trends tinggi
    vs provinsi asal seller Shopee.
    Score tinggi = demand di provinsi X tapi supply dari luar X.
    """
    regional = TRENDS_REGIONAL_DATA.get(trends_keyword, {})
    if not regional or not lokasi_toko_list:
        return 0.0, "-"

    # Top provinsi demand
    top_demand = sorted(regional.items(), key=lambda x: x[1], reverse=True)
    high_demand_provinces = {p for p, s in top_demand[:5] if s > 50}

    # Top provinsi supply (dari Shopee)
    supply_count: dict[str, int] = {}
    for lok in lokasi_toko_list:
        if lok:
            supply_count[lok] = supply_count.get(lok, 0) + 1
    top_supply = sorted(supply_count.items(), key=lambda x: x[1], reverse=True)
    top_lokasi = top_supply[0][0] if top_supply else "-"
    high_supply_provinces = {p for p, _ in top_supply[:5]}

    # Gap = demand provinces NOT covered by supply
    gap_provinces = high_demand_provinces - high_supply_provinces
    arbitrage_score = len(gap_provinces) / max(len(high_demand_provinces), 1) * 100
    return round(arbitrage_score, 1), top_lokasi

def compute_telemetry(
    conn: sqlite3.Connection,
    kw_shopee: str,
    kw_trends: str,
    label: str,
) -> Optional[TelemetryResult]:

    # Ambil data Shopee terbaru
    rows = conn.execute("""
        SELECT * FROM shopee_products
        WHERE keyword = ?
        ORDER BY collected DESC LIMIT 200
    """, (kw_shopee,)).fetchall()

    if not rows:
        return None

    produk_list = list(rows)

    # Ambil demand score dari Google Trends (rata-rata time series)
    ts = conn.execute("""
        SELECT AVG(interest) FROM trends_time WHERE keyword = ?
    """, (kw_trends,)).fetchone()
    demand_score = round(ts[0] or 0, 1)

    # Hitung supply metrics
    revenues  = [r["estimasi_revenue"] for r in produk_list]
    harga_all = [r["harga_min"] for r in produk_list if r["harga_min"] > 0]
    terjual_all = [r["terjual"] for r in produk_list]
    lokasi_all  = [r["lokasi_toko"] for r in produk_list]
    disukai_all = [r["disukai"] for r in produk_list]

    supply_count   = len(produk_list)
    avg_harga      = round(sum(harga_all) / max(len(harga_all), 1), 0)
    median_terjual = _median(terjual_all)
    total_rev      = sum(revenues)
    pareto         = _pareto_index(revenues)
    herfindahl     = _herfindahl(revenues)
    official_ratio = round(sum(1 for r in produk_list if r["official"]) / supply_count * 100, 1)

    # Love ratio = disukai / terjual (viral potential)
    love_ratios = [
        d / max(t, 1)
        for d, t in zip(disukai_all, terjual_all)
    ]
    avg_love = round(sum(love_ratios) / max(len(love_ratios), 1), 3)

    # Geographic arbitrage
    geo_score, top_lokasi = _geo_arbitrage(kw_trends, lokasi_all)

    # Gap score = demand/supply normalized
    # Jika demand tinggi (Trends 80+) tapi supply sedikit (<20 produk) → gap besar
    normalized_demand = demand_score / 100  # 0-1
    normalized_supply = min(supply_count / 100, 1.0)  # 0-1, cap at 100 products
    gap_score = round((normalized_demand - normalized_supply * 0.5) * 100, 1)
    gap_score = max(0.0, gap_score)

    # Composite opportunity score
    # demand(30%) + gap(25%) + geo_arbitrage(20%) + love_ratio(15%) + low_competition(10%)
    low_comp = max(0.0, 100 - (herfindahl * 100) * 2)
    opp_score = round(
        demand_score      * 0.30 +
        gap_score         * 0.25 +
        geo_score         * 0.20 +
        min(avg_love * 200, 100) * 0.15 +
        low_comp          * 0.10,
        1
    )

    return TelemetryResult(
        keyword            = kw_shopee,
        label              = label,
        trends_keyword     = kw_trends,
        demand_score       = demand_score,
        supply_count       = supply_count,
        gap_score          = gap_score,
        avg_harga          = avg_harga,
        median_terjual     = median_terjual,
        total_revenue_est  = total_rev,
        pareto_index       = pareto,
        herfindahl_index   = herfindahl,
        official_ratio     = official_ratio,
        avg_love_ratio     = avg_love,
        avg_convert_ratio  = 0.0,
        top_lokasi         = top_lokasi,
        geo_arbitrage_score= geo_score,
        opportunity_score  = opp_score,
    )

def save_telemetry(conn: sqlite3.Connection, t: TelemetryResult):
    now = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO telemetry_snapshots
        (keyword, label, trends_keyword, demand_score, supply_count, gap_score,
         avg_harga, median_terjual, total_revenue_est, pareto_index, herfindahl_index,
         official_ratio, avg_love_ratio, avg_convert_ratio, top_lokasi,
         geo_arbitrage_score, opportunity_score, collected)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        t.keyword, t.label, t.trends_keyword, t.demand_score, t.supply_count,
        t.gap_score, t.avg_harga, t.median_terjual, t.total_revenue_est,
        t.pareto_index, t.herfindahl_index, t.official_ratio, t.avg_love_ratio,
        t.avg_convert_ratio, t.top_lokasi, t.geo_arbitrage_score,
        t.opportunity_score, now,
    ))
    conn.commit()

def step_analyze(conn: sqlite3.Connection) -> list[TelemetryResult]:
    print("\n[3/4] Menghitung semua telemetri...")
    results = []
    for kw_shopee, kw_trends, label in KEYWORD_CONFIG:
        t = compute_telemetry(conn, kw_shopee, kw_trends, label)
        if t:
            save_telemetry(conn, t)
            results.append(t)
            print(f"  ✓ {label}: skor={t.opportunity_score} | gap={t.gap_score} | demand={t.demand_score}")
        else:
            print(f"  - {label}: belum ada data Shopee (jalankan 'collect' dulu)")
    return results


# ══════════════════════════════════════════════════════════════════
# STEP 4: REPORT GENERATOR
# ══════════════════════════════════════════════════════════════════

def _bar(value: float, max_val: float = 100, width: int = 20) -> str:
    filled = int((value / max(max_val, 1)) * width)
    return "█" * filled + "░" * (width - filled)

def _skor_label(score: float) -> str:
    if score >= 75:   return "SANGAT BAIK ★★★"
    elif score >= 55: return "BAIK ★★"
    elif score >= 35: return "SEDANG ★"
    else:             return "RENDAH"

def _format_rp(nilai: float) -> str:
    if nilai >= 1_000_000_000: return f"Rp {nilai/1e9:.1f} Miliar"
    elif nilai >= 1_000_000:   return f"Rp {nilai/1e6:.1f} Juta"
    elif nilai >= 1_000:       return f"Rp {nilai/1e3:.0f} Ribu"
    return f"Rp {nilai:.0f}"

def generate_report(results: list[TelemetryResult], conn: sqlite3.Connection):
    print("\n[4/4] Membuat laporan...")

    now_str = datetime.now().strftime("%d %B %Y, %H:%M")
    sep  = "═" * 80
    sep2 = "─" * 80

    print(f"\n\n{sep}")
    print(f"  MARKET INTELLIGENCE TELEMETRY REPORT")
    print(f"  China Supplier × Indonesia Marketplace")
    print(f"  {now_str}")
    print(f"{sep}")

    if not results:
        print("\n  Belum ada data. Jalankan: python telemetry_pipeline.py run")
        return

    # ── RANKING UTAMA ────────────────────────────────────────
    print(f"\n  {'RANKING PELUANG KATEGORI':^78}")
    print(f"{sep2}")
    print(f"  {'#':<3} {'KATEGORI':<26} {'SKOR':>6} {'DEMAND':>7} {'GAP':>6} "
          f"{'GEO ARB':>8} {'VISUAL'}")
    print(f"{sep2}")

    sorted_results = sorted(results, key=lambda x: x.opportunity_score, reverse=True)
    for i, t in enumerate(sorted_results, 1):
        bar = _bar(t.opportunity_score)
        print(
            f"  {i:<3} {t.label:<26} {t.opportunity_score:>5.1f} "
            f"{t.demand_score:>7.1f} {t.gap_score:>6.1f} "
            f"{t.geo_arbitrage_score:>7.1f}%  {bar}"
        )

    # ── DETAIL PER KATEGORI ──────────────────────────────────
    print(f"\n\n{sep}")
    print(f"  ANALISIS DETAIL PER KATEGORI")
    print(f"{sep}")

    for t in sorted_results:
        print(f"\n  ┌─ {t.label.upper()} {'─' * (60 - len(t.label))}")
        print(f"  │  Skor Peluang  : {t.opportunity_score:>6.1f}/100  {_skor_label(t.opportunity_score)}")
        print(f"  │")
        print(f"  │  [DEMAND — Google Trends]")
        print(f"  │  Interest rata-rata  : {t.demand_score:>6.1f}/100")
        print(f"  │  Visual              : {_bar(t.demand_score)}")
        print(f"  │")
        print(f"  │  [SUPPLY — Shopee]")
        print(f"  │  Jumlah produk       : {t.supply_count:>6} produk")
        print(f"  │  Rata-rata harga     : {_format_rp(t.avg_harga):>18}")
        print(f"  │  Median terjual      : {t.median_terjual:>6.0f} unit")
        print(f"  │  Est. Revenue total  : {_format_rp(t.total_revenue_est):>18}")
        print(f"  │  Produk Official     : {t.official_ratio:>5.1f}%")
        print(f"  │  Top lokasi seller   : {t.top_lokasi}")
        print(f"  │")
        print(f"  │  [KOMPETISI]")
        hhi_label = ("Sangat Kompetitif" if t.herfindahl_index < 0.1
                     else "Kompetitif" if t.herfindahl_index < 0.25
                     else "Oligopoli" if t.herfindahl_index < 0.5
                     else "Monopolistik")
        print(f"  │  Herfindahl Index    : {t.herfindahl_index:.4f}  ({hhi_label})")
        print(f"  │  Pareto Revenue      : Top 20% seller kuasai {t.pareto_index:.0f}% revenue")
        print(f"  │")
        print(f"  │  [MOMENTUM & VIRAL]")
        love_label = "Tinggi ✓" if t.avg_love_ratio > 0.5 else "Normal"
        print(f"  │  Love Ratio          : {t.avg_love_ratio:.3f}  ({love_label})")
        print(f"  │")
        print(f"  │  [GEOGRAPHIC ARBITRAGE]")
        print(f"  │  Gap Score           : {t.geo_arbitrage_score:.1f}%")

        # Tampilkan provinsi demand tinggi vs supply
        regional = TRENDS_REGIONAL_DATA.get(t.trends_keyword, {})
        top_demand = sorted(regional.items(), key=lambda x: x[1], reverse=True)[:5]
        if top_demand:
            print(f"  │  Top demand provinsi :", end="")
            for prov, score in top_demand:
                if score > 0:
                    print(f" {prov}({score})", end="")
            print()

        print(f"  └{'─' * 68}")

    # ── DEMAND-SUPPLY GAP MATRIX ─────────────────────────────
    print(f"\n\n{sep}")
    print(f"  DEMAND-SUPPLY GAP MATRIX")
    print(f"{sep2}")
    print(f"  {'KATEGORI':<26} {'DEMAND':>7} {'SUPPLY':>8} {'GAP':>7} {'AKSI'}")
    print(f"{sep2}")
    for t in sorted_results:
        supply_norm = min(t.supply_count / 100 * 100, 100)
        gap = t.demand_score - supply_norm * 0.5
        if gap > 40:
            aksi = "→ MASUK SEKARANG"
        elif gap > 20:
            aksi = "→ MONITOR"
        else:
            aksi = "→ KOMPETITIF"
        print(f"  {t.label:<26} {t.demand_score:>6.1f}  {t.supply_count:>7} produk  {gap:>6.1f}  {aksi}")

    # ── REKOMENDASI ──────────────────────────────────────────
    print(f"\n\n{sep}")
    print(f"  REKOMENDASI STRATEGIS")
    print(f"{sep2}")

    if sorted_results:
        top1 = sorted_results[0]
        top2 = sorted_results[1] if len(sorted_results) > 1 else None

        print(f"\n  #1 PRIORITAS UTAMA: {top1.label.upper()}")
        print(f"     Skor {top1.opportunity_score}/100 — {_skor_label(top1.opportunity_score)}")
        print(f"     Demand Google Trends: {top1.demand_score}/100")
        print(f"     Geographic arbitrage: {top1.geo_arbitrage_score:.0f}% provinsi demand belum terlayani")
        print(f"     Aksi: Cari supplier {top1.trends_keyword} — target provinsi demand tinggi")

        if top2:
            print(f"\n  #2 DIVERSIFIKASI: {top2.label.upper()}")
            print(f"     Skor {top2.opportunity_score}/100 — {_skor_label(top2.opportunity_score)}")
            print(f"     Aksi: Mulai setelah #{1} stabil")

    # ── TIMELINE OPTIMAL ─────────────────────────────────────
    print(f"\n\n{sep}")
    print(f"  KALENDER OPTIMAL BERDASARKAN DATA TRENDS")
    print(f"{sep2}")
    calendar = [
        ("Januari–Februari", "Baju import (peak Imlek, skor Trends 100)", "★★★"),
        ("Maret–April",      "Lampu & sepatu (stable demand)",             "★★"),
        ("Mei–Juni",         "Sepatu (peak Jun=100), Powerbank (Jun=100)", "★★★"),
        ("Agustus",          "Agen china (peak 100), Skincare mulai naik", "★★"),
        ("Oktober–November", "Semua kategori (pre-Harbolnas 11.11)",       "★★★"),
        ("Desember",         "Lampu (99), Skincare (85), Fashion",         "★★"),
    ]
    for bulan, desc, bintang in calendar:
        print(f"  {bintang}  {bulan:<22} {desc}")

    print(f"\n{sep}")
    print(f"  Laporan dibuat: {now_str}")
    print(f"  Database: {DB_PATH}")
    print(f"  Jalankan lagi minggu depan untuk melihat perubahan tren.")
    print(f"{sep}\n")

    # ── SIMPAN KE FILE ───────────────────────────────────────
    report_file = f"laporan_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
    import io, contextlib
    buf = io.StringIO()
    # (Laporan sudah dicetak ke stdout, user bisa redirect)
    print(f"  Tip: Simpan laporan dengan:\n"
          f"  python telemetry_pipeline.py report > {report_file}\n")


# ══════════════════════════════════════════════════════════════════
# DEMO MODE (tanpa internet — pakai data simulasi)
# ══════════════════════════════════════════════════════════════════

def generate_demo_shopee(conn: sqlite3.Connection):
    """
    Buat data Shopee simulasi untuk demo/testing.
    Berdasarkan pola pasar nyata Indonesia.
    """
    print("\n[2/4] Mode DEMO: membuat data Shopee simulasi...")
    import math

    demo_configs = {
        "lampu led": {
            "n": 45, "harga_range": (15000, 85000), "sold_range": (500, 15000),
            "rating": (4.5, 4.9), "lokasi": ["Kalimantan Selatan"]*8 + ["DKI Jakarta"]*12 +
                                             ["Jawa Barat"]*10 + ["Jawa Timur"]*8 + ["Banten"]*7,
            "official_chance": 0.15,
        },
        "sepatu import": {
            "n": 52, "harga_range": (80000, 450000), "sold_range": (200, 8000),
            "rating": (4.3, 4.8), "lokasi": ["DKI Jakarta"]*20 + ["Jawa Barat"]*15 +
                                             ["Jawa Timur"]*10 + ["Banten"]*7,
            "official_chance": 0.25,
        },
        "skincare viral": {
            "n": 38, "harga_range": (35000, 280000), "sold_range": (300, 12000),
            "rating": (4.6, 5.0), "lokasi": ["DKI Jakarta"]*18 + ["Banten"]*10 +
                                             ["Jawa Barat"]*5 + ["Bali"]*5,
            "official_chance": 0.35,
        },
        "baju import wanita": {
            "n": 60, "harga_range": (45000, 350000), "sold_range": (100, 5000),
            "rating": (4.2, 4.8), "lokasi": ["DKI Jakarta"]*15 + ["Jawa Barat"]*15 +
                                             ["Jawa Tengah"]*10 + ["Jawa Timur"]*10 + ["Banten"]*10,
            "official_chance": 0.10,
        },
        "powerbank": {
            "n": 40, "harga_range": (85000, 650000), "sold_range": (1000, 25000),
            "rating": (4.4, 4.9), "lokasi": ["DKI Jakarta"]*20 + ["Jawa Barat"]*10 +
                                             ["Banten"]*5 + ["Jawa Timur"]*5,
            "official_chance": 0.45,
        },
        "tas import": {
            "n": 35, "harga_range": (95000, 850000), "sold_range": (50, 3000),
            "rating": (4.3, 4.7), "lokasi": ["DKI Jakarta"]*18 + ["Jawa Barat"]*10 +
                                             ["Banten"]*4 + ["Jawa Timur"]*3,
            "official_chance": 0.20,
        },
    }

    now  = datetime.now().isoformat()
    total = 0
    for keyword, cfg in demo_configs.items():
        for j in range(cfg["n"]):
            harga = random.uniform(*cfg["harga_range"])
            sold  = int(random.uniform(*cfg["sold_range"]) * (1 - j * 0.01))
            sold  = max(sold, 1)
            rev   = sold * harga
            lokasi = random.choice(cfg["lokasi"])
            official = random.random() < cfg["official_chance"]
            disukai  = int(sold * random.uniform(0.3, 1.8))
            rating   = random.uniform(*cfg["rating"])

            conn.execute("""
                INSERT INTO shopee_products
                (keyword, nama, harga_min, harga_max, terjual, rating, jumlah_review,
                 stok, disukai, nama_toko, lokasi_toko, official, url,
                 estimasi_revenue, skor_peluang, collected)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                keyword,
                f"Produk {keyword.title()} #{j+1} {'OFFICIAL' if official else 'Import'}",
                round(harga, 0), round(harga * random.uniform(1.0, 1.3), 0),
                sold, round(rating, 1), int(sold * random.uniform(0.4, 0.9)),
                int(sold * random.uniform(0.1, 2.0)), disukai,
                f"Toko{keyword.replace(' ','')[:8].title()}{j+1}",
                lokasi, int(official),
                f"https://shopee.co.id/product/demo/{j+1}",
                round(rev, 0), 0.0, now,
            ))
        total += cfg["n"]
        print(f"  ✓ \"{keyword}\": {cfg['n']} produk demo dibuat")

    conn.commit()
    print(f"  ✓ Total {total} produk demo tersimpan")


# ══════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Market Intelligence Telemetry Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  seed      Muat data Google Trends ke database
  collect   Ambil data produk dari Shopee (butuh internet ke shopee.co.id)
  demo      Gunakan data simulasi (tanpa internet — untuk testing)
  analyze   Hitung semua telemetri dari data yang ada
  report    Tampilkan laporan lengkap
  run       Jalankan semua (seed + collect + analyze + report)
  run-demo  Jalankan semua dengan data demo (seed + demo + analyze + report)

Contoh:
  python telemetry_pipeline.py run-demo
  python telemetry_pipeline.py run
  python telemetry_pipeline.py report > laporan.txt
        """
    )
    parser.add_argument("command", choices=[
        "seed", "collect", "demo", "analyze", "report", "run", "run-demo"
    ])
    parser.add_argument("--limit", type=int, default=60,
                        help="Jumlah produk per keyword dari Shopee (default: 60)")
    args = parser.parse_args()

    conn = init_db()

    if args.command == "seed":
        seed_trends(conn)

    elif args.command == "collect":
        step_collect(conn, args.limit)

    elif args.command == "demo":
        generate_demo_shopee(conn)

    elif args.command == "analyze":
        results = step_analyze(conn)
        if not results:
            print("\n  Tidak ada data untuk dianalisis.")
            print("  Jalankan dulu: python telemetry_pipeline.py run-demo")

    elif args.command == "report":
        results = step_analyze(conn)
        generate_report(results, conn)

    elif args.command == "run":
        print("\n  MODE: Data NYATA dari Shopee")
        seed_trends(conn)
        step_collect(conn, args.limit)
        results = step_analyze(conn)
        generate_report(results, conn)

    elif args.command == "run-demo":
        print("\n  MODE: Data SIMULASI (demo tanpa internet Shopee)")
        seed_trends(conn)
        generate_demo_shopee(conn)
        results = step_analyze(conn)
        generate_report(results, conn)

    conn.close()


if __name__ == "__main__":
    main()
