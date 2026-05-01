#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║   MARKET INTELLIGENCE TELEMETRY PIPELINE  v2.0                   ║
║   China Supplier × Indonesia Marketplace                         ║
║                                                                  ║
║   Install: pip install requests                                  ║
║                                                                  ║
║   Perintah:                                                      ║
║     run-demo   → Jalankan semua (data simulasi, tanpa internet)  ║
║     run        → Jalankan semua (data nyata Shopee)              ║
║     report     → Tampilkan laporan terakhir                      ║
║     add        → Tambah keyword baru                             ║
║     keywords   → Lihat semua keyword aktif                       ║
║     search     → Cari satu keyword langsung                      ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sqlite3
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

# ══════════════════════════════════════════════════════════════════
# KONFIGURASI — Bisa diubah via keywords.json
# ══════════════════════════════════════════════════════════════════

DB_PATH      = "telemetry.db"
KEYWORDS_FILE = "keywords.json"

# Keyword bawaan — akan disimpan ke keywords.json saat pertama kali jalan
DEFAULT_KEYWORDS = [
    {"shopee": "lampu led",          "trends": "lampu china",     "label": "Lampu & Pencahayaan"},
    {"shopee": "sepatu import",      "trends": "sepatu china",    "label": "Sepatu Import"},
    {"shopee": "skincare viral",     "trends": "skincare china",  "label": "Skincare China"},
    {"shopee": "baju import wanita", "trends": "baju china",      "label": "Fashion Wanita Import"},
    {"shopee": "powerbank",          "trends": "powerbank china", "label": "Powerbank"},
    {"shopee": "tas import",         "trends": "tas china",       "label": "Tas Import"},
]

def load_keywords() -> list[dict]:
    """Baca keywords.json — buat dari default jika belum ada."""
    p = Path(KEYWORDS_FILE)
    if not p.exists():
        save_keywords(DEFAULT_KEYWORDS)
    with open(p, encoding="utf-8") as f:
        return json.load(f)

def save_keywords(keywords: list[dict]):
    with open(KEYWORDS_FILE, "w", encoding="utf-8") as f:
        json.dump(keywords, f, ensure_ascii=False, indent=2)

def add_keyword(shopee: str, trends: str, label: str):
    kws = load_keywords()
    # Cek duplikat
    for k in kws:
        if k["shopee"].lower() == shopee.lower():
            print(f"  [!] Keyword '{shopee}' sudah ada.")
            return
    kws.append({"shopee": shopee, "trends": trends, "label": label})
    save_keywords(kws)
    print(f"  ✓ Ditambahkan: [{label}] shopee='{shopee}' trends='{trends}'")

# ══════════════════════════════════════════════════════════════════
# DATA GOOGLE TRENDS (hasil riset Supermetrics, Apr 2026)
# ══════════════════════════════════════════════════════════════════

TRENDS_REGIONAL_DATA = {
    "lampu china": {
        "Kalimantan Selatan": 100, "Kalimantan Barat": 84, "Kalimantan Timur": 76,
        "Banten": 61, "DKI Jakarta": 61, "Jawa Timur": 53, "Sumatera Utara": 53,
        "Jawa Tengah": 46, "Sumatera Selatan": 46, "Jawa Barat": 46, "Kepulauan Riau": 7,
    },
    "skincare china": {
        "DKI Jakarta": 100, "Bali": 68, "Yogyakarta": 63, "Banten": 42,
        "Sumatera Utara": 42, "Jawa Timur": 36, "Jawa Barat": 36, "Jawa Tengah": 26,
    },
    "sepatu china": {
        "Bangka Belitung": 100, "Kepulauan Riau": 60, "DKI Jakarta": 52, "Yogyakarta": 40,
        "Banten": 36, "Jawa Tengah": 36, "Jawa Timur": 34, "Sumatera Utara": 34,
        "Sulawesi Selatan": 34, "Jawa Barat": 32, "Kalimantan Barat": 32,
        "Kalimantan Timur": 30, "Riau": 30, "Sumatera Selatan": 30,
    },
    "baju china": {
        "Bangka Belitung": 100, "Kalimantan Barat": 76, "Kalimantan Utara": 73,
        "Kepulauan Riau": 72, "Kalimantan Tengah": 62, "Sulawesi Utara": 60,
        "Sumatera Utara": 58, "Kalimantan Timur": 56, "Jambi": 55, "Bengkulu": 53,
        "Kalimantan Selatan": 52, "DKI Jakarta": 50, "Banten": 48, "Riau": 47,
        "Jawa Timur": 44, "Jawa Barat": 43, "Jawa Tengah": 37, "Yogyakarta": 36,
    },
    "powerbank china": {
        "DKI Jakarta": 100, "Jawa Barat": 72, "Banten": 65,
        "Jawa Timur": 58, "Jawa Tengah": 45, "Sumatera Utara": 40,
    },
    "tas china": {
        "DKI Jakarta": 100, "Banten": 68, "Jawa Barat": 55,
        "Jawa Timur": 48, "Jawa Tengah": 40, "Sumatera Utara": 35, "Bali": 30,
    },
}

TRENDS_TIME_DATA = {
    "lampu china":     [93, 100, 80, 65, 76, 70, 99, 88, 65, 82, 80, 71, 80, 97, 89, 46, 46],
    "baju china":      [33, 36, 33, 38, 35, 54, 36, 35, 34, 23, 35, 33, 39, 41, 50,
                        53, 47, 49, 56, 50, 50, 48, 53, 100, 59, 61, 37, 49, 46, 50, 45, 61],
    "sepatu china":    [63, 50, 33, 67, 43, 100, 47, 52, 56, 65, 58, 59, 47, 61, 40,
                        70, 43, 45, 45, 49, 48, 56, 44, 43, 33, 35, 51, 54, 67, 68,
                        62, 65, 61, 39, 44, 49, 44, 57, 57, 71, 39, 38, 34, 44, 42,
                        50, 63, 56, 34, 60, 60, 49, 46, 41, 37],
    "skincare china":  [48, 41, 75, 64, 70, 69, 100, 85, 50, 42, 65, 62, 82, 47, 35, 41, 52, 43, 48, 54, 20, 23, 20],
    "powerbank china": [100, 79, 60, 54, 62, 45, 48, 54, 54, 50, 49, 43, 45, 23, 24],
    "tas china":       [45, 50, 42, 37, 50, 31, 26, 31, 30, 54, 30, 44, 46, 37, 26,
                        42, 40, 27, 39, 40, 41, 29, 34, 47, 33, 27, 47, 48, 29, 37, 30, 28, 41, 33, 35, 56, 34, 31],
}


# ══════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS trends_regional (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT, provinsi TEXT, interest INTEGER, collected TEXT
        );
        CREATE TABLE IF NOT EXISTS trends_time (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT, minggu_ke INTEGER, interest INTEGER, collected TEXT
        );
        CREATE TABLE IF NOT EXISTS shopee_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT, nama TEXT, harga_min REAL, harga_max REAL,
            terjual INTEGER, rating REAL, jumlah_review INTEGER,
            stok INTEGER, disukai INTEGER, nama_toko TEXT,
            lokasi_toko TEXT, official INTEGER, url TEXT,
            estimasi_revenue REAL, skor_peluang REAL, collected TEXT
        );
        CREATE TABLE IF NOT EXISTS telemetry_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT, label TEXT, trends_keyword TEXT,
            demand_score REAL, supply_count INTEGER, gap_score REAL,
            avg_harga REAL, median_terjual REAL, total_revenue_est REAL,
            pareto_index REAL, herfindahl_index REAL, official_ratio REAL,
            avg_love_ratio REAL, top_lokasi TEXT, geo_arbitrage_score REAL,
            opportunity_score REAL, collected TEXT
        );
    """)
    conn.commit()
    return conn


# ══════════════════════════════════════════════════════════════════
# DATA CONTRACTS
# ══════════════════════════════════════════════════════════════════

@dataclass
class Produk:
    keyword: str
    nama: str
    harga_min: float
    harga_max: float
    terjual: int
    rating: float
    jumlah_review: int
    stok: int
    disukai: int
    nama_toko: str
    lokasi_toko: str
    official: bool
    url: str
    estimasi_revenue: float = 0.0
    skor_peluang: float = 0.0

    def __post_init__(self):
        self.estimasi_revenue = round(self.terjual * self.harga_min, 0)


@dataclass
class TelemetryResult:
    keyword: str
    label: str
    trends_keyword: str
    demand_score: float
    supply_count: int
    gap_score: float
    avg_harga: float
    median_terjual: float
    total_revenue_est: float
    pareto_index: float
    herfindahl_index: float
    official_ratio: float
    avg_love_ratio: float
    top_lokasi: str
    geo_arbitrage_score: float
    opportunity_score: float


# ══════════════════════════════════════════════════════════════════
# STEP 1: SEED GOOGLE TRENDS
# ══════════════════════════════════════════════════════════════════

def seed_trends(conn: sqlite3.Connection):
    print("\n[1/4] Menyimpan data Google Trends...")
    now = datetime.now().isoformat()
    # Hapus data lama agar tidak duplikat
    conn.execute("DELETE FROM trends_regional")
    conn.execute("DELETE FROM trends_time")
    for keyword, regions in TRENDS_REGIONAL_DATA.items():
        for provinsi, interest in regions.items():
            conn.execute(
                "INSERT INTO trends_regional (keyword,provinsi,interest,collected) VALUES (?,?,?,?)",
                (keyword, provinsi, interest, now)
            )
    for keyword, series in TRENDS_TIME_DATA.items():
        for i, interest in enumerate(series):
            conn.execute(
                "INSERT INTO trends_time (keyword,minggu_ke,interest,collected) VALUES (?,?,?,?)",
                (keyword, i + 1, interest, now)
            )
    conn.commit()
    print(f"  ✓ {sum(len(v) for v in TRENDS_REGIONAL_DATA.values())} data regional")
    print(f"  ✓ {sum(len(v) for v in TRENDS_TIME_DATA.values())} data time-series")


# ══════════════════════════════════════════════════════════════════
# STEP 2: COLLECT SHOPEE
# ══════════════════════════════════════════════════════════════════

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
]

def _headers():
    return {
        "User-Agent":       random.choice(_USER_AGENTS),
        "Accept":           "application/json",
        "Accept-Language":  "id-ID,id;q=0.9",
        "Referer":          "https://shopee.co.id/",
        "Origin":           "https://shopee.co.id",
        "X-Api-Source":     "pc",
        "X-Requested-With": "XMLHttpRequest",
    }

def _parse(keyword: str, raw: dict) -> Optional[Produk]:
    info   = raw.get("item_basic", raw)
    rating = info.get("item_rating", {})
    iid    = info.get("itemid", 0)
    sid    = info.get("shopid", 0)
    if not iid or not sid:
        return None
    harga_min = info.get("price", 0) / 100000
    harga_max = info.get("price_max", info.get("price", 0)) / 100000
    return Produk(
        keyword=keyword, nama=info.get("name", "")[:80],
        harga_min=harga_min, harga_max=harga_max,
        terjual=info.get("sold", 0),
        rating=rating.get("rating_star", 0.0),
        jumlah_review=sum(rating.get("rating_count", [0] * 6)),
        stok=info.get("stock", 0),
        disukai=info.get("liked_count", 0),
        nama_toko=info.get("shop_name", ""),
        lokasi_toko=info.get("shop_location", ""),
        official=bool(info.get("is_official_shop", False)),
        url=f"https://shopee.co.id/product/{sid}/{iid}",
    )

def collect_shopee(keyword: str, limit: int = 60, retries: int = 3) -> list[Produk]:
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
        for attempt in range(retries):
            wait = random.uniform(0.8, 1.5) * (2 ** attempt)
            time.sleep(wait)
            try:
                resp = session.get(
                    "https://shopee.co.id/api/v4/search/search_items",
                    params=params, headers=_headers(), timeout=15,
                )
                resp.raise_for_status()
                data  = resp.json()
                items = data.get("items", [])
                for raw in items:
                    p = _parse(keyword, raw)
                    if p:
                        hasil.append(p)
                if len(items) < batch:
                    return hasil
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403 and attempt < retries - 1:
                    print(f"\n    [!] 403 Forbidden — retry {attempt+1}/{retries}...", end="")
                    continue
                print(f"\n    [!] HTTP Error: {e}")
                return hasil
            except Exception as e:
                print(f"\n    [!] {e}")
                if attempt == retries - 1:
                    return hasil
    return hasil

def save_shopee(conn: sqlite3.Connection, keyword: str, produk_list: list[Produk]):
    # Hapus data lama untuk keyword ini
    conn.execute("DELETE FROM shopee_products WHERE keyword = ?", (keyword,))
    now = datetime.now().isoformat()
    for p in produk_list:
        conn.execute("""
            INSERT INTO shopee_products
            (keyword,nama,harga_min,harga_max,terjual,rating,jumlah_review,
             stok,disukai,nama_toko,lokasi_toko,official,url,
             estimasi_revenue,skor_peluang,collected)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (p.keyword, p.nama, p.harga_min, p.harga_max, p.terjual, p.rating,
              p.jumlah_review, p.stok, p.disukai, p.nama_toko, p.lokasi_toko,
              int(p.official), p.url, p.estimasi_revenue, p.skor_peluang, now))
    conn.commit()

def step_collect(conn: sqlite3.Connection, limit: int = 60):
    print("\n[2/4] Mengambil data produk dari Shopee...")
    keywords = load_keywords()
    total    = 0
    for kw in keywords:
        print(f"  Mencari \"{kw['shopee']}\"... ", end="", flush=True)
        produk = collect_shopee(kw["shopee"], limit)
        if produk:
            save_shopee(conn, kw["shopee"], produk)
            print(f"{len(produk)} produk")
            total += len(produk)
        else:
            print("tidak ada hasil / cek koneksi internet")
    print(f"  ✓ Total {total} produk tersimpan")


# ══════════════════════════════════════════════════════════════════
# STEP 2B: DEMO (data simulasi)
# ══════════════════════════════════════════════════════════════════

def generate_demo(conn: sqlite3.Connection):
    print("\n[2/4] Mode DEMO — membuat data simulasi...")
    demo_cfg = {
        "lampu led":          {"n": 45, "harga": (15000, 85000),   "sold": (500, 15000),
                               "rating": (4.5, 4.9), "official": 0.15,
                               "lokasi": ["Kalimantan Selatan"]*8+["DKI Jakarta"]*12+["Jawa Barat"]*10+["Jawa Timur"]*8+["Banten"]*7},
        "sepatu import":      {"n": 52, "harga": (80000, 450000),  "sold": (200, 8000),
                               "rating": (4.3, 4.8), "official": 0.25,
                               "lokasi": ["DKI Jakarta"]*20+["Jawa Barat"]*15+["Jawa Timur"]*10+["Banten"]*7},
        "skincare viral":     {"n": 38, "harga": (35000, 280000),  "sold": (300, 12000),
                               "rating": (4.6, 5.0), "official": 0.35,
                               "lokasi": ["DKI Jakarta"]*18+["Banten"]*10+["Jawa Barat"]*5+["Bali"]*5},
        "baju import wanita": {"n": 60, "harga": (45000, 350000),  "sold": (100, 5000),
                               "rating": (4.2, 4.8), "official": 0.10,
                               "lokasi": ["DKI Jakarta"]*15+["Jawa Barat"]*15+["Jawa Tengah"]*10+["Jawa Timur"]*10+["Banten"]*10},
        "powerbank":          {"n": 40, "harga": (85000, 650000),  "sold": (1000, 25000),
                               "rating": (4.4, 4.9), "official": 0.45,
                               "lokasi": ["DKI Jakarta"]*20+["Jawa Barat"]*10+["Banten"]*5+["Jawa Timur"]*5},
        "tas import":         {"n": 35, "harga": (95000, 850000),  "sold": (50, 3000),
                               "rating": (4.3, 4.7), "official": 0.20,
                               "lokasi": ["DKI Jakarta"]*18+["Jawa Barat"]*10+["Banten"]*4+["Jawa Timur"]*3},
    }
    # Tambahkan keyword custom yang belum ada di demo_cfg
    keywords = load_keywords()
    now  = datetime.now().isoformat()
    total = 0
    for kw in keywords:
        key = kw["shopee"]
        cfg = demo_cfg.get(key, {
            "n": 20, "harga": (50000, 300000), "sold": (100, 5000),
            "rating": (4.3, 4.8), "official": 0.15,
            "lokasi": ["DKI Jakarta"]*10+["Jawa Barat"]*5+["Jawa Timur"]*5,
        })
        conn.execute("DELETE FROM shopee_products WHERE keyword = ?", (key,))
        for j in range(cfg["n"]):
            harga   = random.uniform(*cfg["harga"])
            sold    = max(int(random.uniform(*cfg["sold"]) * (1 - j * 0.01)), 1)
            official = random.random() < cfg["official"]
            disukai  = int(sold * random.uniform(0.3, 1.8))
            lokasi   = random.choice(cfg["lokasi"])
            conn.execute("""
                INSERT INTO shopee_products
                (keyword,nama,harga_min,harga_max,terjual,rating,jumlah_review,
                 stok,disukai,nama_toko,lokasi_toko,official,url,
                 estimasi_revenue,skor_peluang,collected)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                key,
                f"{'[OFFICIAL] ' if official else ''}{kw['label']} #{j+1}",
                round(harga, 0), round(harga * random.uniform(1.0, 1.3), 0),
                sold, round(random.uniform(*cfg["rating"]), 1),
                int(sold * random.uniform(0.4, 0.9)),
                int(sold * random.uniform(0.1, 2.0)), disukai,
                f"Toko{key[:8].replace(' ','').title()}{j+1}",
                lokasi, int(official),
                f"https://shopee.co.id/product/demo/{j+1}",
                round(sold * harga, 0), 0.0, now,
            ))
        total += cfg["n"]
        print(f"  ✓ \"{key}\": {cfg['n']} produk demo")
    conn.commit()
    print(f"  ✓ Total {total} produk demo tersimpan")


# ══════════════════════════════════════════════════════════════════
# STEP 3: TELEMETRY ENGINE
# ══════════════════════════════════════════════════════════════════

def _median(values: list) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    return (s[n // 2] + s[(n - 1) // 2]) / 2

def _pareto(revenues: list) -> float:
    if not revenues:
        return 0.0
    s = sorted(revenues, reverse=True)
    top20 = s[:max(1, len(s) // 5)]
    return round(sum(top20) / max(sum(s), 1) * 100, 1)

def _herfindahl(revenues: list) -> float:
    total = sum(revenues)
    if not total:
        return 0.0
    return round(sum((r / total) ** 2 for r in revenues), 4)

def _geo_arbitrage(trends_keyword: str, lokasi_list: list) -> tuple[float, str]:
    regional = TRENDS_REGIONAL_DATA.get(trends_keyword, {})
    if not regional or not lokasi_list:
        return 0.0, "-"
    top_demand = {p for p, s in sorted(regional.items(), key=lambda x: x[1], reverse=True)[:5] if s > 50}
    supply_count: dict[str, int] = {}
    for lok in lokasi_list:
        if lok:
            supply_count[lok] = supply_count.get(lok, 0) + 1
    top_supply_sorted = sorted(supply_count.items(), key=lambda x: x[1], reverse=True)
    top_lokasi        = top_supply_sorted[0][0] if top_supply_sorted else "-"
    top_supply        = {p for p, _ in top_supply_sorted[:5]}
    gap               = top_demand - top_supply
    return round(len(gap) / max(len(top_demand), 1) * 100, 1), top_lokasi

def compute_telemetry(conn: sqlite3.Connection, kw: dict) -> Optional[TelemetryResult]:
    rows = conn.execute("""
        SELECT * FROM shopee_products WHERE keyword = ?
        ORDER BY collected DESC LIMIT 200
    """, (kw["shopee"],)).fetchall()
    if not rows:
        return None

    ts = conn.execute("SELECT AVG(interest) FROM trends_time WHERE keyword = ?",
                      (kw["trends"],)).fetchone()
    demand_score = round(ts[0] or 0, 1)

    revenues    = [r["estimasi_revenue"] for r in rows]
    harga_all   = [r["harga_min"] for r in rows if r["harga_min"] > 0]
    terjual_all = [r["terjual"] for r in rows]
    lokasi_all  = [r["lokasi_toko"] for r in rows]
    disukai_all = [r["disukai"] for r in rows]

    supply_count   = len(rows)
    avg_harga      = round(sum(harga_all) / max(len(harga_all), 1), 0)
    median_terjual = _median(terjual_all)
    total_rev      = sum(revenues)
    pareto         = _pareto(revenues)
    herfindahl     = _herfindahl(revenues)
    official_ratio = round(sum(1 for r in rows if r["official"]) / supply_count * 100, 1)
    love_ratios    = [d / max(t, 1) for d, t in zip(disukai_all, terjual_all)]
    avg_love       = round(sum(love_ratios) / max(len(love_ratios), 1), 3)
    geo_score, top_lokasi = _geo_arbitrage(kw["trends"], lokasi_all)

    normalized_demand  = demand_score / 100
    normalized_supply  = min(supply_count / 100, 1.0)
    gap_score          = round(max(0.0, (normalized_demand - normalized_supply * 0.5) * 100), 1)
    low_comp           = max(0.0, 100 - (herfindahl * 100) * 2)
    opp_score          = round(
        demand_score * 0.30 + gap_score * 0.25 +
        geo_score * 0.20 + min(avg_love * 200, 100) * 0.15 + low_comp * 0.10, 1
    )
    return TelemetryResult(
        keyword=kw["shopee"], label=kw["label"], trends_keyword=kw["trends"],
        demand_score=demand_score, supply_count=supply_count, gap_score=gap_score,
        avg_harga=avg_harga, median_terjual=median_terjual, total_revenue_est=total_rev,
        pareto_index=pareto, herfindahl_index=herfindahl, official_ratio=official_ratio,
        avg_love_ratio=avg_love, top_lokasi=top_lokasi,
        geo_arbitrage_score=geo_score, opportunity_score=opp_score,
    )

def save_telemetry(conn: sqlite3.Connection, t: TelemetryResult):
    conn.execute("""
        INSERT INTO telemetry_snapshots
        (keyword,label,trends_keyword,demand_score,supply_count,gap_score,
         avg_harga,median_terjual,total_revenue_est,pareto_index,herfindahl_index,
         official_ratio,avg_love_ratio,top_lokasi,geo_arbitrage_score,opportunity_score,collected)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        t.keyword, t.label, t.trends_keyword, t.demand_score, t.supply_count,
        t.gap_score, t.avg_harga, t.median_terjual, t.total_revenue_est,
        t.pareto_index, t.herfindahl_index, t.official_ratio, t.avg_love_ratio,
        t.top_lokasi, t.geo_arbitrage_score, t.opportunity_score,
        datetime.now().isoformat(),
    ))
    conn.commit()

def step_analyze(conn: sqlite3.Connection) -> list[TelemetryResult]:
    print("\n[3/4] Menghitung telemetri...")
    results  = []
    keywords = load_keywords()
    for kw in keywords:
        t = compute_telemetry(conn, kw)
        if t:
            save_telemetry(conn, t)
            results.append(t)
            print(f"  ✓ {t.label}: skor={t.opportunity_score} | gap={t.gap_score} | demand={t.demand_score}")
        else:
            print(f"  - {kw['label']}: belum ada data Shopee")
    return results


# ══════════════════════════════════════════════════════════════════
# STEP 4: LAPORAN
# ══════════════════════════════════════════════════════════════════

def _bar(value: float, max_val: float = 100, width: int = 20) -> str:
    filled = int((value / max(max_val, 1)) * width)
    return "█" * filled + "░" * (width - filled)

def _label(score: float) -> str:
    if score >= 75:   return "SANGAT BAIK ***"
    elif score >= 55: return "BAIK **"
    elif score >= 35: return "SEDANG *"
    return "RENDAH"

def _rp(nilai: float) -> str:
    if nilai >= 1_000_000_000: return f"Rp{nilai/1e9:.1f}M"
    elif nilai >= 1_000_000:   return f"Rp{nilai/1e6:.1f}jt"
    elif nilai >= 1_000:       return f"Rp{nilai/1e3:.0f}rb"
    return f"Rp{nilai:.0f}"

# Deteksi lebar terminal — penting untuk Termux di HP
W = min(shutil.get_terminal_size((80, 24)).columns, 90)

def _sep(char="═"): return char * W
def _sep2(char="─"): return char * W

def generate_report(results: list[TelemetryResult]):
    print("\n[4/4] Membuat laporan...")
    now_str = datetime.now().strftime("%d %B %Y, %H:%M")
    print(f"\n\n{_sep()}")
    print(f"  MARKET TELEMETRY REPORT")
    print(f"  China Supplier x Indonesia — {now_str}")
    print(_sep())

    if not results:
        print("\n  Belum ada data. Jalankan: python telemetry_pipeline.py run-demo")
        return

    sorted_r = sorted(results, key=lambda x: x.opportunity_score, reverse=True)

    # ── RANKING ──────────────────────────────────────────────
    print(f"\n  {'RANKING PELUANG':^{W-4}}")
    print(_sep2())
    for i, t in enumerate(sorted_r, 1):
        bar  = _bar(t.opportunity_score, width=15)
        aksi = "→ MASUK" if t.gap_score > 40 else ("→ MONITOR" if t.gap_score > 20 else "→ KOMPETITIF")
        print(f"  {i}. {t.label:<24} {t.opportunity_score:>5.1f}  {bar}  {aksi}")

    # ── DETAIL ───────────────────────────────────────────────
    print(f"\n{_sep()}")
    print(f"  DETAIL PER KATEGORI")
    print(_sep())

    for t in sorted_r:
        hhi_label = ("Sangat Kompetitif" if t.herfindahl_index < 0.1
                     else "Kompetitif" if t.herfindahl_index < 0.25
                     else "Oligopoli")
        love_label = "Viral Potential ✓" if t.avg_love_ratio > 0.5 else "Normal"
        print(f"\n  ┌ {t.label.upper()} — Skor {t.opportunity_score}/100 [{_label(t.opportunity_score)}]")
        print(f"  │ Demand (Trends)  : {_bar(t.demand_score)} {t.demand_score}/100")
        print(f"  │ Supply (Shopee)  : {t.supply_count} produk | {_rp(t.avg_harga)} rata-rata")
        print(f"  │ Revenue est.     : {_rp(t.total_revenue_est)} total | median terjual {t.median_terjual:.0f}")
        print(f"  │ Kompetisi        : HHI={t.herfindahl_index:.4f} ({hhi_label})")
        print(f"  │ Official ratio   : {t.official_ratio:.0f}% | Pareto: top20% = {t.pareto_index:.0f}% revenue")
        print(f"  │ Love ratio       : {t.avg_love_ratio:.3f} ({love_label})")
        print(f"  │ Geo arbitrage    : {t.geo_arbitrage_score:.0f}% demand belum terlayani")
        # Top demand provinsi
        regional = TRENDS_REGIONAL_DATA.get(t.trends_keyword, {})
        top = sorted(regional.items(), key=lambda x: x[1], reverse=True)[:3]
        prov_str = " | ".join(f"{p}({s})" for p, s in top if s > 0)
        print(f"  │ Top demand       : {prov_str}")
        print(f"  └ Top seller dari  : {t.top_lokasi}")

    # ── GAP MATRIX ───────────────────────────────────────────
    print(f"\n{_sep()}")
    print(f"  DEMAND vs SUPPLY GAP")
    print(_sep2())
    for t in sorted_r:
        bar_d = _bar(t.demand_score, width=10)
        bar_s = _bar(min(t.supply_count, 100), width=10)
        print(f"  {t.label:<24} D:{bar_d} S:{bar_s} GAP={t.gap_score:>5.1f}")

    # ── REKOMENDASI ──────────────────────────────────────────
    print(f"\n{_sep()}")
    print(f"  REKOMENDASI STRATEGIS")
    print(_sep2())
    for i, t in enumerate(sorted_r[:3], 1):
        aksi = ("MASUK SEKARANG" if t.gap_score > 40
                else "MULAI PERSIAPAN" if t.gap_score > 20 else "DIVERSIFIKASI")
        print(f"\n  #{i} {t.label} — {aksi}")
        print(f"      Skor {t.opportunity_score}/100 | Demand {t.demand_score}/100 | Geo {t.geo_arbitrage_score:.0f}%")
        regional = TRENDS_REGIONAL_DATA.get(t.trends_keyword, {})
        top_prov = sorted(regional.items(), key=lambda x: x[1], reverse=True)[:2]
        provs    = ", ".join(p for p, s in top_prov if s > 50)
        if provs:
            print(f"      Target wilayah: {provs}")

    # ── KALENDER ─────────────────────────────────────────────
    print(f"\n{_sep()}")
    print(f"  KALENDER OPTIMAL")
    print(_sep2())
    calendar = [
        ("Jan-Feb",  "Fashion (Imlek peak 100)", "***"),
        ("Mar-Apr",  "Lampu & Sepatu stabil",    "**"),
        ("Mei-Jun",  "Sepatu(100) Powerbank(100)","***"),
        ("Agustus",  "Skincare naik, Agen china", "**"),
        ("Okt-Nov",  "SEMUA — Harbolnas 11.11",  "***"),
        ("Desember", "Lampu(99) Skincare(85)",   "**"),
    ]
    for bln, desc, bintang in calendar:
        print(f"  {bintang}  {bln:<10} {desc}")

    print(f"\n{_sep()}")
    print(f"  Jalankan tiap minggu untuk lihat perubahan tren.")
    print(f"  Simpan: python telemetry_pipeline.py report > laporan.txt")
    print(_sep())


# ══════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Market Intelligence Telemetry Pipeline v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Contoh:
  python telemetry_pipeline.py run-demo
  python telemetry_pipeline.py run
  python telemetry_pipeline.py add "topi import" "topi china" "Topi Import"
  python telemetry_pipeline.py search "kaos polos"
  python telemetry_pipeline.py report > laporan.txt
        """
    )
    sub = parser.add_subparsers(dest="command")

    # run / run-demo / report / seed
    sub.add_parser("run",      help="Data nyata dari Shopee + laporan")
    sub.add_parser("run-demo", help="Data simulasi (tanpa internet Shopee) + laporan")
    sub.add_parser("report",   help="Tampilkan laporan dari data terakhir")
    sub.add_parser("seed",     help="Muat ulang data Google Trends")

    # collect
    p_col = sub.add_parser("collect", help="Ambil data Shopee saja")
    p_col.add_argument("--limit", type=int, default=60)

    # search — cari satu keyword langsung
    p_search = sub.add_parser("search", help="Cari satu keyword di Shopee sekarang")
    p_search.add_argument("keyword", help="Keyword pencarian")
    p_search.add_argument("--limit", type=int, default=30)

    # add — tambah keyword baru
    p_add = sub.add_parser("add", help="Tambah keyword baru ke daftar")
    p_add.add_argument("shopee",  help="Keyword untuk dicari di Shopee")
    p_add.add_argument("trends",  help="Keyword untuk data Trends (misal: 'lampu china')")
    p_add.add_argument("label",   help="Nama tampilan (misal: 'Lampu Import')")

    # keywords — tampilkan daftar
    sub.add_parser("keywords", help="Tampilkan semua keyword aktif")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    conn = init_db()

    if args.command == "seed":
        seed_trends(conn)

    elif args.command == "collect":
        step_collect(conn, args.limit)

    elif args.command == "keywords":
        kws = load_keywords()
        print(f"\n  {len(kws)} keyword aktif (dari {KEYWORDS_FILE}):\n")
        for i, k in enumerate(kws, 1):
            print(f"  {i:>2}. [{k['label']}]")
            print(f"      shopee : {k['shopee']}")
            print(f"      trends : {k['trends']}")
        print()

    elif args.command == "add":
        add_keyword(args.shopee, args.trends, args.label)
        print(f"  File {KEYWORDS_FILE} diperbarui.")
        print(f"  Jalankan 'run-demo' atau 'run' untuk analisis terbaru.")

    elif args.command == "search":
        print(f"\n  Mencari \"{args.keyword}\"... ", end="", flush=True)
        produk = collect_shopee(args.keyword, args.limit)
        if not produk:
            print("tidak ada hasil / cek koneksi internet")
            return
        print(f"{len(produk)} produk ditemukan\n")
        print(f"  {'#':<3} {'NAMA':<35} {'HARGA':>12} {'TERJUAL':>8} {'REV EST':>10} {'⭐':>4}")
        print("  " + "─" * 74)
        for i, p in enumerate(sorted(produk, key=lambda x: x.estimasi_revenue, reverse=True)[:20], 1):
            print(f"  {i:<3} {p.nama[:35]:<35} {_rp(p.harga_min):>12} {p.terjual:>8,} {_rp(p.estimasi_revenue):>10} {p.rating:>4.1f}")
        print(f"\n  Total: {len(produk)} produk | Est. Revenue: {_rp(sum(p.estimasi_revenue for p in produk))}")

    elif args.command == "report":
        results = step_analyze(conn)
        generate_report(results)

    elif args.command == "run":
        print("\n  MODE: Data NYATA dari Shopee")
        seed_trends(conn)
        step_collect(conn, 60)
        results = step_analyze(conn)
        generate_report(results)

    elif args.command == "run-demo":
        print("\n  MODE: Data SIMULASI")
        seed_trends(conn)
        generate_demo(conn)
        results = step_analyze(conn)
        generate_report(results)

    conn.close()

if __name__ == "__main__":
    main()
