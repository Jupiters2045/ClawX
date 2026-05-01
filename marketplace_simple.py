#!/usr/bin/env python3
"""
Marketplace Intelligence — Versi Ringan
Hanya butuh: requests (sudah terinstall)
Usage:
  python3 marketplace_simple.py search "lampu led"
  python3 marketplace_simple.py search "sepatu import" --limit 50
  python3 marketplace_simple.py search "skincare china" --output hasil.csv
"""

import argparse
import csv
import json
import random
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime

import requests

# ──────────────────────────────────────────────
# DATA SCHEMA
# ──────────────────────────────────────────────

@dataclass
class Produk:
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


# ──────────────────────────────────────────────
# SHOPEE CONNECTOR
# ──────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

def buat_headers():
    return {
        "User-Agent":       random.choice(USER_AGENTS),
        "Accept":           "application/json",
        "Accept-Language":  "id-ID,id;q=0.9",
        "Referer":          "https://shopee.co.id/",
        "Origin":           "https://shopee.co.id",
        "X-Api-Source":     "pc",
        "X-Requested-With": "XMLHttpRequest",
    }

def parse_produk(raw: dict):
    info   = raw.get("item_basic", raw)
    rating = info.get("item_rating", {})
    iid    = info.get("itemid", 0)
    sid    = info.get("shopid", 0)
    if not iid or not sid:
        return None

    harga_min = info.get("price", 0) / 100000
    harga_max = info.get("price_max", info.get("price", 0)) / 100000

    return Produk(
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

def cari_shopee(keyword: str, limit: int = 30) -> list[Produk]:
    hasil   = []
    session = requests.Session()

    for offset in range(0, limit, 60):
        batch  = min(60, limit - offset)
        params = {
            "by":        "sales",
            "keyword":   keyword,
            "limit":     batch,
            "newest":    offset,
            "order":     "desc",
            "page_type": "search",
            "scenario":  "PAGE_GLOBAL_SEARCH",
            "version":   2,
            "matchtype": 0,
        }
        time.sleep(random.uniform(0.5, 1.2))
        try:
            resp = session.get(
                "https://shopee.co.id/api/v4/search/search_items",
                params=params,
                headers=buat_headers(),
                timeout=12,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [ERROR] {e}")
            break

        items = data.get("items", [])
        for raw in items:
            p = parse_produk(raw)
            if p:
                hasil.append(p)
        if len(items) < batch:
            break

    return hasil


# ──────────────────────────────────────────────
# SCORING
# ──────────────────────────────────────────────

def hitung_skor(produk_list: list[Produk]) -> list[Produk]:
    if not produk_list:
        return produk_list

    max_rev     = max(p.estimasi_revenue for p in produk_list) or 1
    num_sellers = len(set(p.nama_toko for p in produk_list))

    for p in produk_list:
        rev_score    = (p.estimasi_revenue / max_rev) * 40
        komp_score   = max(0.0, 25 - (num_sellers / len(produk_list)) * 25)
        rating_score = (p.rating / 5.0) * 20 if p.rating > 0 else 0
        like_ratio   = min((p.disukai / (p.terjual or 1)), 1.0)
        growth_score = like_ratio * 15
        p.skor_peluang = round(rev_score + komp_score + rating_score + growth_score, 1)

    return sorted(produk_list, key=lambda x: x.skor_peluang, reverse=True)


# ──────────────────────────────────────────────
# DISPLAY
# ──────────────────────────────────────────────

def format_rupiah(nilai: float) -> str:
    if nilai >= 1_000_000_000:
        return f"Rp {nilai/1e9:.1f}M"
    elif nilai >= 1_000_000:
        return f"Rp {nilai/1e6:.1f}jt"
    elif nilai >= 1_000:
        return f"Rp {nilai/1e3:.0f}rb"
    return f"Rp {nilai:.0f}"

def tampilkan(produk_list: list[Produk], keyword: str):
    print()
    print("=" * 110)
    print(f"  HASIL PENCARIAN: \"{keyword}\"  |  {len(produk_list)} produk ditemukan  |  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 110)

    header = f"{'#':>3}  {'NAMA PRODUK':<40}  {'HARGA':>14}  {'TERJUAL':>8}  {'REV EST':>10}  {'⭐':>4}  {'SKOR':>6}  {'LOKASI':<15}"
    print(header)
    print("-" * 110)

    total_rev = 0
    sellers   = set()

    for i, p in enumerate(produk_list[:25], 1):
        harga_str = (f"Rp {p.harga_min:,.0f}" if p.harga_min == p.harga_max
                     else f"Rp {p.harga_min:,.0f}-{p.harga_max:,.0f}")
        nama      = (p.nama[:38] + "..") if len(p.nama) > 40 else p.nama
        official  = " [OFFICIAL]" if p.official else ""
        skor_mark = "***" if p.skor_peluang >= 70 else ("**" if p.skor_peluang >= 50 else "")

        print(
            f"{i:>3}  {nama + official:<40}  {harga_str:>14}  "
            f"{p.terjual:>8,}  {format_rupiah(p.estimasi_revenue):>10}  "
            f"{p.rating:>4.1f}  {p.skor_peluang:>5.1f}{skor_mark:<2}  "
            f"{p.lokasi_toko:<15}"
        )
        total_rev += p.estimasi_revenue
        sellers.add(p.nama_toko)

    print("=" * 110)
    print(f"  Total produk: {len(produk_list)}  |  Seller unik: {len(sellers)}  |  Est. Revenue gabungan: {format_rupiah(total_rev)}")
    print(f"  Keterangan Skor: *** >= 70 (Sangat Baik)  ** >= 50 (Baik)  < 50 (Biasa)")
    print("=" * 110)
    print()


# ──────────────────────────────────────────────
# EXPORT CSV
# ──────────────────────────────────────────────

def export_csv(produk_list: list[Produk], filename: str):
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=asdict(produk_list[0]).keys())
        writer.writeheader()
        for p in produk_list:
            writer.writerows([asdict(p)])
    print(f"  Tersimpan → {filename}  ({len(produk_list)} baris)")


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Marketplace Intelligence — Shopee Product Research"
    )
    sub = parser.add_subparsers(dest="command")

    # search command
    search_p = sub.add_parser("search", help="Cari produk di Shopee")
    search_p.add_argument("keyword", help="Keyword produk, contoh: 'lampu led'")
    search_p.add_argument("--limit",  type=int, default=30, help="Jumlah produk (default: 30)")
    search_p.add_argument("--output", type=str, default=None, help="Simpan ke file CSV")

    args = parser.parse_args()

    if args.command == "search":
        print(f"\n  Mencari \"{args.keyword}\" di Shopee... ", end="", flush=True)
        produk = cari_shopee(args.keyword, args.limit)

        if not produk:
            print("\n  Tidak ada produk ditemukan. Coba keyword lain.")
            sys.exit(1)

        print(f"ditemukan {len(produk)} produk.")
        produk = hitung_skor(produk)
        tampilkan(produk, args.keyword)

        if args.output:
            export_csv(produk, args.output)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
