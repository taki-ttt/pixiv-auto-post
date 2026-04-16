#!/usr/bin/env python3
"""
統計収集スクリプト  collect_stats.py
======================================
毎日深夜 2 時に GitHub Actions から呼び出される。
metadata.csv の投稿済みエントリのうち、投稿後 48 時間以上経過したものの
Pixiv 統計 (views / bookmarks / likes) を取得し performance_log.csv に記録する。
"""

import csv
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pixivpy3 import AppPixivAPI

# ──────────────────────────────────────────────
#  定数
# ──────────────────────────────────────────────
JST            = timezone(timedelta(hours=9))
ROOT           = Path(__file__).parent
METADATA_PATH  = ROOT / "metadata.csv"
PERF_LOG_PATH  = ROOT / "performance_log.csv"

PERF_FIELDS = [
    "post_id", "pixiv_id", "posted_at", "tags",
    "image_count", "views_48h", "bookmarks_48h",
    "likes_48h", "bookmark_rate",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
#  CSV ユーティリティ
# ──────────────────────────────────────────────
def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def append_csv(path: Path, row: dict) -> None:
    exists = path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PERF_FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def already_logged(perf_rows: list[dict], post_id: str) -> bool:
    return any(r["post_id"] == post_id for r in perf_rows)


# ──────────────────────────────────────────────
#  Pixiv 統計取得
# ──────────────────────────────────────────────
def auth_pixiv() -> AppPixivAPI:
    api = AppPixivAPI()
    api.auth(refresh_token=os.environ["PIXIV_REFRESH_TOKEN"])
    return api


def fetch_stats(api: AppPixivAPI, illust_id: str) -> dict | None:
    """Pixiv API からイラスト統計を取得する"""
    try:
        result = api.illust_detail(int(illust_id))
        illust = result.get("illust")
        if not illust:
            log.warning("illust_id=%s: データなし", illust_id)
            return None
        return {
            "views":     illust.get("total_view",      0),
            "bookmarks": illust.get("total_bookmarks", 0),
            "likes":     illust.get("total_comments",  0),  # likes は API 制限のためコメント数で代替
        }
    except Exception as e:
        log.error("統計取得エラー (id=%s): %s", illust_id, e)
        return None


# ──────────────────────────────────────────────
#  メイン
# ──────────────────────────────────────────────
def main() -> None:
    cfg_path = ROOT / "config.json"
    with open(cfg_path, encoding="utf-8") as f:
        cfg = json.load(f)

    collect_after_hours = cfg.get("stats_collect_hours_after", 48)
    threshold = datetime.now(JST) - timedelta(hours=collect_after_hours)

    metadata  = read_csv(METADATA_PATH)
    perf_rows = read_csv(PERF_LOG_PATH)

    # 収集対象: posted=yes かつ 48h+ 経過 かつ未ログ
    targets = []
    for row in metadata:
        if row.get("posted", "no").lower() != "yes":
            continue
        if not row.get("pixiv_id") or row["pixiv_id"] in ("dry_run", "unknown", ""):
            continue
        if already_logged(perf_rows, row["post_id"]):
            continue
        try:
            posted_dt = datetime.strptime(row["posted_at"], "%Y-%m-%d %H:%M").replace(tzinfo=JST)
        except (ValueError, KeyError):
            continue
        if posted_dt <= threshold:
            targets.append(row)

    if not targets:
        log.info("統計収集対象なし。")
        return

    log.info("収集対象: %d 件", len(targets))
    api = auth_pixiv()

    for row in targets:
        post_id   = row["post_id"]
        pixiv_id  = row["pixiv_id"]
        log.info("取得中: %s (pixiv_id=%s)", post_id, pixiv_id)

        stats = fetch_stats(api, pixiv_id)
        if not stats:
            continue

        views     = stats["views"]
        bookmarks = stats["bookmarks"]
        likes     = stats["likes"]
        bm_rate   = round(bookmarks / views, 4) if views > 0 else 0.0

        perf_row = {
            "post_id":       post_id,
            "pixiv_id":      pixiv_id,
            "posted_at":     row.get("posted_at", ""),
            "tags":          row.get("tags", ""),
            "image_count":   row.get("image_count", "1"),
            "views_48h":     views,
            "bookmarks_48h": bookmarks,
            "likes_48h":     likes,
            "bookmark_rate": bm_rate,
        }
        append_csv(PERF_LOG_PATH, perf_row)
        log.info(
            "  ✅ views=%d  bookmarks=%d  bookmark_rate=%.4f",
            views, bookmarks, bm_rate,
        )

    log.info("統計収集完了。performance_log.csv を更新しました。")


if __name__ == "__main__":
    main()
