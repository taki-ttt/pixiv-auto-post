#!/usr/bin/env python3
"""
週次分析スクリプト  analyze_weekly.py
=======================================
毎週月曜 0 時に GitHub Actions から呼び出される。
performance_log.csv を集計して:
  1. HTML レポートを reports/ フォルダに保存
  2. 高スコアタグを templates.json の recommended_tags に追記
"""

import csv
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ──────────────────────────────────────────────
#  定数
# ──────────────────────────────────────────────
JST            = timezone(timedelta(hours=9))
ROOT           = Path(__file__).parent
PERF_LOG_PATH  = ROOT / "performance_log.csv"
TEMPLATES_PATH = ROOT / "templates.json"
REPORTS_DIR    = ROOT / "reports"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
#  データ読み込み
# ──────────────────────────────────────────────
def read_perf_log() -> list[dict]:
    if not PERF_LOG_PATH.exists():
        return []
    with open(PERF_LOG_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ──────────────────────────────────────────────
#  集計
# ──────────────────────────────────────────────
def safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def analyze(rows: list[dict]) -> dict:
    """performance_log の全行を集計してサマリ辞書を返す"""

    # タグ別集計
    tag_scores: dict[str, list[float]] = defaultdict(list)
    # 時間帯別集計 (0〜23 時)
    hour_views: dict[int, list[float]] = defaultdict(list)
    # 枚数別集計
    count_scores: dict[int, list[float]] = defaultdict(list)

    total_views     = 0.0
    total_bookmarks = 0.0
    ranked: list[dict] = []

    for r in rows:
        bm_rate     = safe_float(r.get("bookmark_rate"))
        views       = safe_float(r.get("views_48h"))
        bookmarks   = safe_float(r.get("bookmarks_48h"))
        img_count   = int(safe_float(r.get("image_count", 1)))
        total_views     += views
        total_bookmarks += bookmarks

        # タグ別
        raw_tags = r.get("tags", "")
        for tag in [t.strip() for t in raw_tags.split("|") if t.strip()]:
            tag_scores[tag].append(bm_rate)

        # 時間帯別
        posted_at = r.get("posted_at", "")
        try:
            hour = int(posted_at.split(" ")[1].split(":")[0])
            hour_views[hour].append(views)
        except (IndexError, ValueError):
            pass

        # 枚数別
        count_scores[img_count].append(bm_rate)

        ranked.append({
            "post_id":   r.get("post_id", ""),
            "pixiv_id":  r.get("pixiv_id", ""),
            "posted_at": posted_at,
            "views":     int(views),
            "bookmarks": int(bookmarks),
            "bm_rate":   bm_rate,
            "tags":      raw_tags,
        })

    # タグランキング (平均 bookmark_rate 降順、3件以上のみ対象)
    tag_avg = {
        tag: round(sum(scores) / len(scores), 4)
        for tag, scores in tag_scores.items()
        if len(scores) >= 2
    }
    tag_ranking = sorted(tag_avg.items(), key=lambda x: x[1], reverse=True)[:20]

    # 時間帯ランキング (平均 views 降順)
    hour_avg = {
        hour: round(sum(v) / len(v))
        for hour, v in hour_views.items()
    }
    hour_ranking = sorted(hour_avg.items(), key=lambda x: x[1], reverse=True)

    # 枚数別ランキング
    count_avg = {
        cnt: round(sum(s) / len(s), 4)
        for cnt, s in count_scores.items()
    }
    count_ranking = sorted(count_avg.items(), key=lambda x: x[0])

    # 投稿ランキング (bookmark_rate 降順 top10)
    top_posts = sorted(ranked, key=lambda x: x["bm_rate"], reverse=True)[:10]

    return {
        "total_posts":     len(rows),
        "total_views":     int(total_views),
        "total_bookmarks": int(total_bookmarks),
        "avg_bm_rate":     round(total_bookmarks / total_views, 4) if total_views else 0,
        "tag_ranking":     tag_ranking,
        "hour_ranking":    hour_ranking,
        "count_ranking":   count_ranking,
        "top_posts":       top_posts,
        "all_posts":       ranked,
    }


# ──────────────────────────────────────────────
#  templates.json 更新
# ──────────────────────────────────────────────
def update_recommended_tags(tag_ranking: list[tuple]) -> None:
    with open(TEMPLATES_PATH, encoding="utf-8") as f:
        tmpl = json.load(f)

    new_recs = [
        {"tag": tag, "avg_bookmark_rate": rate}
        for tag, rate in tag_ranking[:10]
    ]
    tmpl["recommended_tags"] = new_recs

    with open(TEMPLATES_PATH, "w", encoding="utf-8") as f:
        json.dump(tmpl, f, ensure_ascii=False, indent=2)

    log.info("templates.json の recommended_tags を更新しました")


# ──────────────────────────────────────────────
#  HTML レポート生成
# ──────────────────────────────────────────────
def generate_html(summary: dict, report_date: str) -> str:
    tag_rows = "".join(
        f"<tr><td>{i+1}</td><td>{tag}</td><td>{rate:.4f}</td></tr>"
        for i, (tag, rate) in enumerate(summary["tag_ranking"])
    )
    hour_rows = "".join(
        f"<tr><td>{h:02d}:00</td><td>{avg:,}</td></tr>"
        for h, avg in summary["hour_ranking"][:8]
    )
    count_rows = "".join(
        f"<tr><td>{cnt}枚</td><td>{rate:.4f}</td></tr>"
        for cnt, rate in summary["count_ranking"]
    )
    post_rows = "".join(
        f"""<tr>
          <td>{p["post_id"]}</td>
          <td>{p["posted_at"]}</td>
          <td>{p["views"]:,}</td>
          <td>{p["bookmarks"]:,}</td>
          <td>{p["bm_rate"]:.4f}</td>
          <td style="font-size:11px">{p["tags"].replace("|"," / ")}</td>
        </tr>"""
        for p in summary["top_posts"]
    )

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pixiv 投稿分析レポート {report_date}</title>
<style>
  body {{ font-family: "Helvetica Neue", Arial, sans-serif; margin: 0; background: #f5f5f5; color: #333; }}
  .container {{ max-width: 1000px; margin: 0 auto; padding: 24px; }}
  h1 {{ color: #1F4E79; font-size: 24px; border-bottom: 3px solid #1F4E79; padding-bottom: 8px; }}
  h2 {{ color: #2E75B6; font-size: 18px; margin-top: 32px; }}
  .cards {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 16px 0; }}
  .card {{ background: #fff; border-radius: 8px; padding: 16px 24px; box-shadow: 0 2px 6px rgba(0,0,0,.1); min-width: 160px; }}
  .card .num {{ font-size: 28px; font-weight: bold; color: #1F4E79; }}
  .card .lbl {{ font-size: 12px; color: #888; margin-top: 4px; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 6px rgba(0,0,0,.08); }}
  th {{ background: #1F4E79; color: #fff; padding: 10px 14px; text-align: left; font-size: 13px; }}
  td {{ padding: 8px 14px; font-size: 13px; border-bottom: 1px solid #eee; }}
  tr:hover td {{ background: #f0f7ff; }}
  .badge {{ background: #EBF3FB; color: #1F4E79; border-radius: 4px; padding: 2px 8px; font-size: 11px; }}
  footer {{ margin-top: 40px; text-align: center; font-size: 11px; color: #aaa; }}
</style>
</head>
<body>
<div class="container">
  <h1>📊 Pixiv 投稿分析レポート</h1>
  <p>生成日時: {report_date} &nbsp;|&nbsp; 対象投稿数: {summary["total_posts"]}件</p>

  <div class="cards">
    <div class="card"><div class="num">{summary["total_posts"]}</div><div class="lbl">総投稿数</div></div>
    <div class="card"><div class="num">{summary["total_views"]:,}</div><div class="lbl">総閲覧数</div></div>
    <div class="card"><div class="num">{summary["total_bookmarks"]:,}</div><div class="lbl">総ブックマーク</div></div>
    <div class="card"><div class="num">{summary["avg_bm_rate"]:.3f}</div><div class="lbl">平均ブックマーク率</div></div>
  </div>

  <h2>🏷️ タグ別平均ブックマーク率ランキング (上位20)</h2>
  <p style="font-size:12px;color:#888">※ 2件以上使用されたタグのみ集計。このタグを多く使った投稿の平均スコアです。</p>
  <table>
    <tr><th>#</th><th>タグ</th><th>平均ブックマーク率</th></tr>
    {tag_rows if tag_rows else "<tr><td colspan='3'>データ不足（各タグ2件以上必要）</td></tr>"}
  </table>

  <h2>⏰ 時間帯別平均閲覧数 (上位8)</h2>
  <table>
    <tr><th>投稿時刻</th><th>平均閲覧数 (48h)</th></tr>
    {hour_rows if hour_rows else "<tr><td colspan='2'>データなし</td></tr>"}
  </table>

  <h2>🖼️ 画像枚数別ブックマーク率</h2>
  <table>
    <tr><th>枚数</th><th>平均ブックマーク率</th></tr>
    {count_rows if count_rows else "<tr><td colspan='2'>データなし</td></tr>"}
  </table>

  <h2>🏆 パフォーマンス上位 10 投稿</h2>
  <table>
    <tr><th>投稿ID</th><th>投稿日時</th><th>閲覧数</th><th>BM数</th><th>BM率</th><th>タグ</th></tr>
    {post_rows if post_rows else "<tr><td colspan='6'>データなし</td></tr>"}
  </table>

  <footer>自動生成 by pixiv-auto-post / analyze_weekly.py</footer>
</div>
</body>
</html>"""


# ──────────────────────────────────────────────
#  メイン
# ──────────────────────────────────────────────
def main() -> None:
    rows = read_perf_log()

    if not rows:
        log.warning("performance_log.csv にデータがありません。レポートをスキップします。")
        return

    log.info("集計対象: %d 件", len(rows))
    summary = analyze(rows)

    # reports/ フォルダに HTML を保存
    REPORTS_DIR.mkdir(exist_ok=True)
    report_date = datetime.now(JST).strftime("%Y-%m-%d_%H%M")
    report_path = REPORTS_DIR / f"report_{report_date}.html"

    html = generate_html(summary, report_date.replace("_", " "))
    report_path.write_text(html, encoding="utf-8")
    log.info("レポート生成: %s", report_path)

    # 最新レポートを index.html としてもコピー
    (REPORTS_DIR / "index.html").write_text(html, encoding="utf-8")

    # templates.json の recommended_tags を更新
    if summary["tag_ranking"]:
        update_recommended_tags(summary["tag_ranking"])
    else:
        log.info("タグランキングなし（データ不足）。recommended_tags は更新しません。")

    log.info(
        "✅ 分析完了 | 投稿=%d 閲覧計=%d BM計=%d 平均BM率=%.4f",
        summary["total_posts"],
        summary["total_views"],
        summary["total_bookmarks"],
        summary["avg_bm_rate"],
    )


if __name__ == "__main__":
    main()
