#!/usr/bin/env python3
"""
PIXIV自動投稿スクリプト  post_to_pixiv.py
=========================================
GitHub Actions から 30 分おきに呼び出される。
config.json のスケジュールと現在時刻を照合し、
該当時刻なら Google Drive から画像を取得して Pixiv に投稿する。
"""

import csv
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from pixiv_uploader import upload_illust as playwright_upload

# ──────────────────────────────────────────────
#  定数・設定
# ──────────────────────────────────────────────
JST = timezone(timedelta(hours=9))
ROOT = Path(__file__).parent
CONFIG_PATH     = ROOT / "config.json"
TEMPLATES_PATH  = ROOT / "templates.json"
METADATA_PATH   = ROOT / "metadata.csv"

DRIVE_SCOPES    = ["https://www.googleapis.com/auth/drive"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
#  設定ファイル読み込み
# ──────────────────────────────────────────────
def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_templates() -> dict:
    with open(TEMPLATES_PATH, encoding="utf-8") as f:
        return json.load(f)


# ──────────────────────────────────────────────
#  時刻チェック（経過スロット方式）
# ──────────────────────────────────────────────
def get_elapsed_slots(schedule: list[str]) -> list[str]:
    """本日 JST で既に時刻を過ぎたスケジュールスロットのリストを返す。
    例: 現在 15:30 JST, schedule=[09:00,11:00,15:00,17:00]
        → [09:00, 11:00, 15:00] (15:00は過ぎているので含む)"""
    now = datetime.now(JST)
    now_minutes = now.hour * 60 + now.minute
    elapsed = []
    for slot in schedule:
        h, m = map(int, slot.split(":"))
        slot_minutes = h * 60 + m
        if now_minutes >= slot_minutes:
            elapsed.append(slot)
    return elapsed


def get_today_post_count() -> int:
    """本日 (JST) すでに投稿した件数をカウント"""
    today = datetime.now(JST).strftime("%Y-%m-%d")
    if not METADATA_PATH.exists():
        return 0
    with open(METADATA_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return sum(
        1 for r in rows
        if r.get("posted", "no").lower() == "yes"
        and r.get("posted_at", "").startswith(today)
    )


def should_post_now(schedule: list[str], daily_limit: int) -> tuple[bool, str]:
    """経過スロット方式: 本日経過したスロット数 > 本日投稿済み件数 なら投稿する。
    GitHub Actions cron の遅延に左右されない堅牢なロジック。
    Returns: (投稿すべきか, 理由メッセージ)"""
    elapsed = get_elapsed_slots(schedule)
    if not elapsed:
        return False, "本日のスケジュールスロットはまだ開始していません"

    today_count = get_today_post_count()

    # 日次上限チェック
    if today_count >= daily_limit:
        return False, f"本日の投稿上限 ({daily_limit}件) に到達済み"

    # 経過スロット数と投稿済み数を比較
    slots_should_have_posted = min(len(elapsed), daily_limit)
    if today_count >= slots_should_have_posted:
        return False, (
            f"経過スロット数={len(elapsed)}, 投稿済み={today_count} → 未消化なし"
        )

    return True, (
        f"経過スロット数={len(elapsed)}, 投稿済み={today_count} "
        f"→ {slots_should_have_posted - today_count}件未消化あり (次: {elapsed[today_count]})"
    )


# ──────────────────────────────────────────────
#  metadata.csv 操作
# ──────────────────────────────────────────────
METADATA_FIELDS = [
    "post_id", "drive_folder", "title", "tags", "caption",
    "x_restrict", "ai_type", "restrict",
    "posted", "posted_at", "pixiv_id", "image_count",
]


def read_metadata() -> list[dict]:
    if not METADATA_PATH.exists():
        return []
    with open(METADATA_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_metadata(rows: list[dict]) -> None:
    if not rows:
        return
    fields = list(rows[0].keys())
    with open(METADATA_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def get_next_post(rows: list[dict]) -> dict | None:
    """未投稿 (posted が空 or "no") の先頭エントリを返す"""
    for row in rows:
        val = row.get("posted", "").strip().lower()
        if val in ("", "no"):
            return row
    return None


# ──────────────────────────────────────────────
#  Google Drive 操作
# ──────────────────────────────────────────────
def get_drive_service():
    creds_json = os.environ["GOOGLE_CREDENTIALS"]
    creds = Credentials.from_service_account_info(
        json.loads(creds_json), scopes=DRIVE_SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def ensure_posted_folder(svc, parent_id: str) -> str:
    """'posted' サブフォルダの ID を返す。なければ作成する"""
    q = (
        f"'{parent_id}' in parents"
        " and name='posted'"
        " and mimeType='application/vnd.google-apps.folder'"
        " and trashed=false"
    )
    res = svc.files().list(q=q, fields="files(id)").execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]
    body = {
        "name": "posted",
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    return svc.files().create(body=body, fields="id").execute()["id"]


def find_post_folder(svc, parent_id: str, post_id: str) -> str | None:
    """post_id と同名のサブフォルダ ID を返す"""
    safe_name = post_id.replace("\\", "\\\\").replace("'", "\\'")
    q = (
        f"'{parent_id}' in parents"
        f" and name='{safe_name}'"
        " and mimeType='application/vnd.google-apps.folder'"
        " and trashed=false"
    )
    res = svc.files().list(q=q, fields="files(id)").execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def download_images(svc, folder_id: str, dest: Path) -> list[str]:
    """フォルダ内の画像ファイルを dest にダウンロードし、パスリスト(名前順)を返す"""
    res = svc.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id, name, mimeType)",
        orderBy="name",
    ).execute()
    paths = []
    for f in res.get("files", []):
        if not f["mimeType"].startswith("image/"):
            continue
        local = dest / f["name"]
        req = svc.files().get_media(fileId=f["id"])
        with open(local, "wb") as fh:
            dl = MediaIoBaseDownload(fh, req)
            done = False
            while not done:
                _, done = dl.next_chunk()
        paths.append(str(local))
        log.info("  DL: %s", f["name"])
    return sorted(paths)


def move_to_posted(svc, folder_id: str, posted_id: str) -> None:
    meta = svc.files().get(fileId=folder_id, fields="parents").execute()
    prev = ",".join(meta.get("parents", []))
    svc.files().update(
        fileId=folder_id,
        addParents=posted_id,
        removeParents=prev,
        fields="id",
    ).execute()


# ──────────────────────────────────────────────
#  Pixiv 投稿 (Playwright ブラウザ自動操作)
# ──────────────────────────────────────────────
def upload_to_pixiv(
    image_paths: list[str],
    title: str,
    caption: str,
    tags: list[str],
    x_restrict: int = 0,
    ai_type: int = 2,
    restrict: int = 0,
) -> str:
    """Playwright 経由で Pixiv にイラストをアップロードする。
    Returns: 投稿イラスト ID (str)"""
    return playwright_upload(
        image_paths=image_paths,
        title=title,
        caption=caption,
        tags=tags,
        x_restrict=x_restrict,
        ai_type=ai_type,
        restrict=restrict,
    )


# ──────────────────────────────────────────────
#  メイン
# ──────────────────────────────────────────────
def main() -> None:
    cfg  = load_config()
    tmpl = load_templates()
    dry  = cfg.get("dry_run", False)

    if dry:
        log.info("=== DRY RUN モード (実際には投稿しません) ===")

    # ① 投稿判定（経過スロット方式）
    schedule = cfg.get("post_schedule_jst", [])
    limit = cfg.get("daily_post_limit", 10)

    should, reason = should_post_now(schedule, limit)
    if not should:
        log.info("投稿スキップ: %s", reason)
        return

    log.info("投稿実行: %s", reason)

    # ③ 次の未投稿エントリを取得
    rows = read_metadata()
    entry = get_next_post(rows)
    if not entry:
        log.warning("投稿待ちエントリがありません。metadata.csv を確認してください。")
        return

    post_id = entry["post_id"]
    # drive_folder があればそれを使い、なければ post_id をフォルダ名とする
    drive_folder = entry.get("drive_folder", "").strip() or post_id
    log.info("投稿対象: %s (フォルダ: %s)", post_id, drive_folder)

    # ④ テンプレート適用 (空欄ならテンプレートの値を使用)
    title   = entry.get("title",  "").strip() or tmpl["title"]
    raw_tag = entry.get("tags",   "").strip()
    tags    = [t.strip() for t in raw_tag.split("|") if t.strip()] if raw_tag else tmpl["tags"]
    caption = entry.get("caption","").strip() or tmpl["caption"]
    x_restr = int(entry.get("x_restrict", tmpl.get("x_restrict", 0)) or tmpl.get("x_restrict", 0))
    ai_type = int(entry.get("ai_type",    tmpl.get("ai_type",    2)) or tmpl.get("ai_type", 2))
    restrict= int(entry.get("restrict",   tmpl.get("restrict",   0)) or tmpl.get("restrict", 0))

    log.info("タイトル: %s", title)
    log.info("タグ:     %s", " / ".join(tags))

    # ⑤ Google Drive から画像をダウンロード
    try:
        svc = get_drive_service()
    except Exception as e:
        log.error("Google Drive 認証エラー: %s", e)
        sys.exit(1)

    root_folder_id = os.environ["PIXIV_DRIVE_FOLDER_ID"]

    post_folder_id = find_post_folder(svc, root_folder_id, drive_folder)
    if not post_folder_id:
        log.error("Google Drive 上に '%s' フォルダが見つかりません", drive_folder)
        sys.exit(1)

    posted_folder_id = ensure_posted_folder(svc, root_folder_id)

    with tempfile.TemporaryDirectory() as tmpdir:
        img_paths = download_images(svc, post_folder_id, Path(tmpdir))
        if not img_paths:
            log.error("%s フォルダに画像がありません", post_id)
            sys.exit(1)
        log.info("画像 %d 枚をダウンロードしました", len(img_paths))

        # ⑥ Pixiv に投稿 (Playwright ブラウザ自動操作)
        if dry:
            log.info("[DRY RUN] %s: %d 枚を投稿予定", post_id, len(img_paths))
            pixiv_id = "dry_run"
        else:
            try:
                pixiv_id = upload_to_pixiv(
                    img_paths, title, caption, tags,
                    x_restr, ai_type, restrict,
                )
            except Exception as e:
                log.error("Pixiv 投稿エラー: %s", e)
                sys.exit(1)

        # ⑦ metadata.csv を更新
        now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
        for row in rows:
            if row["post_id"] == post_id:
                row["posted"]      = "yes"
                row["posted_at"]   = now_str
                row["pixiv_id"]    = pixiv_id
                row["image_count"] = str(len(img_paths))
        write_metadata(rows)
        log.info("metadata.csv を更新しました")

        # ⑧ Drive の投稿済みフォルダへ移動
        if not dry:
            try:
                move_to_posted(svc, post_folder_id, posted_folder_id)
                log.info("%s を posted/ フォルダに移動しました", post_id)
            except Exception as e:
                log.warning("posted/ への移動に失敗 (投稿自体は成功): %s", e)

    log.info("✅ 完了: %s → pixiv_id=%s", post_id, pixiv_id)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("予期しないエラー: %s", e, exc_info=True)
        sys.exit(1)
