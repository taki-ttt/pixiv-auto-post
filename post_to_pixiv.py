#!/usr/bin/env python3
"""
PIXIV自動投稿スクリプト  post_to_pixiv.py
=========================================
GitHub Actions から 30 分おきに呼び出される。
config.json のスケジュールと現在時刻を照合し、
該当時刻なら Google Drive から画像を取得して Pixiv に投稿する。
"""

import csv
import io
import json
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pixivpy3 import AppPixivAPI

# ──────────────────────────────────────────────
#  定数・設定
# ──────────────────────────────────────────────
JST = timezone(timedelta(hours=9))
ROOT = Path(__file__).parent
CONFIG_PATH     = ROOT / "config.json"
TEMPLATES_PATH  = ROOT / "templates.json"
METADATA_PATH   = ROOT / "metadata.csv"

PIXIV_API_BASE  = "https://app-api.pixiv.net"
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
#  時刻チェック
# ──────────────────────────────────────────────
def is_post_time(schedule: list[str], tolerance_min: int) -> tuple[bool, str]:
    """現在時刻が schedule のいずれかに一致するか判定。(bool, 一致した時刻文字列) を返す"""
    now = datetime.now(JST)
    for slot in schedule:
        h, m = map(int, slot.split(":"))
        target = now.replace(hour=h, minute=m, second=0, microsecond=0)
        diff = abs((now - target).total_seconds()) / 60
        if diff <= tolerance_min:
            return True, slot
    return False, ""


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


# ──────────────────────────────────────────────
#  metadata.csv 操作
# ──────────────────────────────────────────────
METADATA_FIELDS = [
    "post_id", "title", "tags", "caption",
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
    """未投稿 (posted=no) の先頭エントリを返す"""
    for row in rows:
        if row.get("posted", "no").lower() == "no":
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
    q = (
        f"'{parent_id}' in parents"
        f" and name='{post_id}'"
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
#  Pixiv 投稿
# ──────────────────────────────────────────────
def auth_pixiv() -> AppPixivAPI:
    api = AppPixivAPI()
    api.auth(refresh_token=os.environ["PIXIV_REFRESH_TOKEN"])
    return api


def upload_illust(
    api: AppPixivAPI,
    image_paths: list[str],
    title: str,
    caption: str,
    tags: list[str],
    x_restrict: int = 0,
    ai_type: int = 2,
    restrict: int = 0,
) -> str:
    """
    Pixiv アプリ API 経由でイラストをアップロードする。
    Returns: 投稿イラスト ID (str)

    ※ Pixiv の非公式 API を使用します。
      エンドポイントが変更された場合は PIXIV_API_BASE と url を調整してください。
      参考: https://github.com/upbit/pixivpy
    """
    url = f"{PIXIV_API_BASE}/v1/works/add"
    headers = {
        "Authorization": f"Bearer {api.access_token}",
        "User-Agent":    "PixivAndroidApp/6.18.1 (Android 11; SM-G998B)",
        "App-OS":        "android",
        "App-OS-Version":"11",
        "App-Version":   "6.18.1",
    }

    data: list[tuple] = [
        ("title",         title),
        ("work_type",     "illust" if len(image_paths) == 1 else "manga"),
        ("caption",       caption),
        ("restrict",      str(restrict)),
        ("x_restrict",    str(x_restrict)),
        ("ai_type",       str(ai_type)),
        ("original",      "1"),
        ("allow_comment", "1"),
        ("allow_tag_edit","1"),
    ]
    for tag in tags:
        data.append(("tags[]", tag))

    opened: list[io.BufferedReader] = []
    files:  list[tuple] = []
    try:
        for img_path in image_paths:
            suffix = Path(img_path).suffix.lower().lstrip(".")
            mime   = "image/jpeg" if suffix in ("jpg", "jpeg") else f"image/{suffix or 'png'}"
            fh = open(img_path, "rb")
            opened.append(fh)
            files.append(("images[]", (Path(img_path).name, fh, mime)))

        resp = requests.post(url, headers=headers, data=data, files=files, timeout=60)

        if resp.ok:
            illust_id = str(resp.json().get("illust", {}).get("id", "unknown"))
            log.info("投稿成功 → https://www.pixiv.net/artworks/%s", illust_id)
            return illust_id
        else:
            log.error("投稿失敗: %s %s", resp.status_code, resp.text[:300])
            resp.raise_for_status()

    finally:
        for fh in opened:
            fh.close()


# ──────────────────────────────────────────────
#  メイン
# ──────────────────────────────────────────────
def main() -> None:
    cfg  = load_config()
    tmpl = load_templates()
    dry  = cfg.get("dry_run", False)

    if dry:
        log.info("=== DRY RUN モード (実際には投稿しません) ===")

    # ① 投稿時刻チェック
    hit, slot = is_post_time(
        cfg.get("post_schedule_jst", []),
        cfg.get("post_tolerance_minutes", 14),
    )
    if not hit:
        log.info("投稿時刻ではありません。スキップ。")
        return

    log.info("スケジュール時刻 %s に一致しました", slot)

    # ② 本日の投稿上限チェック
    limit = cfg.get("daily_post_limit", 10)
    today_cnt = get_today_post_count()
    if today_cnt >= limit:
        log.info("本日の投稿上限 (%d件) に達しています。スキップ。", limit)
        return
    log.info("本日投稿済み: %d / %d", today_cnt, limit)

    # ③ 次の未投稿エントリを取得
    rows = read_metadata()
    entry = get_next_post(rows)
    if not entry:
        log.warning("投稿待ちエントリがありません。metadata.csv を確認してください。")
        return

    post_id = entry["post_id"]
    log.info("投稿対象: %s", post_id)

    # ④ テンプレート適用 (空欄ならテンプレートの値を使用)
    title   = entry.get("title",  "").strip() or tmpl["title"]
    raw_tag = entry.get("tags",   "").strip()
    tags    = [t.strip() for t in raw_tag.split("|") if t.strip()] if raw_tag else tmpl["tags"]
    caption = entry.get("caption","").strip() or tmpl["caption"]
    x_restr = int(entry.get("x_restrict", tmpl.get("x_restrict", 0)))
    ai_type = int(entry.get("ai_type",    tmpl.get("ai_type",    2)))
    restrict= int(entry.get("restrict",   tmpl.get("restrict",   0)))

    log.info("タイトル: %s", title)
    log.info("タグ:     %s", " / ".join(tags))

    # ⑤ Google Drive から画像をダウンロード
    svc = get_drive_service()
    root_folder_id = os.environ["PIXIV_DRIVE_FOLDER_ID"]

    post_folder_id = find_post_folder(svc, root_folder_id, post_id)
    if not post_folder_id:
        log.error("Google Drive 上に %s フォルダが見つかりません", post_id)
        return

    posted_folder_id = ensure_posted_folder(svc, root_folder_id)

    with tempfile.TemporaryDirectory() as tmpdir:
        img_paths = download_images(svc, post_folder_id, Path(tmpdir))
        if not img_paths:
            log.error("%s フォルダに画像がありません", post_id)
            return
        log.info("画像 %d 枚をダウンロードしました", len(img_paths))

        # ⑥ Pixiv に投稿
        if dry:
            log.info("[DRY RUN] %s: %d 枚を投稿予定", post_id, len(img_paths))
            pixiv_id = "dry_run"
        else:
            api = auth_pixiv()
            pixiv_id = upload_illust(
                api, img_paths, title, caption, tags,
                x_restr, ai_type, restrict,
            )

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
            move_to_posted(svc, post_folder_id, posted_folder_id)
            log.info("%s を posted/ フォルダに移動しました", post_id)

    log.info("✅ 完了: %s → pixiv_id=%s", post_id, pixiv_id)


if __name__ == "__main__":
    main()
