#!/usr/bin/env python3
"""
Pixiv Playwright アップローダー  pixiv_uploader.py
===================================================
Playwright (ヘッドレス Chromium) を使用して Pixiv の Web UI 経由で
イラストを自動投稿する。

Pixiv には公開アップロード API が存在しないため、
ブラウザ自動操作が唯一の自動投稿手段。
"""

import json
import logging
import os
import re
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

log = logging.getLogger(__name__)

PIXIV_LOGIN_URL = "https://accounts.pixiv.net/login"
PIXIV_UPLOAD_URL = "https://www.pixiv.net/upload.php"


def _login(page, email: str, password: str) -> None:
    """Pixiv にログインする"""
    log.info("Pixiv にログイン中...")
    page.goto(PIXIV_LOGIN_URL, wait_until="networkidle", timeout=30000)
    time.sleep(2)  # ページの完全読み込みを待つ

    # メールアドレスとパスワードを入力
    # Pixiv のログインフォームは動的に読み込まれるため、要素が表示されるまで待つ
    email_input = page.wait_for_selector(
        'input[autocomplete="username"], input[type="email"], input[name="pixiv_id"]',
        timeout=15000,
    )
    email_input.fill(email)

    password_input = page.wait_for_selector(
        'input[autocomplete="current-password"], input[type="password"]',
        timeout=10000,
    )
    password_input.fill(password)

    # ログインボタンをクリック
    login_btn = page.wait_for_selector(
        'button[type="submit"], button:has-text("ログイン"), button:has-text("Login")',
        timeout=10000,
    )
    login_btn.click()

    # ログイン完了を待つ (pixiv.net にリダイレクトされるまで)
    try:
        page.wait_for_url("**/pixiv.net/**", timeout=30000)
    except PwTimeout:
        # リダイレクトされない場合はログイン失敗
        screenshot_path = "/tmp/pixiv_login_fail.png"
        page.screenshot(path=screenshot_path)
        raise RuntimeError(
            f"Pixiv ログインに失敗しました (スクリーンショット: {screenshot_path})"
        )

    log.info("Pixiv にログインしました")


def upload_illust(
    image_paths: list[str],
    title: str,
    caption: str,
    tags: list[str],
    x_restrict: int = 0,
    ai_type: int = 2,
    restrict: int = 0,
) -> str:
    """
    Playwright でイラストをアップロードする。

    Args:
        image_paths: アップロードする画像ファイルのパスリスト
        title: 作品タイトル
        caption: 作品の説明文
        tags: タグのリスト
        x_restrict: 0=全年齢, 1=R-18, 2=R-18G
        ai_type: 0=不明, 1=非AI, 2=AI生成
        restrict: 0=公開, 1=マイピク限定

    Returns:
        投稿されたイラストの ID (文字列)
    """
    email = os.environ.get("PIXIV_EMAIL", "")
    password = os.environ.get("PIXIV_PASSWORD", "")
    if not email or not password:
        raise RuntimeError(
            "PIXIV_EMAIL と PIXIV_PASSWORD 環境変数が必要です"
        )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="ja-JP",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        try:
            # ① ログイン
            _login(page, email, password)

            # ② アップロードページへ遷移
            log.info("アップロードページを開きます...")
            page.goto(PIXIV_UPLOAD_URL, wait_until="networkidle", timeout=30000)
            time.sleep(3)

            # ③ 画像ファイルをアップロード
            log.info("画像 %d 枚をアップロード中...", len(image_paths))
            file_input = page.wait_for_selector(
                'input[type="file"]', timeout=15000
            )
            file_input.set_input_files(image_paths)
            time.sleep(3)  # アップロード処理を待つ

            # ④ タイトルを入力
            log.info("メタデータを入力中...")
            title_input = page.wait_for_selector(
                'input[placeholder*="タイトル"], input[name="title"], '
                '#title, [data-testid="title-input"]',
                timeout=15000,
            )
            title_input.fill(title)

            # ⑤ キャプションを入力
            caption_input = page.query_selector(
                'textarea[placeholder*="説明"], textarea[name="caption"], '
                '#caption, [data-testid="caption-input"], '
                '[contenteditable="true"]'
            )
            if caption_input:
                tag_name = caption_input.evaluate("el => el.tagName.toLowerCase()")
                if tag_name == "textarea":
                    caption_input.fill(caption)
                else:
                    # contenteditable の場合
                    caption_input.click()
                    page.keyboard.type(caption)

            # ⑥ タグを入力
            for tag in tags[:10]:  # Pixiv は最大10タグ
                tag_input = page.query_selector(
                    'input[placeholder*="タグ"], input[name*="tag"], '
                    '[data-testid="tag-input"] input'
                )
                if tag_input:
                    tag_input.fill(tag)
                    page.keyboard.press("Enter")
                    time.sleep(0.5)

            # ⑦ 年齢制限を設定
            if x_restrict == 1:
                # R-18 ラジオボタンまたはチェックボックスを選択
                r18_option = page.query_selector(
                    'label:has-text("R-18"), '
                    'input[value="1"][name*="restrict"], '
                    '[data-testid="r18-option"]'
                )
                if r18_option:
                    r18_option.click()
                    time.sleep(0.5)

            # ⑧ AI 生成フラグを設定
            if ai_type == 2:
                ai_option = page.query_selector(
                    'label:has-text("AI"), '
                    'input[value="2"][name*="ai"], '
                    '[data-testid="ai-option"], '
                    'label:has-text("AI生成")'
                )
                if ai_option:
                    ai_option.click()
                    time.sleep(0.5)

            # ⑨ 投稿ボタンをクリック
            log.info("投稿を送信中...")
            # 投稿前にスクリーンショットを保存（デバッグ用）
            page.screenshot(path="/tmp/pixiv_before_submit.png")

            submit_btn = page.query_selector(
                'button:has-text("投稿する"), '
                'button:has-text("投稿"), '
                'button[type="submit"]:has-text("投稿"), '
                'input[type="submit"][value*="投稿"]'
            )
            if not submit_btn:
                page.screenshot(path="/tmp/pixiv_no_submit_btn.png")
                raise RuntimeError(
                    "投稿ボタンが見つかりません "
                    "(スクリーンショット: /tmp/pixiv_no_submit_btn.png)"
                )

            submit_btn.click()

            # ⑩ 投稿完了を待つ（作品ページにリダイレクト）
            try:
                page.wait_for_url(
                    "**/artworks/**", timeout=60000
                )
                current_url = page.url
                match = re.search(r"/artworks/(\d+)", current_url)
                illust_id = match.group(1) if match else "unknown"
                log.info("投稿成功 → %s", current_url)
                return illust_id

            except PwTimeout:
                # artworks ページに遷移しなかった場合
                current_url = page.url
                page.screenshot(path="/tmp/pixiv_after_submit.png")

                # URL から ID を取得できるか試みる
                match = re.search(r"/artworks/(\d+)", current_url)
                if match:
                    return match.group(1)

                # illust_id がページ内に含まれていないか確認
                content = page.content()
                id_match = re.search(r'"illustId"\s*:\s*"(\d+)"', content)
                if id_match:
                    return id_match.group(1)

                raise RuntimeError(
                    f"投稿完了を確認できません (URL: {current_url}) "
                    "(スクリーンショット: /tmp/pixiv_after_submit.png)"
                )

        finally:
            # デバッグ用スクリーンショット
            try:
                page.screenshot(path="/tmp/pixiv_final_state.png")
            except Exception:
                pass
            browser.close()
