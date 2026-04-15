# -*- coding: utf-8 -*-
"""
打开浏览器登录 QQ 音乐，登录成功后 Cookie 自动保存到 Playwright profile 目录。
之后 worker 启动时会自动从这个 profile 读取 Cookie。

用法:
  python login_qqmusic.py                    # 默认用 Edge 打开
  python login_qqmusic.py --browser chrome   # 用 Chrome 打开
  python login_qqmusic.py --profile ./my_profile  # 指定 profile 目录
"""

import argparse
import sys

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description="登录 QQ 音乐，保存 Cookie 到 Playwright profile")
    parser.add_argument("--profile", default=".playwright_profile", help="profile 存储目录 (默认 .playwright_profile)")
    parser.add_argument("--browser", default="chrome", help="浏览器: chrome / msedge / chromium (默认 chrome)")
    args = parser.parse_args()

    print(f"Profile 目录: {args.profile}")
    print(f"浏览器: {args.browser}")
    print()
    print("即将打开 QQ 音乐登录页面，请在浏览器中完成登录。")
    print("登录成功后关闭浏览器窗口即可，Cookie 会自动保存。")
    print()

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=args.profile,
            channel=args.browser if args.browser != "chromium" else None,
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
                "--disable-extensions",
            ],
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            viewport={"width": 800, "height": 600},
        )

        page = context.pages[0] if context.pages else context.new_page()

        # 屏蔽大文件资源，只保留登录需要的请求
        page.route("**/*.{mp3,mp4,flac}", lambda route: route.abort())
        page.route("**/beacon/**", lambda route: route.abort())

        page.goto("https://y.qq.com", wait_until="domcontentloaded")

        print("浏览器已打开，请登录 QQ 音乐...")
        print("登录完成后直接关闭浏览器窗口即可。")
        print()

        # 等待用户手动关闭浏览器
        try:
            page.wait_for_event("close", timeout=0)
        except Exception:
            pass

        # 验证 Cookie
        cookies = context.cookies(["https://y.qq.com", "https://qq.com", "https://u.y.qq.com"])
        qq_cookies = [c for c in cookies if "qq.com" in (c.get("domain") or "")]

        context.close()

    if qq_cookies:
        cookie_names = {c["name"] for c in qq_cookies}
        has_login = "qm_keyst" in cookie_names or "qqmusic_key" in cookie_names
        print(f"已保存 {len(qq_cookies)} 个 QQ 相关 Cookie 到 {args.profile}")
        uin_cookie = next((c for c in qq_cookies if c["name"] == "uin"), None)
        if uin_cookie:
            print(f"UIN: {uin_cookie['value']}")
        if has_login:
            print("检测到登录态 Cookie (qm_keyst)，登录成功！")
        else:
            print("警告: 未检测到 qm_keyst，可能未成功登录 VIP 账号。")
        print("Worker 启动时会自动从此 profile 加载 Cookie。")
    else:
        print("未检测到 QQ 相关 Cookie，可能未成功登录。")
        sys.exit(1)


if __name__ == "__main__":
    main()
