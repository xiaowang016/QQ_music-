# -*- coding: utf-8 -*-
"""
分布式 QQ Music Worker — 通过 HTTP API 领取任务（不直连数据库）。

600 台 worker 统一访问中央 API 服务:
  - 零数据库连接，只需网络能访问 API 地址
  - 批量领取 + 批量回写，减少请求次数
  - 断线自动重连，指数退避
"""

import argparse
import json
import logging
import os
import queue
import re
import shutil
import signal
import socket
import sys
import threading
import time
from pathlib import Path

# 强制走本地网络，清除所有代理
for _key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
             "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"):
    os.environ.pop(_key, None)
import urllib.request
urllib.request.install_opener(urllib.request.build_opener(urllib.request.ProxyHandler({})))

_project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _project_dir)

import urllib3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("worker")
logging.getLogger("urllib3").setLevel(logging.ERROR)

# ============================================================
#  ★ 配置区 ★  （换机器只改这里，或直接编辑 worker_config.json）
# ============================================================

CONFIG_FILE = "worker_config.json"   # 配置文件路径（相对于本文件目录，或绝对路径）

WORKER_ID   = "D019"                   # None → 读配置文件 → 自动主机名；填字符串则固定，如 "Node-001"
API_BASE_URL = os.getenv("QQMUSIC_API_URL", "http://192.168.10.165:8080")

OUTPUT_DIR = r"E:\0413QQ音乐下载\测试"
THREADS = 2
BATCH_SIZE = 20
API_QPS = 2
FLUSH_INTERVAL = 15.0
LEASE_SECONDS = 3600
IDLE_SECONDS = 30
TIMEOUT = 30
MIN_FREE_SPACE_GB = 2.0
PREFER_THIRDPARTY = True  # 是否优先使用付费/第三方解析 API
TASK_INTERVAL = 0          # 每个任务处理完后等待秒数（限速用，0=不限）

# ============================================================

from QQMusicSpider.download import (
    QQMusicDownloadError,
    fetch_download_info,
    fetch_download_info_with_fallback,
    has_explicit_auth,
    load_auth_from_playwright_profile,
    save_song_file,
    set_vkey_rate_limit,
    set_xianyuw_keys,
)
from QQMusicSpider.tasks import QQMusicMetadataError, set_rate_limit
from QQMusicSpider.utils import build_song_folder_name, sanitize_path_part


def parse_args():
    parser = argparse.ArgumentParser(description="Worker: claim tasks from API, process, report back.")
    parser.add_argument("--config", default=CONFIG_FILE, help="配置文件路径")
    parser.add_argument("--api-url", default=None)
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--download-quality", default=None)
    parser.add_argument("--qqmusic-cookie", default=None)
    parser.add_argument("--qqmusic-uin", default=None)
    parser.add_argument("--metadata-only", action="store_true", default=None)
    parser.add_argument("--download", dest="metadata_only", action="store_false")
    parser.add_argument("--lease-seconds", type=int, default=None)
    parser.add_argument("--idle-seconds", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=None)
    parser.add_argument("--threads", type=int, default=None)
    parser.add_argument("--max-tasks", type=int, default=None)
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--api-qps", type=int, default=None)
    parser.add_argument("--skip-playwright", action="store_true", default=None)
    parser.add_argument("--flush-interval", type=float, default=None)
    parser.add_argument("--comment-fallback-profile-dir", default=None)
    parser.add_argument("--comment-fallback-browser-channel", default=None)
    parser.add_argument("--comment-fallback-wait-seconds", type=float, default=None)
    parser.add_argument("--comment-fallback-headful", action="store_true", default=None)
    parser.add_argument("--min-free-space-gb", type=float, default=None, help="自动检测: 最小磁盘剩余空间(GB)，空间不足时自动停机保护")
    parser.add_argument("--prefer-thirdparty", action="store_true", default=None)
    parser.add_argument("--task-interval", type=float, default=None, help="每个任务处理完后等待秒数（限速用）")
    args = parser.parse_args()

    # 读取配置文件
    cfg = {}
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = Path(_project_dir) / config_path
    if config_path.exists():
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
        log.info("加载配置: %s", config_path)

    def pick(arg_val, code_val, cfg_key, cfg_default):
        """优先级：CLI参数 > 代码常量 > 配置文件 > 内置默认"""
        if arg_val is not None:   return arg_val   # CLI 最高
        if code_val is not None:  return code_val  # 代码常量次之
        return cfg.get(cfg_key, cfg_default)        # 配置文件兜底

    args.api_url        = pick(args.api_url,        API_BASE_URL,   "api_url",        "http://192.168.10.165:8080")
    args.worker_id      = pick(args.worker_id,      WORKER_ID,      "worker_id",      socket.gethostname())
    args.output_dir     = str(Path(pick(args.output_dir, OUTPUT_DIR, "output_dir", "")).expanduser().resolve())
    args.download_quality = pick(args.download_quality, None,        "download_quality", "flac")
    args.qqmusic_cookie = pick(args.qqmusic_cookie, None,           "qqmusic_cookie", "")
    args.qqmusic_uin    = pick(args.qqmusic_uin,    None,           "qqmusic_uin",    "0")
    args.metadata_only  = pick(args.metadata_only,  None,           "metadata_only",  False)
    args.lease_seconds  = pick(args.lease_seconds,  LEASE_SECONDS,  "lease_seconds",  3600)
    args.idle_seconds   = pick(args.idle_seconds,   IDLE_SECONDS,   "idle_seconds",   10)
    args.timeout        = pick(args.timeout,        TIMEOUT,        "timeout",        15)
    args.threads        = pick(args.threads,        THREADS,        "threads",        50)
    args.max_tasks      = pick(args.max_tasks,      None,           "max_tasks",      0)
    args.batch_size     = pick(args.batch_size,     BATCH_SIZE,     "batch_size",     500)
    args.api_qps        = pick(args.api_qps,        API_QPS,        "api_qps",        200)
    args.skip_playwright = pick(args.skip_playwright, None,         "skip_playwright", True)
    args.flush_interval = pick(args.flush_interval, FLUSH_INTERVAL, "flush_interval", 3.0)
    args.comment_fallback_profile_dir     = pick(args.comment_fallback_profile_dir,     None, "comment_fallback_profile_dir",     ".playwright_profile")
    args.comment_fallback_browser_channel = pick(args.comment_fallback_browser_channel, None, "comment_fallback_browser_channel", "msedge")
    args.comment_fallback_wait_seconds    = pick(args.comment_fallback_wait_seconds,    None, "comment_fallback_wait_seconds",    8.0)
    args.comment_fallback_headful         = pick(args.comment_fallback_headful,         None, "comment_fallback_headful",         False)
    args.min_free_space_gb                = pick(args.min_free_space_gb,                MIN_FREE_SPACE_GB, "min_free_space_gb",   2.0)
    args.prefer_thirdparty               = pick(args.prefer_thirdparty,               PREFER_THIRDPARTY, "prefer_thirdparty",  False)
    args.task_interval                   = pick(args.task_interval,                   TASK_INTERVAL,     "task_interval",       0)
    return args


# ============================================================
#  HTTP API 客户端
# ============================================================

_api_http = urllib3.PoolManager(
    num_pools=8,
    maxsize=100,
    retries=urllib3.Retry(
        total=5, backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    ),
    timeout=urllib3.Timeout(connect=10, read=30),
)


def api_claim_tasks(api_url, worker_id, batch_size, lease_seconds):
    """从 API 批量领取任务。"""
    resp = _api_http.request(
        "POST",
        f"{api_url}/tasks/claim",
        body=json.dumps({
            "worker_id": worker_id,
            "batch_size": batch_size,
            "lease_seconds": lease_seconds,
        }).encode(),
        headers={"Content-Type": "application/json"},
    )
    if resp.status != 200:
        raise RuntimeError(f"API claim failed: {resp.status} {resp.data.decode()[:200]}")
    data = json.loads(resp.data.decode())
    return data.get("tasks", [])


def api_report_done(api_url, items):
    """批量回报已完成的任务。items: list of dict(task_id, output_dir, audio_file_name)"""
    if not items:
        return
    resp = _api_http.request(
        "POST",
        f"{api_url}/tasks/done",
        body=json.dumps({"items": items}).encode(),
        headers={"Content-Type": "application/json"},
    )
    if resp.status != 200:
        raise RuntimeError(f"API done failed: {resp.status} {resp.data.decode()[:200]}")


def api_report_failed(api_url, items, requeue=False):
    """批量回报失败的任务。items: list of dict(task_id, error)"""
    if not items:
        return
    resp = _api_http.request(
        "POST",
        f"{api_url}/tasks/failed",
        body=json.dumps({"items": items, "requeue": requeue}).encode(),
        headers={"Content-Type": "application/json"},
    )
    if resp.status != 200:
        raise RuntimeError(f"API failed report: {resp.status} {resp.data.decode()[:200]}")


# ============================================================
#  反封策略
# ============================================================

import random as _random

# 每次请求前随机等 0~0.3 秒，打散请求时间线，避免多线程同一毫秒发请求
REQUEST_JITTER = float(os.getenv("QQMUSIC_JITTER", "0.3"))

# 随机 User-Agent 池（每个 worker 会话随机选一个，每 50 次请求换一次）
_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

def _jitter():
    if REQUEST_JITTER > 0:
        time.sleep(_random.uniform(0, REQUEST_JITTER))

# ============================================================
#  直连 QQ 音乐 API（评论/歌词，不走系统代理）
# ============================================================

# 评论 API 限速：防止高频请求触发 QQ 音乐封禁 cookie
_COMMENT_RATE_LIMIT = 60   # 每分钟最多 60 次评论请求
_comment_tokens = None      # _TokenBucket，延迟初始化
_comment_semaphore = threading.Semaphore(3)  # 评论 API 最多同时 3 个并发


def _init_comment_rate_limit(rate_per_min=None):
    global _comment_tokens, _COMMENT_RATE_LIMIT
    if rate_per_min is not None:
        _COMMENT_RATE_LIMIT = rate_per_min
    from QQMusicSpider.download import _TokenBucket
    _comment_tokens = _TokenBucket(_COMMENT_RATE_LIMIT)


_direct_http = urllib3.PoolManager(
    num_pools=20,
    maxsize=120,
    retries=urllib3.Retry(
        total=3, backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    ),
    timeout=urllib3.Timeout(connect=10, read=30),
)
def _random_ua():
    return _random.choice(_UA_POOL)

_REQUEST_HEADERS = {
    "User-Agent": _random.choice(_UA_POOL),
}
_MUSICU_URL = "https://u.y.qq.com/cgi-bin/musicu.fcg"
_LYRIC_URL_TEMPLATE = "https://c.y.qq.com/lyric/fcgi-bin/fcg_query_lyric_yqq.fcg?nobase64=1&musicid={song_id}&format=json"
_SONG_PAGE_REFERER = "https://y.qq.com/n/yqq/song/{song_mid}.html"

# 评论接口需要 cookie，在 main() 中设置
_comment_cookie = ""
_comment_uin = "0"


def set_comment_auth(cookie, uin):
    global _comment_cookie, _comment_uin
    _comment_cookie = cookie
    _comment_uin = uin


def _fetch_json_direct(url, headers=None, timeout=30):
    _jitter()
    merged = {"User-Agent": _random_ua()}
    if headers:
        merged.update(headers)
    try:
        resp = _direct_http.request("GET", url, headers=merged, timeout=timeout)
        return resp.status, json.loads(resp.data.decode("utf-8"))
    except (urllib3.exceptions.HTTPError,
            urllib3.exceptions.NewConnectionError,
            urllib3.exceptions.MaxRetryError,
            urllib3.exceptions.ProtocolError,
            ConnectionError, TimeoutError) as exc:
        raise QQMusicMetadataError(str(exc)) from exc
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise QQMusicMetadataError(str(exc)) from exc


def _post_musicu(req_data, timeout=30, skip_jitter=False):
    """POST 到 u.y.qq.com/cgi-bin/musicu.fcg，带 cookie 鉴权 + 限速。"""
    if not skip_jitter:
        _jitter()
    # 评论 API 限速
    if _comment_tokens and not _comment_tokens.acquire(timeout=15):
        raise QQMusicMetadataError("Comment API rate limit timeout")
    payload = json.dumps(req_data, separators=(",", ":")).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": _random_ua(),
        "Referer": "https://y.qq.com/",
    }
    if _comment_cookie:
        headers["Cookie"] = _comment_cookie
    try:
        with _comment_semaphore:
            resp = _direct_http.request("POST", _MUSICU_URL, body=payload, headers=headers, timeout=timeout)
        return json.loads(resp.data.decode("utf-8"))
    except Exception as exc:
        raise QQMusicMetadataError(str(exc)) from exc


def _fetch_hot_comments_direct(song_id, song_mid, page_size=100, timeout=30):
    """获取评论：先抓热评，不够则用最新评论补齐到 page_size 条。

    目标：拿够 page_size 条；不足 page_size 则拿全部。
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    _comment_deadline = time.monotonic() + 45  # 加了限速后放宽到 45 秒
    target = page_size
    api_page_size = 25   # 服务端硬限制 PageSize ≤ 25，超过返回 10000
    _comment_timeout = 10  # 单次请求超时
    collected = []
    seen = set()

    def _parse_comments(comments_list):
        for c in comments_list:
            if len(collected) >= target:
                break
            nick = c.get("Nick", "")
            content = c.get("Content", "")
            if not content:
                continue
            key = (nick, content)
            if key not in seen:
                seen.add(key)
                collected.append({"comment_name": nick, "comment_text": content})

    def _fetch_page(page_num, method="GetHotCommentList", last_seq=""):
        data = _post_musicu({
            "comm": {"ct": 24, "cv": 0, "uin": _comment_uin},
            "req_0": {
                "module": "music.globalComment.CommentRead",
                "method": method,
                "param": {
                    "BizType": 1, "BizId": str(song_id),
                    "LastCommentSeqNo": last_seq, "PageSize": api_page_size,
                    "PageNum": page_num, "HotType": 1,
                    "WithAirborne": 0, "PicEnable": 1,
                },
            },
        }, timeout=_comment_timeout, skip_jitter=True)
        req = data.get("req_0", {})
        if req.get("code") != 0:
            return None
        return req.get("data", {})

    # === 阶段1：抓热评（同时顺便拿收藏量，省一次网络请求） ===
    first_data = _post_musicu({
        "comm": {"ct": 24, "cv": 0, "uin": _comment_uin},
        "req_0": {
            "module": "music.globalComment.CommentRead",
            "method": "GetHotCommentList",
            "param": {
                "BizType": 1, "BizId": str(song_id),
                "LastCommentSeqNo": "", "PageSize": api_page_size,
                "PageNum": 0, "HotType": 1,
                "WithAirborne": 0, "PicEnable": 1,
            },
        },
        "req_1": {
            "module": "music.musicasset.SongFavRead",
            "method": "GetSongFansNumberById",
            "param": {"v_songId": [song_id]},
        },
    }, timeout=_comment_timeout, skip_jitter=True)

    req0 = first_data.get("req_0", {})
    if req0.get("code") != 0:
        raise QQMusicMetadataError("Comment API returned error")
    page0 = req0.get("data", {})

    # 顺带解析收藏量
    req1 = first_data.get("req_1", {})
    fav_count = None
    fav_count_text = ""
    if req1.get("code") == 0:
        m_numbers = req1.get("data", {}).get("m_numbers", {})
        m_show = req1.get("data", {}).get("m_show", {})
        fav_count = m_numbers.get(str(song_id))
        fav_count_text = m_show.get(str(song_id), "")

    total_comment_count = page0.get("TotalCmNum", 0)

    clist = page0.get("CommentList", {})
    _parse_comments(clist.get("Comments", []))

    if len(collected) < target and clist.get("HasMore"):
        max_pages = min(target // api_page_size + 2, 6)
        remaining = list(range(1, max_pages))
        with ThreadPoolExecutor(max_workers=min(len(remaining), 4)) as pool:
            futures = {pool.submit(_fetch_page, p, "GetHotCommentList"): p for p in remaining}
            results = {}
            for fut in as_completed(futures):
                try:
                    r = fut.result()
                    if r:
                        results[futures[fut]] = r
                except Exception:
                    pass
        for pn in sorted(results):
            if len(collected) >= target:
                break
            _parse_comments(results[pn].get("CommentList", {}).get("Comments", []))

    # === 阶段2：热评不够，用最新评论补齐（循环翻页直到拿够 target 或没有更多） ===
    if len(collected) < target:
        last_seq = ""
        max_new_rounds = (target - len(collected)) // api_page_size + 2  # 按缺口算轮数，多留余量
        for _ in range(max_new_rounds):
            if len(collected) >= target or time.monotonic() > _comment_deadline:
                break
            try:
                page_data = _fetch_page(0, method="GetNewCommentList", last_seq=last_seq)
                if not page_data:
                    break
                cdata = page_data.get("CommentList", {})
                new_comments = cdata.get("Comments", [])
                if not new_comments:
                    break
                _parse_comments(new_comments)
                # 用最后一条评论的 SeqNo 做分页游标
                last_seq = str(new_comments[-1].get("SeqNo", ""))
                if not cdata.get("HasMore") or not last_seq:
                    break
            except Exception:
                break

    return collected, total_comment_count, fav_count, fav_count_text


def _fetch_play_count(song_id, timeout=30):
    """获取歌曲播放/收藏量。"""
    try:
        data = _post_musicu({
            "comm": {"ct": 24, "cv": 0, "uin": _comment_uin},
            "req_0": {
                "module": "music.musicasset.SongFavRead",
                "method": "GetSongFansNumberById",
                "param": {"v_songId": [song_id]},
            },
        }, timeout=timeout)
        req = data.get("req_0", {})
        if req.get("code") != 0:
            return None, None
        m_numbers = req.get("data", {}).get("m_numbers", {})
        m_show = req.get("data", {}).get("m_show", {})
        count = m_numbers.get(str(song_id))
        show = m_show.get(str(song_id), "")
        return count, show
    except Exception:
        return None, None


# ============================================================
#  任务处理
# ============================================================

def process_task(task, output_dir, quality, cookie, uin, timeout, metadata_only=False, **kw):
    song_id = task["song_id"]
    payload = json.loads(task["source_payload"]) if task.get("source_payload") else {}
    singer_names = payload.get("singer_names") or []

    # 评论 + 收藏量一次性获取（已合并到 _fetch_hot_comments_direct 内部）
    comment_result = [None]  # (comments_list, total_comment_count, fav_count, fav_count_text)
    comment_error = [None]

    def _fetch_comments():
        try:
            comment_result[0] = _fetch_hot_comments_direct(song_id=song_id, song_mid=task["song_mid"], page_size=100, timeout=timeout)
        except Exception as exc:
            comment_error[0] = exc

    comment_thread = threading.Thread(target=_fetch_comments, daemon=True)
    comment_thread.start()
    comment_thread.join(timeout=50)  # 评论内部已有 45s 截止，这里多留 5s 余量

    if isinstance(comment_result[0], tuple) and len(comment_result[0]) == 4:
        comments, total_comment_count, fav_count, fav_count_text = comment_result[0]
    elif isinstance(comment_result[0], tuple):
        comments, total_comment_count = comment_result[0]
        fav_count, fav_count_text = None, ""
    else:
        comments, total_comment_count = comment_result[0] or [], 0
        fav_count, fav_count_text = None, ""
    comment_fetch_ok = bool(comments)
    comment_fetch_error = str(comment_error[0]) if comment_error[0] else None
    comment_source = "api" if comment_fetch_ok else None

    # API 失败或返回空评论时，回退到 Playwright
    if not comments and not kw.get("skip_playwright"):
        try:
            from QQMusicSpider.playwright_comments import fetch_hot_comments_via_playwright
            pw_comments, pw_source = fetch_hot_comments_via_playwright(
                song_mid=task["song_mid"],
                top_comments_limit=100,
                user_data_dir=kw.get("comment_fallback_profile_dir"),
                browser_channel=kw.get("comment_fallback_browser_channel", "msedge"),
                headful=kw.get("comment_fallback_headful", False),
                wait_seconds=kw.get("comment_fallback_wait_seconds", 8.0),
            )
            if pw_comments:
                comments = pw_comments
                comment_fetch_ok = True
                comment_fetch_error = None
                comment_source = pw_source
        except Exception as exc:
            if not comment_fetch_error:
                comment_fetch_error = f"playwright_fallback: {exc}"

    folder_name = build_song_folder_name(task.get("song_name"), singer_names, song_id)
    # 防御性去除末尾空格（Windows 路径安全，utils 已修复此问题，此处双重保障）
    folder_name = folder_name.strip()
    song_folder = Path(output_dir) / folder_name
    saved_audio_name = None

    cookie_holder = kw.get("cookie_holder")

    if metadata_only:
        song_folder.mkdir(parents=True, exist_ok=True)
    else:
        # 如果有 CookieHolder，使用动态 cookie
        if cookie_holder:
            cookie, uin = cookie_holder.get_auth()

        _jitter()
        try:
            download_info = fetch_download_info_with_fallback(
                song_mid=task["song_mid"], media_mid=task["media_mid"],
                quality=quality, cookie=cookie, uin=uin, timeout=timeout,
                prefer_thirdparty=kw.get("prefer_thirdparty", False),
            )
        except QQMusicDownloadError as first_err:
            # 第一次失败：如果有 CookieHolder，刷新 Cookie 后重试一次
            if cookie_holder:
                log.debug("下载失败，刷新 Cookie 后重试: %s (song_mid=%s)", first_err, task["song_mid"])
                cookie_holder.notify_download_failure()
                cookie, uin = cookie_holder.refresh()
                try:
                    download_info = fetch_download_info_with_fallback(
                        song_mid=task["song_mid"], media_mid=task["media_mid"],
                        quality=quality, cookie=cookie, uin=uin, timeout=timeout,
                    )
                except QQMusicDownloadError:
                    return None, "no_resource"
            else:
                return None, "no_resource"

        if cookie_holder:
            cookie_holder.notify_download_success()

        song_folder.mkdir(parents=True, exist_ok=True)
        audio_path = song_folder / sanitize_path_part(
            download_info["file_name"], f'{song_id}{download_info["extension"]}',
        )
        save_song_file(download_info["url"], audio_path, cookie=cookie, timeout=timeout)
        saved_audio_name = audio_path.name

    meta = {
        "song_id": song_id,
        "song_mid": task["song_mid"],
        "song_name": task.get("song_name"),
        "singer_names": singer_names,
        "album_name": task.get("album_name", ""),
        "fav_count": fav_count,
        "fav_count_text": fav_count_text,
        "total_comment_count": total_comment_count,
        "hot_comments": comments,
        "comment_count": len(comments),
        "comment_fetch_ok": comment_fetch_ok,
        "comment_source": comment_source,
        "comment_fetch_error": comment_fetch_error,
    }
    _meta_write_pool.submit(_write_meta, song_folder / "meta.json", meta)
    return song_folder, saved_audio_name


from concurrent.futures import ThreadPoolExecutor
_meta_write_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="meta-writer")

def _write_meta(path, meta):
    try:
        tmp_path = Path(str(path) + ".tmp")
        tmp_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(path)
    except Exception as e:
        log.error("写入 meta.json 失败: %s: %s", path, e)


# ============================================================
#  结果收集器：攒一批通过 API 回写
# ============================================================

class ResultCollector:
    def __init__(self, api_url, retry_failed, flush_interval=3.0):
        self._api_url = api_url
        self._retry_failed = retry_failed
        self._flush_interval = flush_interval
        self._lock = threading.Lock()
        self._done = []
        self._failed = []
        self._stop = threading.Event()
        self._flusher = threading.Thread(target=self._flush_loop, name="result-flusher", daemon=True)
        self._total_done = 0
        self._total_failed = 0
        self._start_time = time.monotonic()
        self._on_update_cb = None

    def set_on_update(self, cb):
        """设置统计更新回调: cb(total_done, total_failed, rate)"""
        self._on_update_cb = cb

    def start(self):
        self._flusher.start()

    def stop(self):
        self._stop.set()
        self._flusher.join(timeout=30)
        self._flush()

    def add_done(self, task_id, output_dir, audio_file_name):
        with self._lock:
            self._done.append({"task_id": task_id, "output_dir": output_dir, "audio_file_name": audio_file_name or ""})
            self._total_done += 1
            cb = self._on_update_cb
        if cb:
            _, _, rate = self.stats
            cb(self._total_done, self._total_failed, rate)

    def add_failed(self, task_id, error_message):
        with self._lock:
            self._failed.append({"task_id": task_id, "error": error_message})
            self._total_failed += 1
            cb = self._on_update_cb
        if cb:
            _, _, rate = self.stats
            cb(self._total_done, self._total_failed, rate)

    @property
    def stats(self):
        elapsed = time.monotonic() - self._start_time
        rate = self._total_done / elapsed * 60 if elapsed > 0 else 0
        return self._total_done, self._total_failed, rate

    def _flush_loop(self):
        while not self._stop.is_set():
            self._stop.wait(self._flush_interval)
            self._flush()

    def _flush(self):
        with self._lock:
            done_batch = self._done[:]
            failed_batch = self._failed[:]
            self._done.clear()
            self._failed.clear()

        if not done_batch and not failed_batch:
            return

        try:
            if done_batch:
                api_report_done(self._api_url, done_batch)
            if failed_batch:
                api_report_failed(self._api_url, failed_batch, requeue=self._retry_failed)
        except Exception as exc:
            log.error("API 回写失败: %s，将在下轮重试", exc)
            with self._lock:
                self._done.extend(done_batch)
                self._failed.extend(failed_batch)


# ============================================================
#  Feeder / Worker 线程
# ============================================================

# 可重试的瞬时网络异常（区别于业务错误，不计入失败统计，直接重试）
_RETRYABLE = (ConnectionError, TimeoutError,
              urllib3.exceptions.NewConnectionError,
              urllib3.exceptions.MaxRetryError,
              urllib3.exceptions.ProtocolError,
              urllib3.exceptions.ReadTimeoutError,
              urllib3.exceptions.ConnectTimeoutError)
_MAX_TASK_RETRIES = 2  # 每个任务最多重试 2 次（共 3 次尝试）


def config_updater_loop(api_url, stop_event):
    """定时从中央控制台同步拉取最新的第三方收费解析 Keys。"""
    while not stop_event.is_set():
        try:
            resp = _api_http.request("GET", f"{api_url}/keys/get", timeout=5)
            if resp.status == 200:
                data = json.loads(resp.data.decode("utf-8"))
                keys = data.get("keys", [])
                if keys:
                    set_xianyuw_keys(keys)
                    log.info("[Config] 已成功从中央节点同步最新第三方 Keys，池子容量: %d", len(keys))
        except Exception:
            pass
        stop_event.wait(600)  # 每 10 分钟拉一次


def feeder_loop(args, local_queue, stop_event):
    consecutive_errors = 0
    while not stop_event.is_set():
        if local_queue.qsize() >= args.batch_size:
            stop_event.wait(0.2)
            continue
        try:
            tasks = api_claim_tasks(args.api_url, args.worker_id, args.batch_size, args.lease_seconds)
            consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            backoff = min(0.5 * (2 ** min(consecutive_errors - 1, 7)), 60)
            log.error("领取任务失败 (连续第%d次): %s，%ds 后重试", consecutive_errors, exc, backoff)
            stop_event.wait(backoff)
            continue

        if not tasks:
            log.debug("无可用任务，等待 %ds", args.idle_seconds)
            stop_event.wait(args.idle_seconds)
            continue

        log.info("领取 %d 条任务，本地队列 %d", len(tasks), local_queue.qsize())
        for t in tasks:
            local_queue.put(t)


def worker_loop(worker_index, args, local_queue, collector, cookie_holder, state, stop_event):
    thread_id = f"{args.worker_id}-t{worker_index}" if args.threads > 1 else args.worker_id
    local_count = 0
    
    # 磁盘空间检查频率节制（每 60 秒检查一次，避免 NAS 响应慢导致拉长处理周期）
    _last_disk_check = 0.0
    _disk_ok = True

    while not stop_event.is_set():
        if args.max_tasks and not _reserve_slot(state, args.max_tasks):
            break
        try:
            task = local_queue.get(timeout=5)
        except queue.Empty:
            if args.max_tasks:
                _release_slot(state)
            continue

        try:
            now = time.monotonic()
            if now - _last_disk_check > 60:
                usage = shutil.disk_usage(args.output_dir)
                _disk_ok = usage.free / (1024 ** 3) >= args.min_free_space_gb
                _last_disk_check = now
            
            if not _disk_ok:
                log.error("[%s] ⛔ 磁盘空间不足保护触发! 中止分机节点工作!", thread_id)
                local_queue.put(task)
                stop_event.set()
                break
        except Exception as e:
            log.warning("[%s] 磁盘检查异常 (NAS 可能响应慢): %s", thread_id, e)

        for _retry in range(_MAX_TASK_RETRIES + 1):
            try:
                log.debug("[%s] Start processing song_id=%s", thread_id, task["song_id"])
                cookie, uin = cookie_holder.get_auth()
                song_folder, saved_audio = process_task(
                    task=task, output_dir=args.output_dir, quality=args.download_quality,
                    cookie=cookie, uin=uin, timeout=args.timeout,
                    metadata_only=args.metadata_only,
                    skip_playwright=getattr(args, 'skip_playwright', False),
                    comment_fallback_profile_dir=getattr(args, 'comment_fallback_profile_dir', None),
                    comment_fallback_browser_channel=getattr(args, 'comment_fallback_browser_channel', 'msedge'),
                    comment_fallback_wait_seconds=getattr(args, 'comment_fallback_wait_seconds', 8.0),
                    comment_fallback_headful=getattr(args, 'comment_fallback_headful', False),
                    cookie_holder=cookie_holder,
                    prefer_thirdparty=args.prefer_thirdparty,
                )
            except _RETRYABLE as exc:
                # 瞬时网络错误：指数退避后重试，不计入失败
                if _retry < _MAX_TASK_RETRIES and not stop_event.is_set():
                    _wait = 2 ** _retry
                    log.warning("[%s] 网络抖动 (%d/%d)，%ds 后重试 song_id=%s: %s",
                                thread_id, _retry + 1, _MAX_TASK_RETRIES, _wait, task["song_id"], exc)
                    stop_event.wait(_wait)
                    continue
                collector.add_failed(task["id"], f"网络错误(重试耗尽): {exc}")
                log.warning("[%s] failed song_id=%s: %s", thread_id, task["song_id"], exc)
                break
            except (QQMusicMetadataError, QQMusicDownloadError, OSError, json.JSONDecodeError) as exc:
                collector.add_failed(task["id"], str(exc))
                log.warning("[%s] failed song_id=%s: %s", thread_id, task["song_id"], exc)
                break
            except Exception as exc:
                collector.add_failed(task["id"], f"Unexpected: {exc}")
                log.warning("[%s] failed song_id=%s (unexpected): %s", thread_id, task["song_id"], exc)
                break
            else:
                # 成功
                if saved_audio == "no_resource":
                    collector.add_done(task["id"], "", "no_resource")
                    local_count += 1
                    log.info("[%s] no_resource #%d: %s", thread_id, local_count, task.get("song_name", task["song_id"]))
                else:
                    collector.add_done(task["id"], str(song_folder), saved_audio)
                    local_count += 1
                    log.info("[%s] scraped #%d: %s", thread_id, local_count, task.get("song_name", task["song_id"]))
                break

        # ── 任务间限速 ──
        if args.task_interval > 0:
            stop_event.wait(args.task_interval)


def _reserve_slot(state, max_tasks):
    with state["lock"]:
        if state["claimed"] >= max_tasks:
            return False
        state["claimed"] += 1
        return True


def _release_slot(state):
    with state["lock"]:
        if state["claimed"] > 0:
            state["claimed"] -= 1


def resolve_download_auth(args):
    if has_explicit_auth(args.qqmusic_cookie, args.qqmusic_uin):
        return args.qqmusic_cookie, args.qqmusic_uin

    # ── 优先从中央 Cookie API 拉取 ──────────────────────────────
    for attempt in range(3):
        try:
            resp = _api_http.request(
                "GET", f"{args.api_url}/cookies/get",
                timeout=urllib3.Timeout(connect=5, read=10),
            )
            if resp.status == 200:
                data = json.loads(resp.data.decode("utf-8"))
                cookie = data.get("cookie", "")
                uin = data.get("uin", "0")
                if cookie:
                    log.info("[启动] 从中央 Cookie 池获取 VIP Cookie 成功  uin=%s", uin)
                    return cookie, uin
            log.warning("[启动] 中央 Cookie 池暂无有效 Cookie (attempt %d/3)", attempt + 1)
        except Exception as exc:
            log.warning("[启动] 拉取中央 Cookie 失败: %s (attempt %d/3)", exc, attempt + 1)
        if attempt < 2:
            time.sleep(3)

    # ── 降级：本地 Playwright profile ────────────────────────────
    try:
        cookie, uin = load_auth_from_playwright_profile(
            user_data_dir=args.comment_fallback_profile_dir,
            browser_channel=args.comment_fallback_browser_channel,
            headful=args.comment_fallback_headful,
        )
        log.info("[启动] 从本地 Playwright Profile 获取 Cookie  uin=%s", uin)
        return cookie, uin
    except QQMusicDownloadError:
        pass

    log.warning("[启动] 未获取到任何 Cookie，将以空 Cookie 启动（仅能下载低音质）")
    return args.qqmusic_cookie, args.qqmusic_uin


# Cookie 自动刷新间隔（秒）
COOKIE_REFRESH_INTERVAL = int(os.getenv("QQMUSIC_COOKIE_REFRESH_INTERVAL", "1800"))  # 默认 30 分钟


class CookieHolder:
    """线程安全的 Cookie 持有者，支持定期从 Playwright profile 自动刷新。"""

    def __init__(self, cookie, uin, args):
        self._cookie = cookie
        self._uin = uin
        self._args = args
        self._lock = threading.Lock()
        self._refresh_lock = threading.Lock()  # 独立的刷新锁，防止并发启动 Playwright
        self._last_refresh = time.monotonic()
        self._refresh_interval = COOKIE_REFRESH_INTERVAL
        self._consecutive_failures = 0
        self._playwright_unavailable = False  # 一旦确认 Playwright 不可用，永久跳过刷新
        # 初始 Cookie 为空时，强制第一次 get_auth() 立即触发刷新（从中央 API 拉取）
        if not cookie:
            self._last_refresh = 0.0

    @property
    def cookie(self):
        with self._lock:
            return self._cookie

    @property
    def uin(self):
        with self._lock:
            return self._uin

    def get_auth(self):
        with self._lock:
            elapsed = time.monotonic() - self._last_refresh
            need_refresh = elapsed >= self._refresh_interval
        if need_refresh:
            self.refresh()
        with self._lock:
            return self._cookie, self._uin

    def notify_download_failure(self):
        with self._lock:
            self._consecutive_failures += 1
            should_refresh = self._consecutive_failures >= 5
        if should_refresh:
            self.refresh()

    def notify_download_success(self):
        with self._lock:
            self._consecutive_failures = 0

    def _try_fetch_from_api(self):
        """从中央 API /cookies/get 拉取最新有效 Cookie，成功返回 (cookie, uin)，失败返回 None。"""
        try:
            resp = _api_http.request(
                "GET", f"{self._args.api_url}/cookies/get",
                timeout=5,
            )
            if resp.status == 404:
                return None   # 池子里暂时没有有效 Cookie
            if resp.status != 200:
                log.debug("[CookieHolder] /cookies/get 返回 %d，跳过", resp.status)
                return None
            data = json.loads(resp.data.decode("utf-8"))
            cookie = data.get("cookie", "")
            uin    = data.get("uin", "0")
            if cookie:
                if data.get("stale"):
                    log.debug("[CookieHolder] 中央 Cookie 池已陈旧（>20min），仍采用")
                return cookie, uin
        except Exception as exc:
            log.debug("[CookieHolder] 从中央 API 拉取 Cookie 失败: %s", exc)
        return None

    def refresh(self):
        # 用 _refresh_lock 保证同一时刻只有一个线程在刷新
        if not self._refresh_lock.acquire(blocking=False):
            # 已有线程在刷新，等它完成后直接返回结果
            with self._refresh_lock:
                with self._lock:
                    return self._cookie, self._uin
        try:
            with self._lock:
                # 10 秒内不重复刷新
                if time.monotonic() - self._last_refresh < 10:
                    return self._cookie, self._uin

            # ── 优先：从中央 Cookie API 拉取 ──────────────────────
            result = self._try_fetch_from_api()
            if result:
                cookie, uin = result
                with self._lock:
                    old_cookie = self._cookie
                    self._cookie = cookie
                    self._uin = uin
                    self._last_refresh = time.monotonic()
                    self._consecutive_failures = 0
                    changed = cookie != old_cookie
                if changed:
                    log.info("[CookieHolder] Cookie 已从中央 API 更新  uin=%s", uin)
                    set_comment_auth(cookie, uin)
                else:
                    log.debug("[CookieHolder] 中央 API Cookie 未变化")
                return cookie, uin

            # ── 降级：从本机 Playwright Profile 读取 ──────────────
            if self._playwright_unavailable:
                # 已知不可用，静默跳过
                with self._lock:
                    return self._cookie, self._uin

            try:
                cookie, uin = load_auth_from_playwright_profile(
                    user_data_dir=self._args.comment_fallback_profile_dir,
                    browser_channel=self._args.comment_fallback_browser_channel,
                    headful=self._args.comment_fallback_headful,
                )
                with self._lock:
                    old_cookie = self._cookie
                    self._cookie = cookie
                    self._uin = uin
                    self._last_refresh = time.monotonic()
                    self._consecutive_failures = 0
                    changed = cookie != old_cookie
                if changed:
                    log.info("[CookieHolder] Cookie 已从本机 Profile 刷新  uin=%s", uin)
                    set_comment_auth(cookie, uin)
                else:
                    log.debug("[CookieHolder] 本机 Profile Cookie 未变化")
                return cookie, uin
            except QQMusicDownloadError as exc:
                if "Playwright is not installed" in str(exc):
                    self._playwright_unavailable = True
                    log.warning(
                        "[CookieHolder] Playwright 未安装，本机刷新已禁用"
                        "（将持续依赖中央 Cookie API）"
                    )
                else:
                    log.warning("[CookieHolder] 本机 Profile 刷新失败: %s", exc)
                with self._lock:
                    self._last_refresh = time.monotonic()
                    return self._cookie, self._uin
        finally:
            self._refresh_lock.release()


# ============================================================
#  主入口
# ============================================================

def main():
    args = parse_args()
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    if args.max_tasks:
        args.batch_size = min(args.batch_size, args.max_tasks)

    set_rate_limit(args.api_qps)
    set_vkey_rate_limit(100)  # 官方 VKey API 限速：每分钟最多 100 次
    _init_comment_rate_limit(60)  # 评论 API 限速：每分钟最多 60 次
    log.info("[%s] API 服务: %s", args.worker_id, args.api_url)
    log.info("[%s] VKey API 限速: 100 次/分, 并发5 | 评论 API 限速: 60 次/分, 并发3", args.worker_id)
    log.info("[%s] 模式: %s | 线程: %d | 批量: %d | 输出: %s",
             args.worker_id, "纯元数据" if args.metadata_only else "含下载",
             args.threads, args.batch_size, args.output_dir)

    if args.metadata_only:
        download_cookie, download_uin = args.qqmusic_cookie, args.qqmusic_uin
    else:
        download_cookie, download_uin = resolve_download_auth(args)

    # 创建 CookieHolder，支持自动刷新
    cookie_holder = CookieHolder(download_cookie, download_uin, args)
    log.info("[%s] Cookie 自动刷新间隔: %ds", args.worker_id, COOKIE_REFRESH_INTERVAL)

    # 评论接口也需要 cookie 鉴权
    set_comment_auth(download_cookie, args.qqmusic_uin)

    state = {"lock": threading.Lock(), "claimed": 0}
    local_queue = queue.Queue(maxsize=args.batch_size * 2)
    stop_event = threading.Event()

    collector = ResultCollector(args.api_url, retry_failed=args.retry_failed, flush_interval=args.flush_interval)
    collector.start()

    # 启动配置热更新线程
    threading.Thread(target=config_updater_loop, args=(args.api_url, stop_event), name="config-updater", daemon=True).start()

    feeder = threading.Thread(target=feeder_loop, args=(args, local_queue, stop_event), name="feeder", daemon=True)
    feeder.start()

    workers = []
    for i in range(1, args.threads + 1):
        w = threading.Thread(
            target=worker_loop,
            args=(i, args, local_queue, collector, cookie_holder, state, stop_event),
            name=f"worker-{i}",
        )
        w.start()
        workers.append(w)

    def progress_loop():
        while not stop_event.is_set():
            stop_event.wait(3)
            if stop_event.is_set():
                break
            done, failed, rate = collector.stats
            sys.stdout.write(f"\r  [进度] 完成: {done:,}  失败: {failed:,}  速率: {rate:,.0f} 首/分钟  队列: {local_queue.qsize()}    ")
            sys.stdout.flush()

    threading.Thread(target=progress_loop, name="progress", daemon=True).start()

    def signal_handler(sig, frame):
        print(f"\n\n[主要] 收到退出信号 ({sig})，正在安全停止工作并回写数据请稍候...\n")
        stop_event.set()
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    if hasattr(signal, 'SIGBREAK'):
        signal.signal(signal.SIGBREAK, signal_handler)

    try:
        # 轮询等待，让主线程能响应信号
        while not stop_event.is_set() and any(w.is_alive() for w in workers):
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n\n[主要] 捕获到 Ctrl+C，正在安全退出...\n")
        stop_event.set()

    print()
    log.info("[%s] 正在等待工作线程结束...", args.worker_id)
    for w in workers:
        if w.is_alive():
            w.join(timeout=3.0)

    log.info("[%s] 正在停止数据收集器并同步最后的数据...", args.worker_id)
    collector.stop()
    done, failed, rate = collector.stats
    log.info("[%s] 全部完成: %d 成功, %d 失败, 平均 %.0f 首/分钟", args.worker_id, done, failed, rate)


if __name__ == "__main__":
    main()
