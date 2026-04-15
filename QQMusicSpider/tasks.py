# -*- coding: utf-8 -*-
"""
QQ 音乐元数据抓取函数（不依赖 Scrapy）。
提供歌曲发现、评论抓取、歌词抓取等独立函数，供分布式 Worker 和补救脚本使用。
"""

import json
import threading
import time
import urllib.parse

import urllib3

from QQMusicSpider.playwright_comments import (
    QQMusicPlaywrightError,
    fetch_hot_comments_via_playwright,
)
from QQMusicSpider.utils import (
    build_song_folder_name,
    parse_song_info,
    process_lyric,
    random_user_agent,
    sanitize_path_part,
)


class RateLimiter:
    """线程安全的限速器。锁内只做时间槽分配，sleep 在锁外执行，不阻塞其他线程。"""

    def __init__(self, qps=50):
        self._interval = 1.0 / qps
        self._lock = threading.Lock()
        self._next_slot = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            if self._next_slot <= now:
                self._next_slot = now + self._interval
                wait_time = 0.0
            else:
                wait_time = self._next_slot - now
                self._next_slot += self._interval
        if wait_time > 0:
            time.sleep(wait_time)


# 全局限速器：每台机器默认 50 QPS，300 台共 15000 QPS
_rate_limiter = RateLimiter(qps=50)


def set_rate_limit(qps):
    """允许外部调整限速。"""
    global _rate_limiter
    _rate_limiter = RateLimiter(qps=qps)


# 全局 urllib3 连接池，复用 TCP 连接避免每次请求都握手
_http_pool = urllib3.PoolManager(
    num_pools=20,
    maxsize=50,
    retries=urllib3.Retry(total=2, backoff_factor=0.3, status_forcelist=[500, 502, 503]),
    timeout=urllib3.Timeout(connect=10, read=30),
)

SINGER_LIST_URL_TEMPLATE = (
    "https://u.y.qq.com/cgi-bin/musicu.fcg?data="
    "%7B%22singerList%22%3A%7B%22module%22%3A%22Music.SingerListServer%22%2C"
    "%22method%22%3A%22get_singer_list%22%2C%22param%22%3A%7B%22area%22%3A{area}%2C"
    "%22sex%22%3A-100%2C%22genre%22%3A-100%2C%22index%22%3A-100%2C%22sin%22%3A{index}%2C"
    "%22cur_page%22%3A{cur_page}%7D%7D%7D"
)
SINGER_SONG_LIST_URL_TEMPLATE = (
    "https://u.y.qq.com/cgi-bin/musicu.fcg?data="
    "%7B%22comm%22%3A%7B%22ct%22%3A24%2C%22cv%22%3A0%7D%2C%22singerSongList%22%3A"
    "%7B%22method%22%3A%22GetSingerSongList%22%2C%22param%22%3A%7B%22order%22%3A1%2C"
    "%22singerMid%22%3A%22{singer_mid}%22%2C%22begin%22%3A{begin}%2C%22num%22%3A{num}%7D%2C"
    "%22module%22%3A%22musichall.song_list_server%22%7D%7D"
)
LYRIC_URL_TEMPLATE = "https://c.y.qq.com/lyric/fcgi-bin/fcg_query_lyric_yqq.fcg?nobase64=1&musicid={song_id}&format=json"
SONG_PAGE_REFERER = "https://y.qq.com/n/yqq/song/{song_mid}.html"


class QQMusicMetadataError(Exception):
    pass


def fetch_json(url, headers=None, timeout=30):
    _rate_limiter.wait()
    merged_headers = {"User-Agent": random_user_agent()}
    if headers:
        merged_headers.update(headers)
    try:
        resp = _http_pool.request("GET", url, headers=merged_headers, timeout=timeout)
        return resp.status, json.loads(resp.data.decode("utf-8"))
    except urllib3.exceptions.HTTPError as exc:
        raise QQMusicMetadataError(str(exc)) from exc
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise QQMusicMetadataError(str(exc)) from exc


def iterate_discovery_tasks(singer_page_num, singer_page_size, song_page_num, song_page_size, areas, timeout=30):
    for area in areas:
        for page in range(1, singer_page_num + 1):
            url = SINGER_LIST_URL_TEMPLATE.format(
                area=area,
                index=singer_page_size * (page - 1),
                cur_page=page,
            )
            _, payload = fetch_json(url, timeout=timeout)
            singer_list = payload.get("singerList", {}).get("data", {}).get("singerlist", [])
            if not isinstance(singer_list, list):
                continue

            for singer in singer_list:
                singer_mid = (singer or {}).get("singer_mid")
                if not singer_mid:
                    continue

                for song_page in range(song_page_num):
                    song_url = SINGER_SONG_LIST_URL_TEMPLATE.format(
                        singer_mid=singer_mid,
                        begin=song_page * song_page_size,
                        num=song_page_size,
                    )
                    _, song_payload = fetch_json(song_url, timeout=timeout)
                    song_list = song_payload.get("singerSongList", {}).get("data", {}).get("songList", [])
                    if not isinstance(song_list, list):
                        continue

                    for song in song_list:
                        raw_info = (song or {}).get("songInfo") or {}
                        task = parse_song_info(raw_info)
                        if task and task["song_id"] and task["song_mid"]:
                            yield task


def fetch_hot_comments(song_id, song_mid, page_size=100, timeout=30):
    """使用 u.y.qq.com 的 GetHotCommentList 接口获取热评及新评，支持 LastCommentSeqNo 翻页。"""
    collected = []
    seen = set()
    last_seq = ""
    api_page_size = 25
    max_pages = max(page_size // api_page_size + 1, 4)

    # 1. 尝试热评
    for page in range(max_pages):
        payload = {
            "hotComment": {
                "module": "music.globalComment.CommentRead",
                "method": "GetHotCommentList",
                "param": {
                    "BizType": 1, "BizId": str(song_id),
                    "LastCommentSeqNo": last_seq, "PageSize": api_page_size,
                    "PageNum": page, "HotType": 1,
                    "WithAirborne": 0, "PicEnable": 1
                }
            }
        }
        url = f"https://u.y.qq.com/cgi-bin/musicu.fcg?data={urllib.parse.quote(json.dumps(payload))}"
        status_code, response = fetch_json(url, timeout=timeout)
        if status_code != 200:
            break

        cd = response.get("hotComment", {})
        if cd.get("code") != 0:
            break
        cl = cd.get("data", {}).get("CommentList", {})
        comments = cl.get("Comments", [])
        if not comments:
            break

        for c in comments:
            if len(collected) >= page_size:
                break
            cm_id = str(c.get("CmId", ""))
            if cm_id and cm_id in seen:
                continue
            if cm_id: seen.add(cm_id)
            collected.append({
                "comment_name": c.get("Nick"),
                "comment_text": c.get("Content"),
            })

        if len(collected) >= page_size or not cl.get("HasMore"):
            break
        last_seq = str(comments[-1].get("SeqNo", ""))

    # 2. 如果热评不足，补充新评
    if len(collected) < 10:
        payload = {
            "newComment": {
                "module": "music.globalComment.CommentRead",
                "method": "GetNewCommentList",
                "param": {
                    "BizType": 1, "BizId": str(song_id),
                    "LastCommentSeqNo": "", "PageSize": int(page_size), "PageNum": 0
                }
            }
        }
        url = f"https://u.y.qq.com/cgi-bin/musicu.fcg?data={urllib.parse.quote(json.dumps(payload))}"
        _, response = fetch_json(url, timeout=timeout)
        for c in response.get("newComment", {}).get("data", {}).get("CommentList", {}).get("Comments", []):
            if len(collected) >= page_size:
                break
            text = c.get("Content")
            if any(text == existing["comment_text"] for existing in collected):
                continue
            collected.append({
                "comment_name": c.get("Nick"),
                "comment_text": text,
            })

    return collected[:page_size]


def fetch_hot_comments_with_fallback(
    song_id,
    song_mid,
    page_size=100,
    timeout=30,
    skip_playwright=False,
    fallback_user_data_dir=None,
    fallback_headful=False,
    fallback_browser_channel="msedge",
    fallback_wait_seconds=8.0,
):
    errors = []
    try:
        comments = fetch_hot_comments(song_id=song_id, song_mid=song_mid, page_size=page_size, timeout=timeout)
        if comments:
            return comments, True, None, "legacy_api"
        errors.append("No hot comments returned")
    except QQMusicMetadataError as exc:
        errors.append(str(exc))

    if not skip_playwright:
        try:
            comments, source = fetch_hot_comments_via_playwright(
                song_mid=song_mid,
                top_comments_limit=min(page_size, 100),
                user_data_dir=fallback_user_data_dir,
                headful=fallback_headful,
                browser_channel=fallback_browser_channel,
                wait_seconds=fallback_wait_seconds,
            )
            if comments:
                return comments, True, None, source
            errors.append("Playwright fallback returned no comments")
        except (QQMusicMetadataError, QQMusicPlaywrightError) as exc:
            errors.append(str(exc))

    return [], False, " | ".join(errors) if errors else "Unable to fetch comments", None


def fetch_lyric(song_id, song_mid, timeout=30):
    status_code, payload = fetch_json(
        LYRIC_URL_TEMPLATE.format(song_id=song_id),
        headers={"Referer": SONG_PAGE_REFERER.format(song_mid=song_mid)},
        timeout=timeout,
    )
    if status_code != 200:
        return ""
    if payload.get("retcode") != 0:
        return ""
    return process_lyric(payload.get("lyric", ""))
