# -*- coding: utf-8 -*-

import json
import random
import re
import threading
import time
import urllib.parse
from pathlib import Path

import urllib3

from QQMusicSpider.utils import random_user_agent

try:
    from playwright.sync_api import sync_playwright
except ModuleNotFoundError:  # pragma: no cover - optional runtime dependency
    sync_playwright = None


# API 请求连接池（获取 vkey、第三方 API 等短请求）
_api_pool = urllib3.PoolManager(
    num_pools=10,
    maxsize=30,
    retries=urllib3.Retry(
        total=3, backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    ),
    timeout=urllib3.Timeout(connect=10, read=30),
)

# 咸鱼专用连接池（双代理端口自动探测）
_XIANYUW_PROXY_PORTS = [7890, 7897]

def _create_xianyuw_pool():
    """尝试双代理端口，哪个通用哪个；都不通则直连。"""
    for port in _XIANYUW_PROXY_PORTS:
        proxy_url = f"http://127.0.0.1:{port}"
        try:
            test_pool = urllib3.ProxyManager(
                proxy_url, num_pools=1, maxsize=1,
                timeout=urllib3.Timeout(connect=2, read=3),
            )
            test_pool.request("GET", "https://apii.xianyuw.cn", preload_content=False).release_conn()
            print(f"[proxy] 咸鱼代理探测成功: {proxy_url}")
            return urllib3.ProxyManager(
                proxy_url, num_pools=4, maxsize=10,
                retries=urllib3.Retry(total=1, backoff_factor=0.5, raise_on_status=False),
                timeout=urllib3.Timeout(connect=3, read=5),
            )
        except Exception:
            print(f"[proxy] 端口 {port} 不可用，尝试下一个...")
    print("[proxy] 所有代理端口均不可用，咸鱼使用直连")
    return _api_pool

_xianyuw_pool = _create_xianyuw_pool()

# 下载专用连接池：与 API 池严格隔离，防止流式下载残留数据污染 API 连接
_download_pool = urllib3.PoolManager(
    num_pools=4,
    maxsize=8,
    retries=urllib3.Retry(
        total=3, backoff_factor=2.0,
        status_forcelist=[500, 502, 503],
        allowed_methods=["GET"],
    ),
    timeout=urllib3.Timeout(connect=10, read=120),
)
DOWNLOAD_URL = "https://u.y.qq.com/cgi-bin/musicu.fcg"
PLAY_SIGN = "zzannc1o6o9b4i971602f3554385022046ab796512b7012"
FILE_TYPES = {
    "m4a": {"prefix": "C400", "extension": ".m4a"},
    "128": {"prefix": "M500", "extension": ".mp3"},
    "320": {"prefix": "M800", "extension": ".mp3"},
    "ape": {"prefix": "A000", "extension": ".ape"},
    "flac": {"prefix": "F000", "extension": ".flac"},
}


class QQMusicDownloadError(Exception):
    pass


def normalize_cookie(cookie):
    text = str(cookie or "").strip()
    if not text:
        return ""

    lowered = text.lower()
    if lowered in {"cookie", "your_cookie", "你的cookie", "浣犵殑cookie"}:
        return ""

    try:
        text.encode("latin-1")
    except UnicodeEncodeError:
        return ""
    return text


def normalize_uin(configured_uin):
    text = str(configured_uin or "").strip()
    if not text:
        return "0"

    lowered = text.lower()
    if lowered in {"uin", "your_uin", "你的uin", "浣犵殑uin"}:
        return "0"

    if text.isdigit():
        return text
    return "0"


def has_explicit_auth(cookie="", uin="0"):
    return bool(normalize_cookie(cookie)) or normalize_uin(uin) != "0"


def resolve_uin(cookie, configured_uin):
    text = normalize_uin(configured_uin)
    if text and text != "0":
        return text

    cookie = normalize_cookie(cookie)
    if not cookie:
        return "0"

    match = re.search(r"(?:^|;\s*)uin=([^;]+)", cookie)
    if match:
        return match.group(1).strip()
    return "0"


def load_auth_from_playwright_profile(user_data_dir, browser_channel="msedge", headful=False):
    if sync_playwright is None:
        raise QQMusicDownloadError("Playwright is not installed")

    profile_dir = Path(user_data_dir or "").expanduser()
    if not profile_dir.exists():
        raise QQMusicDownloadError(f"Playwright profile does not exist: {profile_dir}")

    try:
        with sync_playwright() as playwright:
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                channel=browser_channel or None,
                headless=not headful,
                args=["--disable-blink-features=AutomationControlled"],
                locale="zh-CN",
                timezone_id="Asia/Shanghai",
            )
            try:
                raw_cookies = context.cookies(
                    [
                        "https://y.qq.com",
                        "https://qq.com",
                        "https://u.y.qq.com",
                        "https://c.y.qq.com",
                    ]
                )
            finally:
                context.close()
    except Exception as exc:
        raise QQMusicDownloadError(f"Failed to load auth from Playwright profile: {exc}") from exc

    qq_cookies = []
    seen_names = set()
    for item in raw_cookies or []:
        domain = str(item.get("domain") or "")
        name = str(item.get("name") or "").strip()
        value = str(item.get("value") or "")
        if not name or name in seen_names:
            continue
        if "qq.com" not in domain:
            continue
        qq_cookies.append(f"{name}={value}")
        seen_names.add(name)

    cookie = "; ".join(qq_cookies)
    cookie = normalize_cookie(cookie)
    if not cookie:
        raise QQMusicDownloadError("No QQ cookies found in Playwright profile")

    uin = resolve_uin(cookie, "0")
    return cookie, uin


def build_headers(cookie="", host="u.y.qq.com"):
    headers = {
        "User-Agent": random_user_agent(),
        "Referer": "https://y.qq.com/portal/player.html",
        "Host": host,
    }
    cookie = normalize_cookie(cookie)
    if cookie:
        headers["Cookie"] = cookie
    return headers


def resolve_play_path(play_info):
    """从 play_info 中提取可用的播放路径覆盖多种音质。"""
    for key in (
        "purl", "wifiurl", "flowurl", "opi128kurl", "opi192kurl",
        "opi96kurl", "opi48kurl", "opiflackurl", "opi30surl",
    ):
        value = play_info.get(key) or ""
        if value:
            return value
    return ""


QUALITY_FALLBACK_ORDER = ["flac", "ape", "320", "128", "m4a"]

# ============================================================
#  第三方 API fallback（官方接口失败时降级）
# ============================================================

# VKeys API 音质映射: 值越大音质越高
_VKEYS_QUALITIES = [14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]

# Tang API / NKI API 音质字段优先级
_THIRDPARTY_URL_KEYS = [
    "song_play_url_sq", "song_play_url_pq", "song_play_url_accom",
    "song_play_url_hq", "song_play_url", "song_play_url_standard", "song_play_url_fq",
]


_LOSSLESS_EXTS = {"flac", "ape", "wav"}

# 限制第三方 API 并发数，防止远端服务器被打爆而断连
_VKEYS_SEMAPHORE   = threading.Semaphore(15)   # vkeys 最多同时 5 个请求
_XIANYUW_SEMAPHORE = threading.Semaphore(25)   # xianyuw 最多同时 12 个请求
_YAOHU_SEMAPHORE   = threading.Semaphore(20)   # 妖狐数据 API 并发限制


class _TokenBucket:
    """线程安全令牌桶：限制单机请求频率（每分钟 N 次）。"""

    def __init__(self, rate_per_min: int):
        self.rate = rate_per_min
        self.tokens = float(rate_per_min)
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, timeout: float = 60.0) -> bool:
        """阻塞直到获取一个令牌，超时返回 False。"""
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / 60.0))
                self.last_refill = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(0.5, remaining))


# 咸鱼 API 单 IP 限制 100 次/分钟，留余量控制在 90
_XIANYUW_RATE_LIMITER = _TokenBucket(50)


class _CircuitBreaker:
    """熔断器：连续失败 N 次后冷却一段时间，避免白烧请求。"""

    def __init__(self, name: str, fail_threshold: int = 5, cooldown_secs: float = 60):
        self.name = name
        self.fail_threshold = fail_threshold
        self.cooldown_secs = cooldown_secs
        self._fail_count = 0
        self._cooldown_until = 0.0
        self._lock = threading.Lock()

    def is_open(self) -> bool:
        """冷却中返回 True（熔断打开，应跳过请求）。"""
        return time.monotonic() < self._cooldown_until

    def record_success(self):
        with self._lock:
            self._fail_count = 0

    def record_failure(self):
        with self._lock:
            self._fail_count += 1
            if self._fail_count >= self.fail_threshold:
                self._cooldown_until = time.monotonic() + self.cooldown_secs
                print(f"[{self.name}] 连续 {self._fail_count} 次失败，冷却 {self.cooldown_secs}s")
                self._fail_count = 0


_VKEYS_BREAKER   = _CircuitBreaker("VKeys",   fail_threshold=10, cooldown_secs=60)
_YAOHU_BREAKER   = _CircuitBreaker("Yaohu",   fail_threshold=10, cooldown_secs=60)
_XIANYUW_BREAKER = _CircuitBreaker("Xianyuw", fail_threshold=10, cooldown_secs=60)


def _fetch_from_vkeys(song_mid, timeout=10, lossless_only=False):
    """通过 api.vkeys.cn 获取下载链接。只请求一次最高档位，没有就放弃。"""
    if _VKEYS_BREAKER.is_open():
        return None

    quality = _VKEYS_QUALITIES[0]  # 只试最高档位
    try:
        with _VKEYS_SEMAPHORE:
            resp = _api_pool.request(
                "GET", f"https://api.vkeys.cn/v2/music/tencent/geturl?mid={song_mid}&quality={quality}",
                headers={"User-Agent": random_user_agent()},
                timeout=urllib3.Timeout(connect=3, read=5),
            )
        data = json.loads(resp.data.decode("utf-8"))
        url = (data.get("data") or {}).get("url", "")
        if url and url.startswith("http"):
            ext = url.split("?")[0].rsplit(".", 1)[-1].lower()
            if ext not in ("mp3", "flac", "m4a", "ogg", "ape", "wav"):
                ext = "mp3"
            if lossless_only and ext not in _LOSSLESS_EXTS:
                return None
            _VKEYS_BREAKER.record_success()
            return {"url": url, "file_name": f"{song_mid}.{ext}", "extension": f".{ext}",
                    "quality": f"vkeys-{quality}", "source": "vkeys"}
        else:
            _VKEYS_BREAKER.record_failure()
            return None
    except Exception:
        _VKEYS_BREAKER.record_failure()
        return None


_YAOHU_KEYS = ["9fheo4Yh7qHhVQhpk4c"]  # 妖狐 API Key

def _fetch_from_yaohu(song_mid, media_mid=None, timeout=15, lossless_only=False, cookie=""):
    """通过 api.yaohud.cn 获取下载链接。只请求一次最高音质，没有就放弃。"""
    if _YAOHU_BREAKER.is_open():
        return None

    import base64 as _b64
    key = random.choice(_YAOHU_KEYS)
    size = "sq" if lossless_only else "hires"  # 只试一个档位

    # 构建 Cookie 参数
    encoded_cookie = ""
    if cookie:
        try:
            qm_keyst = ""
            uin = ""
            m_keyst = re.search(r"qm_keyst=([^;]+)", cookie)
            if m_keyst: qm_keyst = m_keyst.group(1).strip()
            m_uin = re.search(r"uin=([^;]+)", cookie)
            if m_uin: uin = m_uin.group(1).strip()
            if qm_keyst and uin:
                target_cookie = f"qm_keyst={qm_keyst}; uin={uin}"
                encoded_cookie = _b64.b64encode(target_cookie.encode("utf-8")).decode("utf-8")
        except Exception:
            pass

    try:
        with _YAOHU_SEMAPHORE:
            params = {
                "key": key, "mid": song_mid, "type": "url",
                "size": size, "format": "json"
            }
            if media_mid:
                params["media_mid"] = media_mid
            if encoded_cookie:
                params["cookie"] = encoded_cookie

            query_str = urllib.parse.urlencode(params)
            url = f"https://api.yaohud.cn/api/qqmusic/v2?{query_str}"
            resp = _api_pool.request("GET", url, headers={"User-Agent": random_user_agent()},
                                     timeout=urllib3.Timeout(connect=3, read=5))

            if resp.status != 200:
                _YAOHU_BREAKER.record_failure()
                return None

            data = json.loads(resp.data.decode("utf-8"))
            if data.get("code") == 200:
                info = data.get("data") or {}
                play_url = info if isinstance(info, str) else info.get("url", "")
                if play_url and play_url.startswith("http"):
                    ext = play_url.split("?")[0].rsplit(".", 1)[-1].lower()
                    if ext not in ("mp3", "flac", "m4a", "ogg", "ape", "wav"):
                        ext = "flac" if size in ("hires", "sq") else "mp3"
                    _YAOHU_BREAKER.record_success()
                    return {
                        "url": play_url, "file_name": f"{song_mid}.{ext}",
                        "extension": f".{ext}", "quality": f"yaohu-{size}",
                        "source": "yaohu"
                    }
            _YAOHU_BREAKER.record_failure()
            return None
    except Exception:
        _YAOHU_BREAKER.record_failure()
        return None


def _fetch_from_thirdparty(song_mid, media_mid=None, timeout=10, lossless_only=False, cookie=""):
    """依次尝试第三方 API，返回第一个成功的结果。"""
    # 策略调整：妖狐数据优先于 xianyuw 和 vkeys
    fetch_funcs = [
        lambda: _fetch_from_yaohu(song_mid, media_mid, timeout, lossless_only, cookie=cookie),
        lambda: _fetch_from_vkeys(song_mid, timeout, lossless_only),
        lambda: _fetch_from_xianyuw(song_mid, timeout, lossless_only),
    ]
    for func in fetch_funcs:
        try:
            result = func()
            if result:
                # 统一设置 actual_quality
                result["actual_quality"] = result.get("quality", "thirdparty")
                return result
        except Exception:
            continue
    return None


_XIANYUW_KEYS = [
    "sk-c1671e560f93e781eb810047ac1894a3",
    "sk-c9f43eaaf82767433a8a544f6b6170b7",
    "sk-49038fd73bddd2d854bbe068181c85ab",
    "sk-7f61bbdc6fb2972e5d469630cecbfe27",
    "sk-28fc1c86b65dd7eb700d1b19697c7b46",
    "sk-98abe40b5d293d4dea6cf6dac4dfbca3",
    "sk-ef6f11b950ff1c3f760fc8757e5c1eed",
    "sk-3ad3fb7ddb9c90e27823c2a54ba358d6",
    "sk-4d96061060605923a1640d6e3b06bc73",
    "sk-81ea32cbc43aeb11aaded947b3f327bd",
    "sk-11e19e41f292e1c1cdfe4a9e608aa099",
    "sk-caf4075b026042466b0352338c0d9cbb",
    "sk-ba1665c28bfcec6c6976dc49b93c50a8",
    "sk-3d75d3477ab56e832324b80a1a4f3919",
    "sk-ab471e680f0dcd9ba214df9b863a8c0d",
    "sk-6e13e6e137327928e0567446f5d21dc1",
    "sk-56dddb414a66f0e496ce43b9d0207fbb",
    "sk-ccb4c6d95c224d1e9b6d3cfffdcce007",
    "sk-1347ed24924ca62bc4ec7a5e6019e1a1",
    "sk-cd9b7773dde6209cf0a81b5262b84a09",
    "sk-377ecef62e34146a518bdf2448c6f70d",
    "sk-c915dcf703e10fea4f8bc1db17bcae1d",
    "sk-b7ff4d3550f45350d398fe5ceafa41ad",
    "sk-a565410116f171defb514d4edf83ecf6",
    "sk-3cacf980e6e5a21ba55c642c68dfe56f",
    "sk-e47d5959c55fdf1b23e4ecbe5d4ff58b",
    "sk-22713957b71954dab8831e30b7d9f2a7",
    "sk-9539b087039ee86f6fefa3616e28281b",
    "sk-54c65232b8fe49852e78f4eadef6b018",
    "sk-6ab35ac9cd34be6752a420017d2e19e2",
    "sk-eb76b8e76a7b5884e2dcccd6dcf6e356",
    "sk-e2f87d96a274d9146bcd725bd5937f0a",
    "sk-ba46ef4569dcd32d40265c6368ca44e6",
    "sk-22210dae3195bbfeddbe49a38e9354ea",
    "sk-cc9f4e5caf812d66caca79e36e78ad62",
    "sk-19abf19453af590882b17ddc8df4e4e6",
    "sk-9cfb100896c4264c5a12ee6bcd6a603b",
    "sk-7a7ad40c62cbc6fe83aaed1db03c701c",
    "sk-0612edaa2bbbc507b8743bcaaf6d797b",
    "sk-0e41aae32b019477c01d7cc733b60caf",
    "sk-1b583d001d6052e5e352effd90d1e7d1",
    "sk-7ee10dbb991946add57c4ce0e7ed1007",
]


def set_xianyuw_keys(keys):
    global _XIANYUW_KEYS
    if keys and isinstance(keys, list):
        _XIANYUW_KEYS = keys


def _fetch_from_xianyuw(song_mid, timeout=10, lossless_only=False):
    """通过 apii.xianyuw.cn 获取下载链接。只用一个 key 请求一次，没有就放弃。"""
    if _XIANYUW_BREAKER.is_open():
        return None

    key = random.choice(_XIANYUW_KEYS)
    try:
        if not _XIANYUW_RATE_LIMITER.acquire(timeout=5):
            return None
        with _XIANYUW_SEMAPHORE:
            resp = _xianyuw_pool.request(
                "GET", f"https://apii.xianyuw.cn/api/v1/qq-music-search?id={song_mid}&key={key}&no_url=0&br=hires",
                headers={"User-Agent": random_user_agent()},
                timeout=urllib3.Timeout(connect=3, read=5),
            )
        if resp.status != 200:
            _XIANYUW_BREAKER.record_failure()
            return None
        data = json.loads(resp.data.decode("utf-8"))
        url = (data.get("data") or {}).get("url", "")
        if url and url.startswith("http"):
            ext = url.split("?")[0].rsplit(".", 1)[-1].lower()
            if ext not in ("mp3", "flac", "m4a", "ogg", "ape", "wav"):
                ext = "mp3"
            if lossless_only and ext not in _LOSSLESS_EXTS:
                return None
            _XIANYUW_BREAKER.record_success()
            return {"url": url, "file_name": f"{song_mid}.{ext}", "extension": f".{ext}",
                    "quality": "xianyuw-hires", "source": "xianyuw"}
        else:
            _XIANYUW_BREAKER.record_failure()
            return None
    except Exception:
        _XIANYUW_BREAKER.record_failure()
        return None


def fetch_download_info_batch(song_mid, media_mid=None, cookie="", uin="0", timeout=15):
    """一次请求所有音质，返回最高可用音质的下载信息。5 次请求合并为 1 次。"""
    cookie = normalize_cookie(cookie)
    uin = normalize_uin(uin)
    song_mid = str(song_mid or "").strip()
    if not song_mid:
        raise QQMusicDownloadError("Missing song_mid")

    media_mid = str(media_mid or song_mid).strip()
    resolved_uin = resolve_uin(cookie, uin)
    guid = str(random.randint(1_000_000_000, 9_999_999_999))

    # 按优先级排列：flac > ape > 320 > 128 > m4a
    qualities = ["flac", "ape", "320", "128", "m4a"]
    filenames = []
    songmids = []
    songtypes = []
    for q in qualities:
        fi = FILE_TYPES[q]
        filenames.append(f'{fi["prefix"]}{song_mid}{media_mid}{fi["extension"]}')
        songmids.append(song_mid)
        songtypes.append(0)

    request_data = {
        "req_0": {
            "module": "vkey.GetVkeyServer",
            "method": "CgiGetVkey",
            "param": {
                "filename": filenames,
                "guid": guid,
                "songmid": songmids,
                "songtype": songtypes,
                "uin": resolved_uin,
                "loginflag": 1,
                "platform": "20",
            },
        },
        "loginUin": resolved_uin,
        "comm": {
            "uin": resolved_uin, "format": "json", "ct": 24, "cv": 0,
        },
    }
    params = {
        "g_tk": 1124214810, "loginUin": resolved_uin, "hostUin": 0,
        "format": "json", "sign": PLAY_SIGN,
        "data": json.dumps(request_data, ensure_ascii=False, separators=(",", ":")),
    }
    request_url = f"{DOWNLOAD_URL}?{urllib.parse.urlencode(params)}"

    try:
        resp = _api_pool.request("GET", request_url, headers=build_headers(cookie=cookie), timeout=timeout)
        payload = json.loads(resp.data.decode("utf-8"))
    except Exception as exc:
        raise QQMusicDownloadError(f"Failed to fetch play URL: {exc}") from exc

    data = payload.get("req_0", {}).get("data", {})
    sip_list = data.get("sip") or []
    domain = next((item for item in sip_list if not item.startswith("http://ws")), sip_list[0] if sip_list else "")
    midurlinfo = data.get("midurlinfo") or []

    # 按优先级遍历，返回第一个有 URL 的
    for i, q in enumerate(qualities):
        if i >= len(midurlinfo):
            break
        play_info = midurlinfo[i]
        play_path = resolve_play_path(play_info)
        if play_path and domain:
            resolved_url = play_path if play_path.startswith("http") else f"{domain}{play_path}"
            fi = FILE_TYPES[q]
            return {
                "url": resolved_url,
                "file_name": f"{song_mid}{fi['extension']}",
                "extension": fi["extension"],
                "quality": q,
                "actual_quality": q,
            }

    raise QQMusicDownloadError(f"No playable URL for any quality: {song_mid}")


def fetch_download_info_with_fallback(song_mid, media_mid=None, quality="flac", cookie="", uin="0", timeout=20, prefer_thirdparty=False):
    """
    优先无损策略：
    1. 第三方 API 只取无损（1 轮 3 次请求）
    2. 官方 API 一次请求全部音质，自动选最高（1 次请求）
    3. 都没有再兜底第三方取任意音质（1 轮 3 次请求，仅在官方也失败时触发）
    常规路径最多 4 次请求，不影响效率。
    """
    # ── 1. 第三方优先取无损 ────────────────────────────────────────
    if prefer_thirdparty:
        result = _fetch_from_thirdparty(song_mid, media_mid, timeout=timeout, lossless_only=True, cookie=cookie)
        if result:
            return result

    # ── 2. 官方一次请求全部音质（flac>ape>320>128>m4a）────────────
    try:
        return fetch_download_info_batch(song_mid, media_mid=media_mid,
                                          cookie=cookie, uin=uin, timeout=timeout)
    except QQMusicDownloadError:
        pass

    # ── 3. 官方全挂，兜底第三方取任意音质 ─────────────────────────
    result = _fetch_from_thirdparty(song_mid, media_mid, timeout=timeout, lossless_only=False, cookie=cookie)
    if result:
        return result

    raise QQMusicDownloadError(f"All sources and qualities failed for {song_mid}")


def fetch_download_info(song_mid, media_mid=None, quality="128", cookie="", uin="0", timeout=20):
    if quality not in FILE_TYPES:
        raise QQMusicDownloadError(f"Unsupported quality: {quality}")

    cookie = normalize_cookie(cookie)
    uin = normalize_uin(uin)
    song_mid = str(song_mid or "").strip()
    if not song_mid:
        raise QQMusicDownloadError("Missing song_mid")

    media_mid = str(media_mid or song_mid).strip()
    file_info = FILE_TYPES[quality]
    filename = f'{file_info["prefix"]}{song_mid}{media_mid}{file_info["extension"]}'
    resolved_uin = resolve_uin(cookie, uin)
    guid = str(random.randint(1_000_000_000, 9_999_999_999))

    request_data = {
        "req_0": {
            "module": "vkey.GetVkeyServer",
            "method": "CgiGetVkey",
            "param": {
                "filename": [filename],
                "guid": guid,
                "songmid": [song_mid],
                "songtype": [0],
                "uin": resolved_uin,
                "loginflag": 1,
                "platform": "20",
            },
        },
        "loginUin": resolved_uin,
        "comm": {
            "uin": resolved_uin,
            "format": "json",
            "ct": 24,
            "cv": 0,
        },
    }
    params = {
        "g_tk": 1124214810, "loginUin": resolved_uin, "hostUin": 0,
        "format": "json", "sign": PLAY_SIGN,
        "data": json.dumps(request_data, ensure_ascii=False, separators=(",", ":")),
    }
    request_url = f"{DOWNLOAD_URL}?{urllib.parse.urlencode(params)}"
    
    try:
        resp = _api_pool.request("GET", request_url, headers=build_headers(cookie=cookie), timeout=timeout)
        payload = json.loads(resp.data.decode("utf-8"))
    except Exception as exc:
        raise QQMusicDownloadError(f"Failed to fetch play URL: {exc}") from exc

    data = payload.get("req_0", {}).get("data", {})
    sip_list = data.get("sip") or []
    # 优先选择非 http://ws 开头的域名作为主域名（同步自 D 盘稳定版本逻辑）
    domain = next((item for item in sip_list if not item.startswith("http://ws")), sip_list[0] if sip_list else "")
    
    midurlinfo = data.get("midurlinfo") or []
    if not midurlinfo:
        raise QQMusicDownloadError("QQ Music did not return midurlinfo")

    play_info = midurlinfo[0]
    play_path = resolve_play_path(play_info)
    if not play_path or not domain:
        message = play_info.get("msg") or "No playable URL returned"
        raise QQMusicDownloadError(message)

    resolved_url = play_path if (play_path.startswith("http://") or play_path.startswith("https://")) else f"{domain}{play_path}"

    return {
        "url": resolved_url,
        "file_name": f"{song_mid}{file_info['extension']}",
        "extension": file_info["extension"],
        "quality": quality,
    }


def save_song_file(download_url, destination, cookie="", timeout=60):
    destination = Path(destination)
    # 强制确保父目录存在且路径无末尾空格（双重保障）
    parent = Path(str(destination.parent).rstrip())
    parent.mkdir(parents=True, exist_ok=True)
    tmp_path = parent / (destination.name + ".tmp")
    cookie = normalize_cookie(cookie)
    parsed = urllib.parse.urlparse(download_url)
    headers = build_headers(cookie=cookie, host=parsed.netloc)
    # Connection: close 确保下载完毕后连接不被放回池中，防止残留 MP3 数据污染后续 API 请求
    headers["Connection"] = "close"

    try:
        resp = _download_pool.request(
            "GET", download_url, headers=headers, timeout=timeout, preload_content=False,
        )
        if resp.status not in (200, 206):
            resp.drain_conn()
            raise QQMusicDownloadError(f"HTTP {resp.status} from CDN")
        try:
            with tmp_path.open("wb") as file_obj:
                for chunk in resp.stream(1024 * 64):
                    file_obj.write(chunk)
        finally:
            # close() 关闭底层 socket，不归还给连接池
            resp.close()
        tmp_path.replace(parent / destination.name)
    except QQMusicDownloadError:
        try:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    except Exception as exc:
        try:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise QQMusicDownloadError(f"Failed to download song file: {exc}") from exc
