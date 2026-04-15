# -*- coding: utf-8 -*-
"""
公共工具函数和常量，供项目各模块复用。
"""

import random
import re

USER_AGENT_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]


def random_user_agent():
    return random.choice(USER_AGENT_LIST)


INVALID_PATH_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
RESERVED_WINDOWS_NAMES = {
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
}


def sanitize_path_part(value, fallback, max_len=120):
    """将任意字符串清理为安全的文件/目录名片段，处理 Windows 保留字。"""
    text = str(value or "").strip()
    text = INVALID_PATH_CHARS_RE.sub("_", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    if not text:
        text = fallback
    if text.upper() in RESERVED_WINDOWS_NAMES:
        text = f"_{text}"
    return text[:max_len].strip(" .")


def build_song_folder_name(song_name, singer_names, song_id):
    """根据歌名、歌手、song_id 构建歌曲目录名。"""
    safe_song_name = sanitize_path_part(song_name, f"song_{song_id}")
    first_singer = sanitize_path_part((singer_names or ["unknown"])[0], "unknown")
    return sanitize_path_part(
        f"{safe_song_name} - {first_singer} [{song_id}]",
        str(song_id),
    )


def parse_song_info(song_info):
    """从 QQ 音乐 API 返回的 song_info 字典中提取标准化的歌曲元数据。"""
    if not song_info:
        return None
    song_file = song_info.get("file") or {}
    singers = song_info.get("singer") or []
    album = song_info.get("album") or {}
    mv = song_info.get("mv") or {}

    return {
        "song_id": song_info.get("id"),
        "song_mid": song_info.get("mid"),
        "media_mid": song_file.get("media_mid") or song_info.get("mid"),
        "song_name": song_info.get("title") or song_info.get("name"),
        "subtitle": song_info.get("subtitle", ""),
        "song_time_public": song_info.get("time_public", ""),
        "song_type": song_info.get("type"),
        "language": song_info.get("language"),
        "genre": song_info.get("genre"),
        "song_url": f'https://y.qq.com/n/yqq/song/{song_info.get("mid")}.html',
        "singer_names": [s.get("name") for s in singers if s.get("name")],
        "singer_ids": [s.get("id") for s in singers if s.get("id") is not None],
        "singer_mids": [s.get("mid") for s in singers if s.get("mid")],
        "album_name": album.get("name"),
        "album_id": album.get("id"),
        "album_mid": album.get("mid"),
        "index_album": song_info.get("index_album"),
        "index_cd": song_info.get("index_cd"),
        "mv_id": mv.get("id"),
        "mv_mid": mv.get("mid"),
        "mv_vid": mv.get("vid"),
        "pay": song_info.get("pay") or {},
        "file_info": {
            "size_128mp3": song_file.get("size_128mp3", 0),
            "size_320mp3": song_file.get("size_320mp3", 0),
            "size_flac": song_file.get("size_flac", 0),
            "size_ape": song_file.get("size_ape", 0),
            "size_ogg": song_file.get("size_ogg", 0),
        },
    }


def process_lyric(lyric):
    """解码 QQ 音乐歌词中的 HTML 实体并清理时间标签。"""
    re_lyric = re.findall(r"\[[0-9]+&#[0-9]+;[0-9]+&#[0-9]+;[0-9]+\].*", lyric)
    if re_lyric:
        lyric = re_lyric[0]
        for old, new in [("&#32;", " "), ("&#40;", "("), ("&#41;", ")"),
                         ("&#45;", "-"), ("&#10;", ""), ("&#38;apos&#59;", "'")]:
            lyric = lyric.replace(old, new)
        return "\n".join(s for s in re.split(r"\[[0-9]+&#[0-9]+;[0-9]+&#[0-9]+;[0-9]+\]", lyric) if s.strip())
    for old, new in [("&#32;", " "), ("&#40;", "("), ("&#41;", ")"),
                     ("&#45;", "-"), ("&#10;", "\n"), ("&#38;apos&#59;", "'")]:
        lyric = lyric.replace(old, new)
    return lyric
