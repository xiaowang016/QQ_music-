# -*- coding: utf-8 -*-
"""
QQ 音乐 musicu.fcg 请求签名算法。
借鉴自 QQMusicApi/algorithms/sign.py，用于 Android 端请求签名。
"""

import re
from base64 import b64encode
from hashlib import sha1
import json


PART_1_INDEXES = tuple(i for i in (23, 14, 6, 36, 16, 40, 7, 19) if i < 40)
PART_2_INDEXES = (16, 1, 32, 12, 19, 27, 8, 5)
SCRAMBLE_VALUES = (
    89, 39, 179, 150, 218, 82, 58, 252,
    177, 52, 186, 123, 120, 64, 242, 133,
    143, 161, 121, 179,
)


def sign_request(request_data):
    """
    生成 QQ 音乐 API 请求签名。

    Args:
        request_data: 请求数据字典（将被 JSON 序列化后计算 SHA1）。

    Returns:
        str: 签名字符串，形如 "zzc..." 。
    """
    raw = json.dumps(request_data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    digest = sha1(raw).hexdigest().upper()

    part1 = "".join(digest[i] for i in PART_1_INDEXES)
    part2 = "".join(digest[i] for i in PART_2_INDEXES)

    part3 = bytearray(20)
    for i, value in enumerate(SCRAMBLE_VALUES):
        part3[i] = value ^ int(digest[i * 2: i * 2 + 2], 16)

    b64_part = re.sub(rb"[\\/+=]", b"", b64encode(part3)).decode("utf-8")
    return f"zzc{part1}{b64_part}{part2}".lower()
