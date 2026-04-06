# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/tools/time_util.py
# GitHub: https://github.com/NanmiCoder
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#

# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。


# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2023/12/2 12:52
# @Desc    : Time utility functions

import re
import time
from datetime import datetime, timedelta, timezone


def get_current_timestamp() -> int:
    """
    Get current timestamp (13 digits): 1701493264496
    :return:
    """
    return int(time.time() * 1000)


def get_current_time() -> str:
    """
    Get current time: '2023-12-02 13:01:23'
    :return:
    """
    return time.strftime('%Y-%m-%d %X', time.localtime())

def get_current_time_hour() -> str:
    """
    Get current time with hour: '2023-12-02-13'
    :return:
    """
    return time.strftime('%Y-%m-%d-%H', time.localtime())

def get_current_date() -> str:
    """
    Get current date: '2023-12-02'
    :return:
    """
    return time.strftime('%Y-%m-%d', time.localtime())


def get_time_str_from_unix_time(unixtime):
    """
    Unix integer timestamp ==> datetime string
    :param unixtime:
    :return:
    """
    if int(unixtime) > 1000000000000:
        unixtime = int(unixtime) / 1000
    return time.strftime('%Y-%m-%d %X', time.localtime(unixtime))


def get_date_str_from_unix_time(unixtime):
    """
    Unix integer timestamp ==> date string
    :param unixtime:
    :return:
    """
    if int(unixtime) > 1000000000000:
        unixtime = int(unixtime) / 1000
    return time.strftime('%Y-%m-%d', time.localtime(unixtime))


def get_unix_time_from_time_str(time_str):
    """
    Time string ==> Unix integer timestamp, precise to seconds
    :param time_str:
    :return:
    """
    time_str = str(time_str).strip()
    if not time_str:
        return 0

    supported_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    )
    for format_str in supported_formats:
        try:
            tm_object = time.strptime(time_str, format_str)
            return int(time.mktime(tm_object))
        except ValueError:
            continue
    return 0


def get_unix_timestamp():
    return int(time.time())


def rfc2822_to_china_datetime(rfc2822_time):
    # Define RFC 2822 format
    rfc2822_format = "%a %b %d %H:%M:%S %z %Y"

    # Convert RFC 2822 time string to datetime object
    dt_object = datetime.strptime(rfc2822_time, rfc2822_format)

    # Convert datetime object timezone to China timezone
    dt_object_china = dt_object.astimezone(timezone(timedelta(hours=8)))
    return dt_object_china


def rfc2822_to_timestamp(rfc2822_time):
    # Define RFC 2822 format
    rfc2822_format = "%a %b %d %H:%M:%S %z %Y"

    # Convert RFC 2822 time string to datetime object
    dt_object = datetime.strptime(rfc2822_time, rfc2822_format)

    return int(dt_object.timestamp())


def parse_weibo_created_at_to_timestamp(created_at, now=None):
    """
    Parse Weibo created_at strings from mobile APIs into a Unix timestamp.

    Supported examples:
    - "Sat Mar 21 23:15:00 +0800 2026"
    - "刚刚" / "5分钟前" / "2小时前"
    - "今天 13:20" / "昨天 08:15"
    - "03-21" / "03-21 13:20"
    - "2026-03-21" / "2026-03-21 13:20:00"
    """
    if not created_at:
        return 0

    created_at = str(created_at).strip()
    if not created_at:
        return 0

    local_now = now.astimezone() if now else datetime.now().astimezone()
    local_tz = local_now.tzinfo

    try:
        return rfc2822_to_timestamp(created_at)
    except ValueError:
        pass

    if created_at == "刚刚":
        return int(local_now.timestamp())

    relative_patterns = (
        (r"^(\d+)\s*秒前$", 1),
        (r"^(\d+)\s*分钟前$", 60),
        (r"^(\d+)\s*小时前$", 3600),
    )
    for pattern, multiplier in relative_patterns:
        match = re.match(pattern, created_at)
        if match:
            delta_seconds = int(match.group(1)) * multiplier
            return int((local_now - timedelta(seconds=delta_seconds)).timestamp())

    match = re.match(r"^今天\s+(\d{1,2}):(\d{2})$", created_at)
    if match:
        dt = local_now.replace(
            hour=int(match.group(1)),
            minute=int(match.group(2)),
            second=0,
            microsecond=0,
        )
        return int(dt.timestamp())

    match = re.match(r"^昨天\s+(\d{1,2}):(\d{2})$", created_at)
    if match:
        dt = (local_now - timedelta(days=1)).replace(
            hour=int(match.group(1)),
            minute=int(match.group(2)),
            second=0,
            microsecond=0,
        )
        return int(dt.timestamp())

    absolute_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    )
    for format_str in absolute_formats:
        try:
            dt = datetime.strptime(created_at, format_str)
            if format_str == "%Y-%m-%d":
                dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            return int(dt.replace(tzinfo=local_tz).timestamp())
        except ValueError:
            continue

    month_day_patterns = (
        r"^(\d{1,2})-(\d{1,2})$",
        r"^(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2})$",
        r"^(\d{1,2})月(\d{1,2})日$",
        r"^(\d{1,2})月(\d{1,2})日\s+(\d{1,2}):(\d{2})$",
    )
    for pattern in month_day_patterns:
        match = re.match(pattern, created_at)
        if not match:
            continue

        month = int(match.group(1))
        day = int(match.group(2))
        hour = int(match.group(3)) if match.lastindex and match.lastindex >= 3 else 0
        minute = int(match.group(4)) if match.lastindex and match.lastindex >= 4 else 0
        year = local_now.year
        dt = datetime(year, month, day, hour, minute, 0, tzinfo=local_tz)
        if dt > local_now + timedelta(days=1):
            dt = dt.replace(year=year - 1)
        return int(dt.timestamp())

    return 0


if __name__ == '__main__':
    # Example usage
    _rfc2822_time = "Sat Dec 23 17:12:54 +0800 2023"
    print(rfc2822_to_china_datetime(_rfc2822_time))
