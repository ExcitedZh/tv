#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TVBox Live Stream Multi-Source Aggregator & Formatter
Aggregates IPv6 & IPv4 high-quality streams, normalizes channel names,
and merges multiple stream lines using `#` for automatic TVBox failover.
"""

import urllib.request
import re
import os
from collections import OrderedDict

# 核心订阅源列表 (涵盖主流高星稳定维护项目)
SOURCES = [
    {"url": "https://live.fanmingming.com/tv/m3u/ipv6.m3u", "type": "m3u"},
    {"url": "https://raw.githubusercontent.com/Guovin/iptv-api/master/output/result.txt", "type": "txt"},
    {"url": "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u", "type": "m3u"},
    {"url": "https://raw.githubusercontent.com/kimwang1978/collect-tv-txt/main/tvbox.txt", "type": "txt"}
]

# 频道分类与标准名称定义
CATEGORY_RULES = OrderedDict([
    ("央视频道", [
        ("CCTV-1", [r"CCTV-?1(\b|综合|HD|4K|\+)", r"中央一台"]),
        ("CCTV-2", [r"CCTV-?2(\b|财经|HD|\+)", r"中央二台"]),
        ("CCTV-3", [r"CCTV-?3(\b|综艺|HD|\+)", r"中央三台"]),
        ("CCTV-4", [r"CCTV-?4(\b|中文国际|亚洲|欧洲|美洲|HD|\+)", r"中央四台"]),
        ("CCTV-5", [r"CCTV-?5(\b|体育|HD|\+)(?!5\+)", r"中央五台(?!5\+)"]),
        ("CCTV-5+", [r"CCTV-?5\+(\b|体育赛事|HD)", r"中央五\+"]),
        ("CCTV-6", [r"CCTV-?6(\b|电影|HD|\+)", r"中央六台"]),
        ("CCTV-7", [r"CCTV-?7(\b|国防军事|军事农业|军农|HD|\+)", r"中央七台"]),
        ("CCTV-8", [r"CCTV-?8(\b|电视剧|HD|\+)", r"中央八台"]),
        ("CCTV-9", [r"CCTV-?9(\b|纪录|HD|\+)", r"中央九台"]),
        ("CCTV-10", [r"CCTV-?10(\b|科教|HD|\+)", r"中央十台"]),
        ("CCTV-11", [r"CCTV-?11(\b|戏曲|HD|\+)", r"中央十一台"]),
        ("CCTV-12", [r"CCTV-?12(\b|社会与法|HD|\+)", r"中央十二台"]),
        ("CCTV-13", [r"CCTV-?13(\b|新闻|HD|\+)", r"中央十三台"]),
        ("CCTV-14", [r"CCTV-?14(\b|少儿|HD|\+)", r"中央十四台"]),
        ("CCTV-15", [r"CCTV-?15(\b|音乐|HD|\+)", r"中央十五台"]),
        ("CCTV-16", [r"CCTV-?16(\b|奥林匹克|HD|4K|\+)", r"中央十六台"]),
        ("CCTV-17", [r"CCTV-?17(\b|农业农村|农村农业|HD|\+)", r"中央十七台"]),
        ("CGTN", [r"CGTN(\b|英语|新闻)"]),
        ("CGTN纪录", [r"CGTN(\s|-)?(Documentary|纪录)"]),
    ]),
    ("卫视频道", [
        ("湖南卫视", [r"湖南卫视"]),
        ("浙江卫视", [r"浙江卫视"]),
        ("江苏卫视", [r"江苏卫视"]),
        ("东方卫视", [r"东方卫视"]),
        ("北京卫视", [r"北京卫视"]),
        ("广东卫视", [r"广东卫视"]),
        ("深圳卫视", [r"深圳卫视"]),
        ("湖北卫视", [r"湖北卫视"]),
        ("安徽卫视", [r"安徽卫视"]),
        ("山东卫视", [r"山东卫视"]),
        ("四川卫视", [r"四川卫视"]),
        ("天津卫视", [r"天津卫视"]),
        ("重庆卫视", [r"重庆卫视"]),
        ("江西卫视", [r"江西卫视"]),
        ("河南卫视", [r"河南卫视"]),
        ("河北卫视", [r"河北卫视"]),
        ("辽宁卫视", [r"辽宁卫视"]),
        ("吉林卫视", [r"吉林卫视"]),
        ("黑龙江卫视", [r"黑龙江卫视"]),
        ("福建东南卫视", [r"(福建|东南)卫视"]),
        ("贵州卫视", [r"贵州卫视"]),
        ("云南卫视", [r"云南卫视"]),
        ("广西卫视", [r"广西卫视"]),
        ("海南卫视", [r"海南卫视|旅游卫视"]),
        ("陕西卫视", [r"陕西卫视"]),
        ("山西卫视", [r"山西卫视"]),
        ("甘肃卫视", [r"甘肃卫视"]),
        ("青海卫视", [r"青海卫视"]),
        ("宁夏卫视", [r"宁夏卫视"]),
        ("新疆卫视", [r"新疆卫视"]),
        ("西藏卫视", [r"西藏卫视"]),
        ("内蒙古卫视", [r"内蒙古卫视"]),
    ]),
    ("数字/影视频道", [
        ("CHC高清电影", [r"CHC.*高清电影"]),
        ("CHC动作电影", [r"CHC.*动作电影"]),
        ("CHC家庭影院", [r"CHC.*家庭影院"]),
        ("淘电影", [r"淘电影"]),
        ("淘剧场", [r"淘剧场"]),
        ("重温经典", [r"重温经典"]),
        ("欢笑剧场", [r"欢笑剧场"]),
        ("都市剧场", [r"都市剧场"]),
        ("求索纪录", [r"求索纪录"]),
        ("求索科学", [r"求索科学"]),
        ("求索动物", [r"求索动物"]),
    ]),
    ("少儿动画", [
        ("卡酷少儿", [r"卡酷少儿|北京卡酷"]),
        ("金鹰卡通", [r"金鹰卡通|湖南金鹰"]),
        ("嘉佳卡通", [r"嘉佳卡通|广东嘉佳"]),
        ("优漫卡通", [r"优漫卡通|江苏优漫"]),
        ("炫动卡通", [r"炫动卡通|上海炫动"]),
        ("哈哈炫动", [r"哈哈炫动"]),
    ])
])

def fetch_content(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "okhttp/3.15.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read().decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"Warning: Failed to fetch {url}: {e}")
        return ""

def parse_m3u(content):
    channels = []
    lines = content.split('\n')
    current_name = None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith('#EXTINF:'):
            # Extract channel name from comma
            parts = line.split(',')
            if len(parts) > 1:
                current_name = parts[-1].strip()
        elif not line.startswith('#') and current_name:
            channels.append((current_name, line))
            current_name = None
    return channels

def parse_txt(content):
    channels = []
    lines = content.split('\n')
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#') or '#genre#' in line:
            continue
        if ',' in line:
            parts = line.split(',', 1)
            name = parts[0].strip()
            urls = parts[1].strip()
            for u in urls.split('#'):
                u = u.strip()
                if u.startswith('http') or u.startswith('rtp'):
                    channels.append((name, u))
    return channels

def match_channel(raw_name):
    # Strip common suffixes like HD, 4K, 1080P, etc.
    clean_name = re.sub(r'\[.*?\]|\(.*?\)|超清|高清|1080P|720P|4K|FHD|HD|\s+', '', raw_name, flags=re.IGNORECASE)
    for cat_name, ch_list in CATEGORY_RULES.items():
        for std_name, patterns in ch_list:
            for pat in patterns:
                if re.search(pat, raw_name, re.IGNORECASE) or re.search(pat, clean_name, re.IGNORECASE):
                    return cat_name, std_name
    return None, None

def main():
    print("Fetching and processing IPTV sources...")
    all_raw_channels = []
    for src in SOURCES:
        c = fetch_content(src["url"])
        if not c:
            continue
        if src["type"] == "m3u":
            parsed = parse_m3u(c)
        else:
            parsed = parse_txt(c)
        print(f"Loaded {len(parsed)} streams from {src['url']}")
        all_raw_channels.extend(parsed)

    # Group by category and standard channel name
    aggregated = OrderedDict()
    for cat_name, ch_list in CATEGORY_RULES.items():
        aggregated[cat_name] = OrderedDict()
        for std_name, _ in ch_list:
            aggregated[cat_name][std_name] = []

    for raw_name, url in all_raw_channels:
        cat_name, std_name = match_channel(raw_name)
        if cat_name and std_name:
            if url not in aggregated[cat_name][std_name]:
                aggregated[cat_name][std_name].append(url)

    # Generate live.txt (TVBox format with `#` multi-link support)
    txt_lines = []
    m3u_lines = ["#EXTM3U"]

    for cat_name, ch_map in aggregated.items():
        txt_lines.append(f"{cat_name},#genre#")
        for std_name, urls in ch_map.items():
            if urls:
                # Keep top 8 distinct streams per channel to avoid bloating
                selected_urls = urls[:8]
                merged_urls = "#".join(selected_urls)
                txt_lines.append(f"{std_name},{merged_urls}")
                
                # Also generate M3U format
                for u in selected_urls:
                    m3u_lines.append(f'#EXTINF:-1 tvg-name="{std_name}" group-title="{cat_name}",{std_name}')
                    m3u_lines.append(u)

    # Write files
    with open("live.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines) + "\n")
    print(f"Generated live.txt ({len(txt_lines)} lines)")

    with open("live.m3u", "w", encoding="utf-8") as f:
        f.write("\n".join(m3u_lines) + "\n")
    print(f"Generated live.m3u ({len(m3u_lines)} lines)")

if __name__ == "__main__":
    main()
