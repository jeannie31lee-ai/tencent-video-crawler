#!/usr/bin/env python3
"""
腾讯视频全量爬虫 V8 - 修正iyear映射 + 全维度交叉爬取
关键修正：
  1. 使用API返回的正确iyear筛选值（非旧版1-17）
  2. 电视剧: 11个年份段 × 3种排序 + 撞上限的年份按地区拆分
  3. 电影: 20个年份段 × 3种排序
  4. 合并去重 → 过滤短剧 → 确定付费类型
"""

import requests, json, time, sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "https://pbaccess.video.qq.com/trpc.multi_vector_layout.mvl_controller.MVLPageHTTPService/getMVLPage?&vversion_platform=2"
VID_INFO_URL = "https://h5vv6.video.qq.com/getinfo"
HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://v.qq.com",
    "Referer": "https://v.qq.com/channel/tv/list",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
}

# ===== 正确的筛选维度 =====
TV_YEARS = [
    (1, "即将上线"), (2026, "2026"), (2025, "2025"),
    (2, "2024"), (3, "2023"), (4, "2022"), (5, "2021"),
    (6, "2020-2016"), (7, "2015-2011"), (8, "2010-2000"), (9, "更早"),
]
TV_SORTS = [(75, "最热"), (79, "最新上架"), (85, "高分好评")]
TV_AREAS = [(0, "内地"), (14, "中国香港"), (4, "中国台湾"), (8, "美国"), (5, "韩国"), (10, "日本"), (9, "泰国"), (1, "英国"), (9999, "其他")]

MV_YEARS = [
    (999, "即将上线"), (2026, "2026"), (2025, "2025"),
    (2024, "2024"), (2023, "2023"), (2022, "2022"), (2021, "2021"),
    (2020, "2020"), (20, "2019"), (2018, "2018"),
    (1, "2017"), (2, "2016"), (3, "2015"), (4, "2014"),
    (5, "2013-2011"), (6, "2010-2006"), (7, "2005-2000"),
    (8, "90年代"), (9, "80年代"), (10, "其他"),
]
MV_SORTS = [(75, "最热"), (83, "最新"), (81, "高分好评")]
MV_AREAS = [(100024, "内地"), (100025, "中国香港"), (100026, "中国台湾"), (100029, "美国"), (100027, "日本"), (100028, "韩国"), (100031, "泰国"), (100030, "印度"), (15, "英国"), (16, "法国"), (17, "德国"), (18, "加拿大"), (19, "西班牙"), (20, "意大利"), (21, "澳大利亚"), (100033, "其他")]

FEMALE_GENRES = {"爱情", "家庭", "青春", "古装", "宫斗", "甜宠", "都市"}
MALE_GENRES = {"军旅", "刑侦", "竞技", "武侠", "科幻", "战争", "谍战", "悬疑", "权谋", "猎奇"}

def fetch_page(channel_id, filter_params, page_context=None):
    body = {"page_params": {"page_type": "operation", "page_id": "channel_list", "channel_id": channel_id, "filter_params": filter_params}}
    if page_context:
        body["page_context"] = page_context
    for attempt in range(3):
        try:
            resp = requests.post(API_URL, headers=HEADERS, json=body, timeout=15)
            data = resp.json()
            if "data" not in data:
                time.sleep(2); continue
            return data["data"]
        except:
            time.sleep(3)
    return None

def parse_items(api_data):
    items = []
    try:
        cards = api_data["modules"]["normal"]["cards"][0]["children_list"]["poster_card"]["cards"]
        for card in cards:
            p = card.get("params", {})
            # 独播检测
            exclusive = "非独播"
            lml = p.get("latest_mark_label", "")
            try:
                lml_data = json.loads(lml) if isinstance(lml, str) and lml else {}
                tag2_info = lml_data.get("2", {}).get("info", {})
                if tag2_info.get("id") == "15" and tag2_info.get("text") == "独播":
                    exclusive = "独播"
            except:
                if "独播" in str(lml):
                    exclusive = "独播"

            # 集数
            episode_info = p.get("timelong", "")
            if lml:
                try:
                    ml = json.loads(lml) if isinstance(lml, str) else lml
                    for pk in ["4", "3"]:
                        if pk in ml:
                            txt = ml[pk].get("info", {}).get("text", "")
                            if "集" in txt or "期" in txt:
                                episode_info = txt; break
                except: pass

            # 第一集VID
            first_vid = ""
            all_ids_raw = p.get("all_ids", "")
            if all_ids_raw:
                try:
                    all_ids = json.loads(all_ids_raw) if isinstance(all_ids_raw, str) else all_ids_raw
                    if all_ids:
                        first_vid = all_ids[0].get("V", "")
                except: pass
            if not first_vid:
                first_vid = p.get("first_vid_in_set", "") or p.get("first_vid", "")

            # 题材标签
            genre_tags = []
            sl_raw = p.get("chnlist_search_label", "")
            if sl_raw:
                try:
                    sl_list = json.loads(sl_raw) if isinstance(sl_raw, str) else sl_raw
                    for sl in sl_list:
                        if sl.get("category", 0) in [80, 90]:
                            genre_tags.append(sl.get("label", ""))
                except: pass

            items.append({
                "cid": card.get("id", p.get("cid", "")),
                "title": p.get("title", ""),
                "area_name": p.get("area_name", ""),
                "year": p.get("year", ""),
                "leading_actor": p.get("leading_actor", "").strip("[]"),
                "main_genre": p.get("main_genre", ""),
                "episode_info": episode_info,
                "exclusive": exclusive,
                "genre_tags": "、".join(genre_tags),
                "first_vid": first_vid,
                "pay_type": "",
            })
    except:
        pass
    return items

def crawl_all_pages(channel_id, filter_params, max_pages=1000):
    all_items = []
    page_context = None
    for page_num in range(1, max_pages + 1):
        api_data = fetch_page(channel_id, filter_params, page_context)
        if not api_data: break
        items = parse_items(api_data)
        if not items: break
        all_items.extend(items)
        has_next = api_data.get("has_next_page", False)
        page_context = api_data.get("page_context")
        if not has_next or not page_context: break
        time.sleep(0.05)
        if page_num % 50 == 0:
            print(f"      page {page_num}: {len(all_items)} items", flush=True)
    return all_items

def crawl_titles_set(channel_id, filter_params, max_pages=1000):
    titles = set()
    page_context = None
    for page_num in range(1, max_pages + 1):
        api_data = fetch_page(channel_id, filter_params, page_context)
        if not api_data: break
        try:
            cards = api_data["modules"]["normal"]["cards"][0]["children_list"]["poster_card"]["cards"]
            for c in cards:
                titles.add(c.get("params", {}).get("title", ""))
        except: break
        if not api_data.get("has_next_page"): break
        page_context = api_data.get("page_context")
        if not page_context: break
        time.sleep(0.05)
    return titles

def merge_items(all_items_list):
    merged = {}
    for it in all_items_list:
        cid = it["cid"]
        if cid not in merged:
            merged[cid] = it
        else:
            old = merged[cid]
            if it["exclusive"] == "独播" and old["exclusive"] != "独播":
                merged[cid]["exclusive"] = "独播"
            for key in ["episode_info", "first_vid", "genre_tags"]:
                if not old.get(key) and it.get(key):
                    merged[cid][key] = it[key]
    return list(merged.values())

def get_vid_duration(vid):
    try:
        resp = requests.get(VID_INFO_URL, params={"vid": vid, "platform": "10201", "otype": "json", "defn": "sd"},
                           headers={"Referer": "https://v.qq.com/", "User-Agent": "Mozilla/5.0"}, timeout=10)
        text = resp.text
        if text.startswith("QZOutputJson="): text = text[len("QZOutputJson="):]
        if text.endswith(";"): text = text[:-1]
        data = json.loads(text)
        vi_list = data.get("vl", {}).get("vi", [])
        if vi_list:
            return float(vi_list[0].get("td", "0"))
    except: pass
    return -1

def get_durations_batch(vid_list, max_workers=20):
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(get_vid_duration, vid): vid for vid in vid_list if vid}
        done = 0
        for future in as_completed(future_map):
            vid = future_map[future]
            try: results[vid] = future.result()
            except: results[vid] = -1
            done += 1
            if done % 500 == 0:
                print(f"      duration checked: {done}/{len(future_map)}", flush=True)
    return results

def determine_gender(main_genre, genre_tags_str):
    all_genres = set()
    if main_genre: all_genres.add(main_genre)
    if genre_tags_str:
        for g in genre_tags_str.split("、"):
            all_genres.add(g.strip())
    f = bool(all_genres & FEMALE_GENRES)
    m = bool(all_genres & MALE_GENRES)
    if f and not m: return "女频"
    if m and not f: return "男频"
    return "通用"


def main():
    # ============ 电视剧: 全维度爬取 ============
    print("=" * 60)
    print("电视剧: 年份×排序 全维度爬取")
    print("=" * 60)

    tv_all = []

    # 维度1: 每个年份段 × 每种排序
    for yval, ylabel in TV_YEARS:
        for sval, slabel in TV_SORTS:
            fp = f"sort={sval}&iyear={yval}"
            items = crawl_all_pages("100113", fp)
            tv_all.extend(items)
            print(f"  {ylabel} × {slabel}: {len(items)}", flush=True)
            time.sleep(0.1)

    # 维度2: 撞上限的年份段(2010-2000)按地区拆分
    print("  [2010-2000 按地区拆分补充]", flush=True)
    for aval, alabel in TV_AREAS:
        for sval, slabel in TV_SORTS:
            fp = f"sort={sval}&iyear=8&iarea={aval}"
            items = crawl_all_pages("100113", fp)
            if items:
                tv_all.extend(items)
                print(f"    {alabel} × {slabel}: {len(items)}", flush=True)
            time.sleep(0.1)

    # 维度3: 无年份的各sort
    for sval, slabel in TV_SORTS:
        items = crawl_all_pages("100113", f"sort={sval}")
        tv_all.extend(items)
        print(f"  全部 × {slabel}: {len(items)}", flush=True)

    tv_merged = merge_items(tv_all)
    print(f"\n  电视剧合并去重: {len(tv_all)} -> {len(tv_merged)} 部", flush=True)

    # ============ 电影: 全维度爬取 ============
    print(f"\n{'=' * 60}")
    print("电影: 年份×排序 全维度爬取")
    print("=" * 60)

    movie_all = []

    # 维度1: 每个年份段 × 每种排序
    for yval, ylabel in MV_YEARS:
        for sval, slabel in MV_SORTS:
            fp = f"sort={sval}&iyear={yval}"
            items = crawl_all_pages("100173", fp)
            movie_all.extend(items)
            print(f"  {ylabel} × {slabel}: {len(items)}", flush=True)
            time.sleep(0.1)

    # 维度2: 无年份的各sort
    for sval, slabel in MV_SORTS:
        items = crawl_all_pages("100173", f"sort={sval}")
        movie_all.extend(items)
        print(f"  全部 × {slabel}: {len(items)}", flush=True)

    movie_merged = merge_items(movie_all)
    print(f"\n  电影合并去重: {len(movie_all)} -> {len(movie_merged)} 部", flush=True)

    # ============ 确定付费类型 ============
    print(f"\n{'=' * 60}")
    print("确定付费类型")
    print("=" * 60)

    # 电视剧付费: ipay=1(免费), 2(限免), 3(会员)
    tv_pay_sort = TV_SORTS[0][0]  # 用最热排序
    tv_free = crawl_titles_set("100113", f"sort={tv_pay_sort}&ipay=1")
    tv_limited = crawl_titles_set("100113", f"sort={tv_pay_sort}&ipay=2")
    tv_vip = crawl_titles_set("100113", f"sort={tv_pay_sort}&ipay=3")
    print(f"  电视剧: 免费{len(tv_free)}, 限免{len(tv_limited)}, 会员{len(tv_vip)}", flush=True)

    for it in tv_merged:
        t = it["title"]
        if t in tv_free: it["pay_type"] = "免费"
        elif t in tv_limited: it["pay_type"] = "会员"  # 限免归为会员
        elif t in tv_vip: it["pay_type"] = "会员"
        else: it["pay_type"] = "付费"

    # 电影付费: ipay=1(免费), 8(会员), 4(付费), 3300(限免)
    mv_free = crawl_titles_set("100173", f"sort=75&ipay=1")
    mv_vip = crawl_titles_set("100173", f"sort=75&ipay=8")
    mv_paid = crawl_titles_set("100173", f"sort=75&ipay=4")
    mv_limited = crawl_titles_set("100173", f"sort=75&ipay=3300")
    print(f"  电影: 免费{len(mv_free)}, 会员{len(mv_vip)}, 付费{len(mv_paid)}, 限免{len(mv_limited)}", flush=True)

    for it in movie_merged:
        t = it["title"]
        if t in mv_vip: it["pay_type"] = "会员"
        elif t in mv_free: it["pay_type"] = "免费"
        elif t in mv_limited: it["pay_type"] = "会员"
        elif t in mv_paid: it["pay_type"] = "付费"
        else: it["pay_type"] = "付费"

    # ============ 过滤短剧 ============
    print(f"\n{'=' * 60}")
    print("过滤短剧 (单集≤20分钟)")
    print("=" * 60)

    tv_vids = [it["first_vid"] for it in tv_merged if it["first_vid"]]
    print(f"  电视剧: 检查 {len(tv_vids)} 部时长...", flush=True)
    tv_durations = get_durations_batch(tv_vids, max_workers=20)

    filtered_tv = []
    short_count = 0
    for it in tv_merged:
        vid = it["first_vid"]
        dur = tv_durations.get(vid, -1)
        dur_min = dur / 60 if dur > 0 else -1
        if dur_min > 0 and dur_min <= 20:
            short_count += 1
        else:
            filtered_tv.append(it)
    print(f"  电视剧: 排除 {short_count} 部短剧, 保留 {len(filtered_tv)} 部", flush=True)

    mv_vids = [it["first_vid"] for it in movie_merged if it["first_vid"]]
    print(f"  电影: 检查 {len(mv_vids)} 部时长...", flush=True)
    mv_durations = get_durations_batch(mv_vids, max_workers=20)

    filtered_movie = []
    short_mv = 0
    for it in movie_merged:
        vid = it["first_vid"]
        dur = mv_durations.get(vid, -1)
        dur_min = dur / 60 if dur > 0 else -1
        if dur_min > 0 and dur_min <= 20:
            short_mv += 1
        else:
            if dur > 0:
                it["episode_info"] = f"{int(dur / 60)}分钟"
            filtered_movie.append(it)
    print(f"  电影: 排除 {short_mv} 部短片, 保留 {len(filtered_movie)} 部", flush=True)

    # ============ 输出 ============
    output = {}
    for name, items in [("电视剧", filtered_tv), ("电影", filtered_movie)]:
        output[name] = []
        for it in items:
            output[name].append({
                "平台": "腾讯视频",
                "剧名": it["title"],
                "类型": name,
                "是否独播": it["exclusive"],
                "付费类型": it["pay_type"],
                "年份": it["year"],
                "演员": it["leading_actor"],
                "地区": it["area_name"],
                "集数": it["episode_info"],
                "题材": it["main_genre"],
                "男女频": determine_gender(it["main_genre"], it["genre_tags"]),
            })

    with open("/tmp/tencent_video_v8_raw.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    total = sum(len(v) for v in output.values())
    print(f"\n{'=' * 60}")
    print(f"完成! 总计 {total} 部")
    for name, items in output.items():
        excl = sum(1 for it in items if it["是否独播"] == "独播")
        pay = Counter(it["付费类型"] for it in items)
        print(f"  {name}: {len(items)} 部, 独播{excl}")
        print(f"    付费: {dict(pay)}")
    print(f"\n数据已保存到 /tmp/tencent_video_v8_raw.json", flush=True)

if __name__ == "__main__":
    main()
