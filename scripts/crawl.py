#!/usr/bin/env python3
"""
腾讯视频全量爬虫 V9 - 增强过滤版
基于V8，新增三项过滤规则：
  1. 过滤仅有预告片的待播影剧（getinfo返回no_video/td=0）
  2. 过滤外站播放链接（getinfo返回no_video）
  3. 过滤集均时长<20min的短剧（td≤1200s）
关键改进：
  - 保存中间数据含VID，支持断点续跑
  - getinfo检查所有条目（非仅TV）
  - 返回status+duration双重信号
"""

import requests, json, time, sys, os
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

# ===== Checkpoints =====
CHECKPOINT_CRAWL = "/tmp/tencent_v9_crawl.json"
CHECKPOINT_DURATIONS = "/tmp/tencent_v9_durations.json"
OUTPUT_FILE = "/tmp/tencent_video_v9.json"

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

# ===== 独播修正名单 =====
FORCE_NON_EXCLUSIVE = {"姐妹情缘", "金山", "走路上学", "泥鳅也是鱼", "北川重生"}

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
            all_ids_count = 0
            if all_ids_raw:
                try:
                    all_ids = json.loads(all_ids_raw) if isinstance(all_ids_raw, str) else all_ids_raw
                    all_ids_count = len(all_ids)
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
                "all_ids_count": all_ids_count,
                "first_F": str(p.get("first_F", "")),
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
            if it.get("all_ids_count", 0) > old.get("all_ids_count", 0):
                merged[cid]["all_ids_count"] = it["all_ids_count"]
    return list(merged.values())

def get_vid_info(vid):
    """返回 (duration_seconds, status_str)"""
    for attempt in range(2):
        try:
            resp = requests.get(VID_INFO_URL, params={"vid": vid, "platform": "10201", "otype": "json", "defn": "sd"},
                               headers={"Referer": "https://v.qq.com/", "User-Agent": "Mozilla/5.0"}, timeout=10)
            text = resp.text
            if text.startswith("QZOutputJson="): text = text[len("QZOutputJson="):]
            if text.endswith(";"): text = text[:-1]
            data = json.loads(text)
            vi_list = data.get("vl", {}).get("vi", [])
            if vi_list:
                td = float(vi_list[0].get("td", "0"))
                return (td, "ok")
            # No vi list → check for error msg
            msg = data.get("msg", "")
            if "no_video" in msg or not vi_list:
                return (0, "no_video")
        except Exception as e:
            if attempt == 0:
                time.sleep(1)
    return (-1, "error")

def get_vid_infos_batch(vid_list, max_workers=30):
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(get_vid_info, vid): vid for vid in vid_list if vid}
        done = 0
        total = len(future_map)
        for future in as_completed(future_map):
            vid = future_map[future]
            try:
                results[vid] = future.result()
            except:
                results[vid] = (-1, "error")
            done += 1
            if done % 500 == 0:
                print(f"      getinfo checked: {done}/{total}", flush=True)
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


def phase1_crawl():
    """Phase 1: 爬取全量数据并保存含VID的中间数据"""
    if os.path.exists(CHECKPOINT_CRAWL):
        print(f"\n[Phase 1] 加载爬取缓存: {CHECKPOINT_CRAWL}", flush=True)
        with open(CHECKPOINT_CRAWL, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"  电视剧: {len(data['tv'])} 部, 电影: {len(data['movie'])} 部", flush=True)
        return data

    print("=" * 60)
    print("[Phase 1] 电视剧: 年份x排序 全维度爬取")
    print("=" * 60, flush=True)

    tv_all = []

    # 维度1: 每个年份段 × 每种排序
    for yval, ylabel in TV_YEARS:
        for sval, slabel in TV_SORTS:
            fp = f"sort={sval}&iyear={yval}"
            items = crawl_all_pages("100113", fp)
            tv_all.extend(items)
            print(f"  {ylabel} x {slabel}: {len(items)}", flush=True)
            time.sleep(0.1)

    # 维度2: 撞上限的年份段(2010-2000)按地区拆分
    print("  [2010-2000 按地区拆分补充]", flush=True)
    for aval, alabel in TV_AREAS:
        for sval, slabel in TV_SORTS:
            fp = f"sort={sval}&iyear=8&iarea={aval}"
            items = crawl_all_pages("100113", fp)
            if items:
                tv_all.extend(items)
                print(f"    {alabel} x {slabel}: {len(items)}", flush=True)
            time.sleep(0.1)

    # 维度3: 无年份的各sort
    for sval, slabel in TV_SORTS:
        items = crawl_all_pages("100113", f"sort={sval}")
        tv_all.extend(items)
        print(f"  全部 x {slabel}: {len(items)}", flush=True)

    tv_merged = merge_items(tv_all)
    print(f"\n  电视剧合并去重: {len(tv_all)} -> {len(tv_merged)} 部", flush=True)

    # ============ 电影 ============
    print(f"\n{'=' * 60}")
    print("[Phase 1] 电影: 年份x排序 全维度爬取")
    print("=" * 60, flush=True)

    movie_all = []

    for yval, ylabel in MV_YEARS:
        for sval, slabel in MV_SORTS:
            fp = f"sort={sval}&iyear={yval}"
            items = crawl_all_pages("100173", fp)
            movie_all.extend(items)
            print(f"  {ylabel} x {slabel}: {len(items)}", flush=True)
            time.sleep(0.1)

    for sval, slabel in MV_SORTS:
        items = crawl_all_pages("100173", f"sort={sval}")
        movie_all.extend(items)
        print(f"  全部 x {slabel}: {len(items)}", flush=True)

    movie_merged = merge_items(movie_all)
    print(f"\n  电影合并去重: {len(movie_all)} -> {len(movie_merged)} 部", flush=True)

    # ============ 确定付费类型 ============
    print(f"\n{'=' * 60}")
    print("[Phase 1] 确定付费类型")
    print("=" * 60, flush=True)

    tv_free = crawl_titles_set("100113", f"sort=75&ipay=1")
    tv_limited = crawl_titles_set("100113", f"sort=75&ipay=2")
    tv_vip = crawl_titles_set("100113", f"sort=75&ipay=3")
    print(f"  电视剧: 免费{len(tv_free)}, 限免{len(tv_limited)}, 会员{len(tv_vip)}", flush=True)

    for it in tv_merged:
        t = it["title"]
        if t in tv_free: it["pay_type"] = "免费"
        elif t in tv_limited: it["pay_type"] = "会员"
        elif t in tv_vip: it["pay_type"] = "会员"
        else: it["pay_type"] = "付费"

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

    # Save checkpoint
    data = {"tv": tv_merged, "movie": movie_merged}
    with open(CHECKPOINT_CRAWL, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n  爬取完成，保存到 {CHECKPOINT_CRAWL}", flush=True)
    print(f"  电视剧: {len(tv_merged)} 部, 电影: {len(movie_merged)} 部", flush=True)

    return data


def phase2_check_durations(data):
    """Phase 2: 检查所有条目的getinfo状态和时长"""
    if os.path.exists(CHECKPOINT_DURATIONS):
        print(f"\n[Phase 2] 加载时长缓存: {CHECKPOINT_DURATIONS}", flush=True)
        with open(CHECKPOINT_DURATIONS, "r", encoding="utf-8") as f:
            return json.load(f)

    print(f"\n{'=' * 60}")
    print("[Phase 2] 检查所有VID时长和状态")
    print("=" * 60, flush=True)

    all_items = data["tv"] + data["movie"]
    all_vids = list(set(it["first_vid"] for it in all_items if it.get("first_vid")))
    print(f"  共 {len(all_vids)} 个唯一VID需检查", flush=True)

    vid_infos = get_vid_infos_batch(all_vids, max_workers=30)

    # Convert tuples to lists for JSON serialization
    results = {}
    for vid, (td, status) in vid_infos.items():
        results[vid] = {"td": td, "status": status}

    # Stats
    ok_count = sum(1 for v in results.values() if v["status"] == "ok" and v["td"] > 0)
    no_video = sum(1 for v in results.values() if v["status"] == "no_video" or (v["status"] == "ok" and v["td"] == 0))
    err_count = sum(1 for v in results.values() if v["status"] == "error")
    print(f"  结果: 有视频={ok_count}, 无视频/td=0={no_video}, 错误={err_count}", flush=True)

    with open(CHECKPOINT_DURATIONS, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"  时长数据保存到 {CHECKPOINT_DURATIONS}", flush=True)

    return results


def phase3_filter_and_output(data, durations):
    """Phase 3: 应用V9过滤规则 + 独播修正 + 输出"""
    print(f"\n{'=' * 60}")
    print("[Phase 3] 应用V9过滤规则")
    print("=" * 60, flush=True)

    output = {}
    total_removed = {"trailer_novideo": 0, "short_drama": 0}

    for category, items in [("电视剧", data["tv"]), ("电影", data["movie"])]:
        kept = []
        removed_trailer = []
        removed_short = []

        for it in items:
            vid = it.get("first_vid", "")
            dur_info = durations.get(vid, {"td": -1, "status": "unknown"})
            td = dur_info.get("td", -1)
            status = dur_info.get("status", "unknown")

            # Rule 1+2: 待播预告 / 外站链接 → getinfo返回no_video或td=0
            if status == "no_video" or (status == "ok" and td == 0):
                removed_trailer.append(it["title"])
                continue

            # Rule 3: 短剧 (0 < td <= 1200 即 ≤20分钟)
            if td > 0 and td <= 1200:
                removed_short.append(it["title"])
                continue

            # 通过筛选 → 保留
            kept.append(it)

        print(f"\n  {category}:", flush=True)
        print(f"    原始: {len(items)} 部", flush=True)
        print(f"    移除(无视频/预告/外站): {len(removed_trailer)} 部", flush=True)
        print(f"    移除(短剧≤20min): {len(removed_short)} 部", flush=True)
        print(f"    保留: {len(kept)} 部", flush=True)

        total_removed["trailer_novideo"] += len(removed_trailer)
        total_removed["short_drama"] += len(removed_short)

        if removed_trailer:
            print(f"    [无视频/预告/外站 样本]: {removed_trailer[:10]}", flush=True)
        if removed_short:
            print(f"    [短剧 样本]: {removed_short[:10]}", flush=True)

        # ===== 独播修正 =====
        corrected = 0
        for it in kept:
            title = it["title"]

            # 修正1: 用户指定的非独播名单
            if title in FORCE_NON_EXCLUSIVE and it["exclusive"] == "独播":
                it["exclusive"] = "非独播"
                corrected += 1

            # 修正2: 2016年前的电影，标记为独播但实际在多平台
            if category == "电影" and it["exclusive"] == "独播":
                try:
                    y = int(it["year"]) if it["year"] else 9999
                    if y < 2016:
                        it["exclusive"] = "非独播"
                        corrected += 1
                except:
                    pass

        if corrected:
            print(f"    独播修正: {corrected} 部", flush=True)

        # 构建输出
        output[category] = []
        for it in kept:
            # 电影：用时长替换集数
            ep_info = it["episode_info"]
            if category == "电影":
                vid = it.get("first_vid", "")
                dur_info = durations.get(vid, {"td": -1})
                td = dur_info.get("td", -1)
                if td > 0:
                    ep_info = f"{int(td / 60)}分钟"

            output[category].append({
                "平台": "腾讯视频",
                "剧名": it["title"],
                "类型": category,
                "是否独播": it["exclusive"],
                "付费类型": it["pay_type"],
                "年份": it["year"],
                "演员": it["leading_actor"],
                "地区": it["area_name"],
                "集数": ep_info,
                "题材": it["main_genre"],
                "男女频": determine_gender(it["main_genre"], it["genre_tags"]),
            })

    # 保存最终数据
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    total = sum(len(v) for v in output.values())
    print(f"\n{'=' * 60}")
    print(f"V9 完成! 总计 {total} 部")
    print(f"  移除(无视频/预告/外站): {total_removed['trailer_novideo']} 部")
    print(f"  移除(短剧≤20min): {total_removed['short_drama']} 部")
    for name, items in output.items():
        excl = sum(1 for it in items if it["是否独播"] == "独播")
        pay = Counter(it["付费类型"] for it in items)
        print(f"  {name}: {len(items)} 部, 独播{excl}")
        print(f"    付费: {dict(pay)}")
    print(f"\n数据已保存到 {OUTPUT_FILE}", flush=True)


def main():
    print("腾讯视频全量爬虫 V9 - 增强过滤版")
    print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60, flush=True)

    # Phase 1: 爬取
    data = phase1_crawl()

    # Phase 2: 检查时长
    durations = phase2_check_durations(data)

    # Phase 3: 过滤输出
    phase3_filter_and_output(data, durations)


if __name__ == "__main__":
    main()
