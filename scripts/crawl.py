#!/usr/bin/env python3
"""
腾讯视频全量爬虫 V5 - 多维度爬取 + 合并去重
策略：
  电视剧: 按年份(iyear=1~17) + 按排序(sort=18/75) 多维度爬取，合并去重
  电影: 按排序(sort=18) 全量爬取（可达5000部），再按年份补漏
"""

import requests, json, time, re, sys
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
CHANNELS = {"100113": "电视剧", "100173": "电影"}
PAY_TYPES_MAP = {"1": "免费", "2": "限免", "3": "会员"}
FEMALE_GENRES = {"爱情", "家庭", "青春", "古装", "宫斗", "甜宠", "都市"}
MALE_GENRES = {"军旅", "刑侦", "竞技", "武侠", "科幻", "战争", "谍战", "悬疑", "权谋", "猎奇"}

# 已验证的腾讯视频独播剧（国内网络平台独播）
VERIFIED_EXCLUSIVE = {
    "青云志", "鬼吹灯之精绝古城", "九州天空城", "法医秦明",
    "三生三世枕上书", "龙岭迷窟", "传闻中的陈芊芊", "有翡", "隐秘而伟大",
    "陈情令", "庆余年", "听雪楼", "倚天屠龙记", "怒晴湘西", "梦回",
    "双世宠妃", "双世宠妃2", "如懿传", "沙海", "将夜",
    "致我们单纯的小美好",
    "斗罗大陆", "长歌行", "你是我的荣耀", "雪中悍刀行", "司藤", "锦心似玉",
    "开端", "梦华录", "星汉灿烂", "卿卿日常", "猎罪图鉴", "且试天下",
    "长相思", "莲花楼", "狂飙", "三体", "长月烬明",
    "庆余年第二季", "与凤行", "玫瑰的故事", "永夜星河", "九重紫",
    "大奉打更人", "国色芳华", "五福临门", "了不起的曹萱萱",
    "今夕何夕", "我的小确幸", "画江湖之天罡", "我的父亲母亲",
    "她的盛焰", "隐身的名字", "玫瑰丛生", "季雨倾城", "明珠奇谭", "临暗",
    "芳华里", "又野又烈", "藏匿爱意", "枕红妆", "重返青春", "去听旷野的风",
}
VERIFIED_NON_EXCLUSIVE = {
    "香蜜沉沉烬如霜", "三生三世十里桃花",
}

def fetch_page(channel_id, filter_params="sort=75", page_context=None):
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
            # 独播检测: latest_mark_label position 2 id="15"
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
    except Exception as e:
        pass
    return items

def crawl_all_pages(channel_id, filter_params, max_pages=500):
    """爬取指定筛选条件下的所有页"""
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
        time.sleep(0.15)
        if page_num % 30 == 0:
            print(f"      page {page_num}: {len(all_items)} items")
    return all_items

def crawl_titles_set(channel_id, filter_params, max_pages=500):
    """仅爬取标题集合（用于付费类型匹配）"""
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
        time.sleep(0.15)
    return titles

def merge_items(all_items_list):
    """按CID合并去重，优先保留信息更完整的"""
    merged = {}
    for it in all_items_list:
        cid = it["cid"]
        if cid not in merged:
            merged[cid] = it
        else:
            # 如果新记录有独播标记而旧的没有，用新的
            old = merged[cid]
            if it["exclusive"] == "独播" and old["exclusive"] != "独播":
                merged[cid]["exclusive"] = "独播"
            # 补充缺失信息
            for key in ["episode_info", "first_vid", "genre_tags"]:
                if not old.get(key) and it.get(key):
                    merged[cid][key] = it[key]
    return list(merged.values())

def get_vid_duration(vid):
    try:
        resp = requests.get(VID_INFO_URL, params={"vid": vid, "platform": "10201", "otype": "json", "defn": "sd"},
                           headers={"Referer": "https://v.qq.com/"}, timeout=10)
        text = resp.text
        if text.startswith("QZOutputJson="):
            text = text[len("QZOutputJson="):]
            if text.endswith(";"):
                text = text[:-1]
        data = json.loads(text)
        vi_list = data.get("vl", {}).get("vi", [])
        if vi_list:
            return float(vi_list[0].get("td", "0"))
    except: pass
    return -1

def get_durations_batch(vid_list, max_workers=15):
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(get_vid_duration, vid): vid for vid in vid_list if vid}
        done = 0
        for future in as_completed(future_map):
            vid = future_map[future]
            try: results[vid] = future.result()
            except: results[vid] = -1
            done += 1
            if done % 200 == 0:
                print(f"      duration checked: {done}/{len(future_map)}")
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
    all_results = {}

    # ============ 电视剧: 多维度爬取 ============
    print("=" * 60)
    print("电视剧: 多维度爬取")
    print("=" * 60)

    tv_all = []

    # 维度1: 按年份(iyear=1~17)爬取
    year_labels = {
        1: "2025", 2: "2024", 3: "2023", 4: "2022", 5: "2021",
        6: "2020", 7: "2019", 8: "2018", 9: "2017", 10: "2016",
        11: "2011-2015", 12: "2006-2010", 13: "2000-2005",
        14: "90年代", 15: "80年代", 16: "更早", 17: "2026",
    }
    for iyear, label in year_labels.items():
        print(f"  iyear={iyear} ({label})...")
        items = crawl_all_pages("100113", f"sort=75&iyear={iyear}")
        print(f"    => {len(items)} 部")
        tv_all.extend(items)
        time.sleep(0.3)

    # 维度2: 按最新排序补充
    print(f"  sort=18 (最新)...")
    items_new = crawl_all_pages("100113", "sort=18")
    print(f"    => {len(items_new)} 部")
    tv_all.extend(items_new)

    # 维度3: 按最热排序补充
    print(f"  sort=75 (最热)...")
    items_hot = crawl_all_pages("100113", "sort=75")
    print(f"    => {len(items_hot)} 部")
    tv_all.extend(items_hot)

    # 合并去重
    tv_merged = merge_items(tv_all)
    print(f"\n  电视剧合并去重: {len(tv_all)} -> {len(tv_merged)} 部")

    # ============ 电影: 多维度爬取 ============
    print(f"\n{'=' * 60}")
    print("电影: 多维度爬取")
    print("=" * 60)

    movie_all = []

    # 维度1: sort=18全量 (可达5000)
    print(f"  sort=18 (最新)...")
    items_new = crawl_all_pages("100173", "sort=18")
    print(f"    => {len(items_new)} 部")
    movie_all.extend(items_new)

    # 维度2: sort=75补充
    print(f"  sort=75 (最热)...")
    items_hot = crawl_all_pages("100173", "sort=75")
    print(f"    => {len(items_hot)} 部")
    movie_all.extend(items_hot)

    # 维度3: 按年份补漏（取较大年份段）
    for iyear, label in [(11, "2011-2015"), (12, "2006-2010"), (13, "2000-2005"), (14, "90年代"), (15, "80年代"), (16, "更早")]:
        print(f"  iyear={iyear} ({label})...")
        items = crawl_all_pages("100173", f"sort=75&iyear={iyear}")
        print(f"    => {len(items)} 部")
        movie_all.extend(items)
        time.sleep(0.3)

    movie_merged = merge_items(movie_all)
    print(f"\n  电影合并去重: {len(movie_all)} -> {len(movie_merged)} 部")

    # ============ 确定付费类型 ============
    print(f"\n{'=' * 60}")
    print("确定付费类型")
    print("=" * 60)

    for channel_id, channel_name, items in [("100113", "电视剧", tv_merged), ("100173", "电影", movie_merged)]:
        print(f"\n  {channel_name}:")
        # 爬取各付费类型的标题集合
        vip_titles = crawl_titles_set(channel_id, "sort=75&ipay=3")
        free_titles = crawl_titles_set(channel_id, "sort=75&ipay=1")
        limited_titles = crawl_titles_set(channel_id, "sort=75&ipay=2")

        print(f"    会员: {len(vip_titles)}, 免费: {len(free_titles)}, 限免: {len(limited_titles)}")

        for it in items:
            t = it["title"]
            if channel_name == "电影":
                # 电影: VIP优先级
                if t in vip_titles:
                    it["pay_type"] = "会员"
                elif t in free_titles:
                    it["pay_type"] = "免费"
                elif t in limited_titles:
                    it["pay_type"] = "限免"
                else:
                    it["pay_type"] = "付费"
            else:
                # 电视剧: 先到先得
                if t in free_titles:
                    it["pay_type"] = "免费"
                elif t in limited_titles:
                    it["pay_type"] = "限免"
                elif t in vip_titles:
                    it["pay_type"] = "会员"
                else:
                    it["pay_type"] = "付费"

        # Normalize: 限免 -> 会员
        for it in items:
            if it["pay_type"] == "限免":
                it["pay_type"] = "会员"

        pay_dist = Counter(it["pay_type"] for it in items)
        print(f"    分布: {dict(pay_dist)}")

    # ============ 过滤短剧 ============
    print(f"\n{'=' * 60}")
    print("过滤短剧 (单集≤20分钟)")
    print("=" * 60)

    # 电视剧短剧过滤
    tv_vids = [it["first_vid"] for it in tv_merged if it["first_vid"]]
    print(f"  电视剧: 检查 {len(tv_vids)} 部时长...")
    tv_durations = get_durations_batch(tv_vids, max_workers=15)

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
    print(f"  电视剧: 排除 {short_count} 部短剧, 保留 {len(filtered_tv)} 部")

    # 电影短片过滤
    mv_vids = [it["first_vid"] for it in movie_merged if it["first_vid"]]
    print(f"  电影: 检查 {len(mv_vids)} 部时长...")
    mv_durations = get_durations_batch(mv_vids, max_workers=15)

    filtered_movie = []
    short_mv = 0
    for it in movie_merged:
        vid = it["first_vid"]
        dur = mv_durations.get(vid, -1)
        dur_min = dur / 60 if dur > 0 else -1
        if dur_min > 0 and dur_min <= 20:
            short_mv += 1
        else:
            # 电影集数改为时长格式
            if dur > 0:
                it["episode_info"] = f"{int(dur / 60)}分钟"
            filtered_movie.append(it)
    print(f"  电影: 排除 {short_mv} 部短片, 保留 {len(filtered_movie)} 部")

    # ============ 修正独播标记 ============
    print(f"\n{'=' * 60}")
    print("修正独播标记")
    print("=" * 60)

    for items in [filtered_tv, filtered_movie]:
        for it in items:
            title = it["title"]
            year_str = str(it.get("year", ""))
            try:
                year = int(year_str) if year_str.isdigit() else 0
            except:
                year = 0

            # Rule 1: 已验证的独播剧
            if title in VERIFIED_EXCLUSIVE:
                it["exclusive"] = "独播"
            # Rule 2: 已验证的非独播剧
            elif title in VERIFIED_NON_EXCLUSIVE:
                it["exclusive"] = "非独播"
            # Rule 3: 2015年前的"独播"标记不可信
            elif year < 2015 and year > 0 and it["exclusive"] == "独播":
                it["exclusive"] = "非独播"

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

    with open("/tmp/tencent_video_v5.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    total = sum(len(v) for v in output.values())
    print(f"\n{'=' * 60}")
    print(f"完成! 总计 {total} 部")
    for name, items in output.items():
        excl = sum(1 for it in items if it["是否独播"] == "独播")
        non_excl = sum(1 for it in items if it["是否独播"] == "非独播")
        pay = Counter(it["付费类型"] for it in items)
        print(f"  {name}: {len(items)} 部, 独播{excl}, 非独播{non_excl}")
        print(f"    付费: {dict(pay)}")

    # Check previously missing shows
    print(f"\n验证之前缺失的剧目:")
    check = ["繁华落尽", "二八杠的夏天", "破产姐妹", "机械师"]
    for name in check:
        found = False
        for cat, items in output.items():
            for it in items:
                if name in it["剧名"]:
                    print(f"  {it['剧名']} ({cat}) => 找到! 独播={it['是否独播']}")
                    found = True
        if not found:
            print(f"  {name} => 仍缺失")

if __name__ == "__main__":
    main()
