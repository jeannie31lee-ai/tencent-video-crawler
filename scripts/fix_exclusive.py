#!/usr/bin/env python3
"""
V7版本独播修正：
核心修正规则：
1. 电影：2016年前的"独播"标签不可信（老电影在1905/优酷等平台普遍有播出），改为非独播
   - 但保留用户明确确认的独播电影
2. 电视剧：保持API tag_2标记（不做年份截断），因为许多早期网剧确实是独播
3. 用户新增的5个非独播修正：姐妹情缘/金山/走路上学/泥鳅也是鱼/北川重生
4. 移除之前错误加入独播名单的项：火影剧场版、恋爱大作战等（在其他平台有播出）
"""

import json

# ===== 用户累计确认的修正列表 =====

# 用户确认为独播的电视剧（API可能遗漏）
USER_CONFIRMED_EXCLUSIVE_TV = {
    # 2015年后
    "陈情令", "庆余年", "听雪楼", "梦回", "双世宠妃", "双世宠妃2",
    "三生三世枕上书", "龙岭迷窟", "传闻中的陈芊芊", "有翡",
    "斗罗大陆", "长歌行", "你是我的荣耀", "雪中悍刀行", "司藤", "锦心似玉",
    "开端", "梦华录", "星汉灿烂", "卿卿日常", "猎罪图鉴", "且试天下",
    "长相思", "莲花楼", "狂飙", "三体", "长月烬明",
    "庆余年第二季", "玫瑰的故事", "永夜星河", "九重紫",
    "大奉打更人", "国色芳华", "五福临门", "了不起的曹萱萱",
    "今夕何夕", "我的小确幸", "画江湖之天罡", "我的父亲母亲",
    "她的盛焰", "隐身的名字", "玫瑰丛生", "季雨倾城", "明珠奇谭", "临暗",
    "芳华里", "又野又烈", "藏匿爱意", "枕红妆", "重返青春", "去听旷野的风",
    # 2015年前 - 用户明确确认
    "医馆笑传", "渗透", "北平无战事", "战火四千金", "连环套",
    "大丈夫", "妈祖", "一仆二主", "神秘人质", "红色", "英雄使命",
}

# 用户确认为非独播的电视剧
USER_CONFIRMED_NON_EXCLUSIVE_TV = {
    "香蜜沉沉烬如霜", "三生三世十里桃花",   # 多平台联播
    "与凤行", "天下正道", "我家住在高岗上", "生死翻盘",
    "毒刺", "铁血雄心",                      # 老剧多平台
    "姐妹情缘",                               # V7新增：优酷有播出
}

# 用户确认为非独播的电影
USER_CONFIRMED_NON_EXCLUSIVE_MOVIE = {
    "金山",         # V7新增：1905电影网有播出
    "走路上学",     # V7新增：1905电影网有播出
    "泥鳅也是鱼",   # V7新增：1905电影网有播出
    "北川重生",     # V7新增：腾讯有搜索但播放跳外站
    "黄沙渡",       # V6已确认非独播
}

# 用户确认为独播的电影 (优先级最高)
USER_CONFIRMED_EXCLUSIVE_MOVIE = {
    # 无 - 暂无用户确认的独播电影需要强制覆盖
}


def main():
    # Load V6 data
    with open("/tmp/tencent_video_v6.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    print("=" * 60)
    print("V7 独播修正")
    print("=" * 60)

    # ===== 电视剧修正 =====
    tv_items = data.get("电视剧", [])
    old_tv_excl = sum(1 for it in tv_items if it["是否独播"] == "独播")
    
    tv_changes = {"to_excl": [], "to_non": []}
    for it in tv_items:
        title = it["剧名"]
        old_status = it["是否独播"]
        
        # Priority 1: 用户确认
        if title in USER_CONFIRMED_EXCLUSIVE_TV:
            new_status = "独播"
        elif title in USER_CONFIRMED_NON_EXCLUSIVE_TV:
            new_status = "非独播"
        else:
            # Priority 2: 保持V6的API标记（不做年份截断）
            new_status = old_status
        
        if old_status != new_status:
            if new_status == "独播":
                tv_changes["to_excl"].append(f"{title} ({it.get('年份', '')})")
            else:
                tv_changes["to_non"].append(f"{title} ({it.get('年份', '')})")
        it["是否独播"] = new_status
    
    new_tv_excl = sum(1 for it in tv_items if it["是否独播"] == "独播")
    print(f"\n电视剧:")
    print(f"  修正前: {old_tv_excl} 部独播")
    print(f"  修正后: {new_tv_excl} 部独播")
    if tv_changes["to_excl"]:
        print(f"  新增独播: {tv_changes['to_excl']}")
    if tv_changes["to_non"]:
        print(f"  移除独播: {tv_changes['to_non']}")

    # ===== 电影修正 =====
    movie_items = data.get("电影", [])
    old_mv_excl = sum(1 for it in movie_items if it["是否独播"] == "独播")
    
    mv_changes = {"to_excl": [], "to_non": []}
    for it in movie_items:
        title = it["剧名"]
        old_status = it["是否独播"]
        year_str = str(it.get("年份", ""))
        try:
            year = int(year_str) if year_str.isdigit() else 0
        except:
            year = 0
        
        # Priority 1: 用户确认
        if title in USER_CONFIRMED_EXCLUSIVE_MOVIE:
            new_status = "独播"
        elif title in USER_CONFIRMED_NON_EXCLUSIVE_MOVIE:
            new_status = "非独播"
        # Priority 2: 电影2016年前 → 非独播
        # 原因：老电影在1905电影网/优酷/爱奇艺等平台普遍有播出
        #       用户反馈的金山(2009)/走路上学(2009)/泥鳅也是鱼(2005)/北川重生(2011)均证实此规律
        elif year > 0 and year < 2016 and old_status == "独播":
            new_status = "非独播"
        else:
            new_status = old_status
        
        if old_status != new_status:
            if new_status == "独播":
                mv_changes["to_excl"].append(f"{title} ({year_str})")
            else:
                mv_changes["to_non"].append(f"{title} ({year_str})")
        it["是否独播"] = new_status
    
    new_mv_excl = sum(1 for it in movie_items if it["是否独播"] == "独播")
    print(f"\n电影:")
    print(f"  修正前: {old_mv_excl} 部独播")
    print(f"  修正后: {new_mv_excl} 部独播 (减少了 {old_mv_excl - new_mv_excl} 部)")
    print(f"  移除独播(2016前): {len([c for c in mv_changes['to_non'] if c not in [f'{t} ({year_str})' for t in USER_CONFIRMED_NON_EXCLUSIVE_MOVIE]])} 部")
    
    if mv_changes["to_non"]:
        print(f"  被移除的独播电影:")
        for c in sorted(mv_changes["to_non"])[:30]:
            print(f"    - {c}")
        if len(mv_changes["to_non"]) > 30:
            print(f"    ... 共 {len(mv_changes['to_non'])} 部")

    # ===== 验证 =====
    print(f"\n{'=' * 60}")
    print("验证关键项目")
    print("=" * 60)
    
    verify_list = [
        # 用户V7新增修正
        "姐妹情缘", "金山", "走路上学", "泥鳅也是鱼", "北川重生",
        # 之前确认的
        "医馆笑传", "渗透", "北平无战事", "大丈夫", "红色",
        "与凤行", "天下正道", "香蜜沉沉烬如霜",
        "陈情令", "庆余年第二季", "九重紫",
        # 经典老电影 (应为非独播)
        "乱世佳人", "地道战", "小兵张嘎", "新龙门客栈", "食神", "喜剧之王",
        # 较新的独播电影
        "异人之下", "唐门斗罗传",
    ]
    for cat in ["电视剧", "电影"]:
        for it in data[cat]:
            if it["剧名"] in verify_list:
                print(f"  {it['剧名']} ({it.get('年份','')}) [{cat}] => {it['是否独播']}")

    # Save
    with open("/tmp/tencent_video_v7.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'=' * 60}")
    print(f"V7最终统计:")
    total = 0
    for cat in ["电视剧", "电影"]:
        items = data.get(cat, [])
        excl = sum(1 for it in items if it["是否独播"] == "独播")
        total += len(items)
        pct = excl / len(items) * 100 if items else 0
        print(f"  {cat}: {len(items)}部, 独播{excl}部({pct:.1f}%), 非独播{len(items)-excl}部")
    print(f"  总计: {total}部")
    print(f"\n数据已保存到 /tmp/tencent_video_v7.json")

if __name__ == "__main__":
    main()
