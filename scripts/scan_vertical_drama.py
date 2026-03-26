"""
扫描所有电视剧和电影页面，检测：
1. 竖屏短剧（页面含"竖屏"标记）
2. 无资源/已下架内容
3. 微短剧标记
"""
import json, requests, re, time
from concurrent.futures import ThreadPoolExecutor, as_completed

with open('/tmp/tencent_video_v9.json') as f:
    v9 = json.load(f)

# 构建待扫描列表：当前输出中的所有电视剧
with open('/tmp/tencent_v9_crawl.json') as f:
    crawl = json.load(f)

# 映射 title -> cid
title_to_cid = {}
for cat in ['tv', 'movie']:
    for it in crawl[cat]:
        title_to_cid[it['title']] = it['cid']

# 当前输出中的项目
scan_items = []
for it in v9['电视剧']:
    cid = title_to_cid.get(it['剧名'])
    if cid:
        scan_items.append(('电视剧', it['剧名'], cid))
for it in v9['电影']:
    cid = title_to_cid.get(it['剧名'])
    if cid:
        scan_items.append(('电影', it['剧名'], cid))

print(f"待扫描: 电视剧 {sum(1 for c,_,_ in scan_items if c=='电视剧')}, 电影 {sum(1 for c,_,_ in scan_items if c=='电影')}")

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}

vertical_items = []  # 竖屏短剧
no_resource = []     # 无资源
micro_drama = []     # 微短剧标记
errors = []

def scan_page(cat, title, cid):
    url = f"https://v.qq.com/x/cover/{cid}.html"
    try:
        resp = requests.get(url, timeout=15, headers=headers)
        text = resp.text
        
        flags = []
        # 检查竖屏标记
        if '竖屏' in text[:50000]:
            flags.append('竖屏')
        # 检查微短剧标记
        if '微短剧' in text[:50000]:
            flags.append('微短剧')
        # 检查短剧标记（在特定上下文中）
        if '短剧' in text[:10000] and '微短剧' not in text[:50000]:
            # 避免误判 - 检查是否在特定标签中
            if re.search(r'(class|tag|label|category)[^>]*短剧', text[:50000]):
                flags.append('短剧标签')
        # 检查无资源
        if '暂无视频' in text or '暂无资源' in text or '敬请期待' in text[:10000]:
            flags.append('无资源')
        # 检查404/下架
        if resp.status_code == 404 or '找不到' in text[:5000] or '已下架' in text[:5000]:
            flags.append('已下架')
        # 检查是否只有预告
        if '预告' in text[:3000] and '正片' not in text[:10000]:
            # 这个太宽泛了，先不用
            pass
            
        return (cat, title, cid, flags)
    except Exception as e:
        return (cat, title, cid, [f'error:{str(e)[:30]}'])

total = len(scan_items)
done = 0
start = time.time()

print("开始扫描...")
with ThreadPoolExecutor(max_workers=20) as pool:
    futures = {pool.submit(scan_page, cat, title, cid): (cat, title) for cat, title, cid in scan_items}
    for fut in as_completed(futures):
        done += 1
        cat, title, cid, flags = fut.result()
        if flags:
            if '竖屏' in flags or '微短剧' in flags:
                vertical_items.append((cat, title, cid, flags))
            if '无资源' in flags or '已下架' in flags:
                no_resource.append((cat, title, cid, flags))
            if 'error' in str(flags):
                errors.append((cat, title, cid, flags))
        if done % 500 == 0:
            elapsed = time.time() - start
            rate = done / elapsed
            eta = (total - done) / rate
            print(f"  进度: {done}/{total} ({done*100/total:.1f}%), 速率: {rate:.1f}/s, 预计剩余: {eta:.0f}s")

elapsed = time.time() - start
print(f"\n扫描完成: {total} 项, 耗时 {elapsed:.0f}s")

print(f"\n=== 竖屏/微短剧 ({len(vertical_items)} 项) ===")
for cat, title, cid, flags in sorted(vertical_items, key=lambda x: x[0]):
    print(f"  [{cat}] {title} | {','.join(flags)}")

print(f"\n=== 无资源/已下架 ({len(no_resource)} 项) ===")
for cat, title, cid, flags in sorted(no_resource, key=lambda x: x[0]):
    print(f"  [{cat}] {title} | {','.join(flags)}")

if errors:
    print(f"\n=== 扫描错误 ({len(errors)} 项) ===")
    for cat, title, cid, flags in errors[:10]:
        print(f"  [{cat}] {title} | {','.join(flags)}")

# 保存结果
result = {
    'vertical_drama': [(cat, title, cid) for cat, title, cid, _ in vertical_items],
    'no_resource': [(cat, title, cid) for cat, title, cid, _ in no_resource],
    'vertical_tv_cids': [cid for cat, _, cid, _ in vertical_items if cat == '电视剧'],
    'vertical_mv_cids': [cid for cat, _, cid, _ in vertical_items if cat == '电影'],
    'no_resource_cids': [cid for _, _, cid, _ in no_resource],
}
with open('/tmp/v9_vertical_scan.json', 'w') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print(f"\n结果已保存到 /tmp/v9_vertical_scan.json")
