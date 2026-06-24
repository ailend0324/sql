import os
import re

md_dir = os.path.expanduser('~/Downloads/obsidian_md/001 - 晨间日记')
out_dir = os.path.expanduser('~/Downloads/obsidian_md/成长梳理')
os.makedirs(out_dir, exist_ok=True)

# 按年份收集
by_year = {}

for filename in sorted(os.listdir(md_dir)):
    if not filename.endswith('.md'):
        continue

    # 从文件名提取日期，格式：晨间日记 YYYY-MMDD
    match = re.search(r'(\d{4})-(\d{4})', filename)
    if not match:
        continue
    year = match.group(1)
    mmdd = match.group(2)
    date_str = f"{year}-{mmdd[:2]}-{mmdd[2:]}"

    filepath = os.path.join(md_dir, filename)
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 提取 C、板块内容（到下一个字母板块为止）
    c_match = re.search(r'(C[、,，．.]\s*.+?)(?=\n[D-Z][、,，．.]|\Z)', content, re.DOTALL)
    if not c_match:
        continue

    c_content = c_match.group(1).strip()
    if len(c_content) < 20:  # 跳过空内容
        continue

    if year not in by_year:
        by_year[year] = []
    by_year[year].append((date_str, c_content))

# 按年份写出
for year, entries in sorted(by_year.items()):
    out_path = os.path.join(out_dir, f'成长梳理-{year}.md')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f'# 成长梳理 {year}\n\n')
        for date_str, content in entries:
            f.write(f'## {date_str}\n\n')
            f.write(content + '\n\n')
            f.write('---\n\n')
    print(f'{year}：{len(entries)} 条')

print('完成！输出目录：~/Downloads/obsidian_md/成长梳理')
