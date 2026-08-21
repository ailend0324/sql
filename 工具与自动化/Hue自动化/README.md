# Hue 查询执行器

把仓库里的 `.sql` 文件直接丢给 Hue 跑，结果落成 CSV —— 省掉「网页版粘贴 → 等 → 点 Download」这一串手动操作。

走的是 Hue 4.x 的 Notebook API，也就是网页编辑器点「执行」时调的那几个接口：

| 接口 | 作用 |
|------|------|
| `/accounts/login/` | 登录拿 session |
| `/notebook/api/execute/<引擎>` | 提交语句 |
| `/notebook/api/check_status` | 轮询状态 |
| `/notebook/api/fetch_result_data` | 分页取数（每页 5000 行） |
| `/notebook/api/close_statement` | 收尾释放 |

## 用法

```bash
pip install requests

export HUE_USER=你的用户名
export HUE_PASSWORD=你的密码

python3 工具与自动化/Hue自动化/hue_run.py \
    供应链/收货管理/收货到货同期对比_2024vs2025.sql \
    -o 到货同期对比.csv
```

跑完把 `到货同期对比.csv` 拖进 `供应链/收货管理/收货到货同期对比看板.html` 就出图。

## 参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `-o, --out` | `hue_result.csv` | 输出路径 |
| `--url` | `http://119.23.30.106:8889` | Hue 地址，也可用环境变量 `HUE_URL` |
| `--engine` | `impala` | `impala` 或 `hive` |
| `--database` | `drt` | 默认库 |
| `--dry-run` | — | 只打印将要执行的 SQL，不连服务器 |

## 关于 SQL 预处理

脚本会先剥掉 `/* */` 块注释和 `--` 行注释，再只取第一条语句（按第一个 `;` 切）。
这正好绕开仓库 AI 协作指南里记的那两个坑：**Impala 不认 emoji，也不认语句尾部的注释**。
所以仓库里那些带 emoji 表头、末尾附校验查询的 SQL 文件都能直接喂给它。

想确认到底会执行什么，先跑一次 `--dry-run`。

## 凭据

用户名密码只从环境变量读，脚本不写任何配置文件。本目录的 `.gitignore` 也拦掉了
`*.ini` / `*.csv` / `exports/`，别把密码或业务数据提交上来。

> 历史备注：这个目录原先有一版 `hue_automation.py`（2026-06-24 的 refactor 提交里删掉了）。
> 那一版调的 `/api/query/execute`、`/api/query/{id}/status` 并不是 Hue 的真实接口，
> 日志里唯一一次运行也停在登录失败。当前这版按真实 Notebook API 重写。
