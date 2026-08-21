#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hue 查询执行器 —— 把 .sql 文件丢给 Hue 跑，结果直接落成 CSV。

走的是 Hue 4.x 的 Notebook API（就是网页版编辑器点「执行」时调的那几个接口）：
    /accounts/login/            登录拿 session
    /notebook/api/execute/<引擎>   提交语句
    /notebook/api/check_status     轮询状态
    /notebook/api/fetch_result_data 分页取数
    /notebook/api/close_statement   收尾释放

用法：
    export HUE_USER=你的用户名
    export HUE_PASSWORD=你的密码
    python3 hue_run.py ../../供应链/收货管理/收货到货同期对比_2024vs2025.sql -o 到货同期对比.csv

常用参数：
    --engine impala|hive     默认 impala
    --database drt           默认 drt
    --url http://...:8889    默认 http://119.23.30.106:8889
    --dry-run                只打印将要执行的 SQL，不连服务器

依赖：requests（pip install requests）
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import uuid

try:
    import requests
except ImportError:
    sys.exit("缺少依赖：pip install requests")

DEFAULT_URL = "http://119.23.30.106:8889"
FETCH_ROWS = 5000          # 每页取多少行
POLL_INTERVAL = 2.0        # 轮询间隔（秒）


# ──────────────────────────────────────────────────────────────────────────
# SQL 预处理：Impala 不认 emoji 和语句尾部的注释，这里统一剥掉
# ──────────────────────────────────────────────────────────────────────────
def extract_statement(path):
    with open(path, encoding="utf-8") as f:
        raw = f.read()
    no_block = re.sub(r"/\*.*?\*/", " ", raw, flags=re.S)          # 去掉 /* */ 块注释
    no_line = re.sub(r"--[^\n]*", "", no_block)                    # 去掉 -- 行注释
    stmt = no_line.split(";")[0].strip()                           # 只取第一条语句
    if not stmt:
        sys.exit(f"{path} 里没找到可执行的 SQL")
    return stmt


# ──────────────────────────────────────────────────────────────────────────
class Hue:
    def __init__(self, url, user, password, engine, database):
        self.url = url.rstrip("/")
        self.user = user
        self.password = password
        self.engine = engine
        self.database = database
        self.s = requests.Session()
        self.s.headers["User-Agent"] = "hue_run.py"

    def _csrf(self):
        return self.s.cookies.get("csrftoken", "")

    def _post(self, path, data, timeout=120):
        r = self.s.post(
            self.url + path,
            data=data,
            headers={"X-CSRFToken": self._csrf(), "Referer": self.url + path},
            timeout=timeout,
        )
        r.raise_for_status()
        try:
            return r.json()
        except ValueError:
            raise RuntimeError(f"{path} 返回的不是 JSON（多半是掉登录了）：{r.text[:200]}")

    def login(self):
        login_url = self.url + "/accounts/login/"
        self.s.get(login_url, timeout=30)
        r = self.s.post(
            login_url,
            data={
                "username": self.user,
                "password": self.password,
                "csrfmiddlewaretoken": self._csrf(),
                "next": "/",
            },
            headers={"Referer": login_url},
            timeout=30,
            allow_redirects=True,
        )
        r.raise_for_status()
        # 登录失败时 Hue 会把登录页原样吐回来
        if "name=\"password\"" in r.text and "/accounts/login" in r.url:
            raise RuntimeError("登录失败：用户名或密码不对")
        if not self.s.cookies.get("sessionid"):
            raise RuntimeError("登录失败：没拿到 sessionid")
        print(f"✓ 已登录 {self.url}（用户 {self.user}）")

    def _payload(self, snippet, notebook_name):
        notebook = {
            "type": self.engine,
            "name": notebook_name,
            "isSaved": False,
            "sessions": [],
            "snippets": [snippet],
            "uuid": str(uuid.uuid4()),
        }
        return {"notebook": json.dumps(notebook), "snippet": json.dumps(snippet)}

    def run(self, sql, notebook_name="hue_run"):
        snippet = {
            "id": str(uuid.uuid4()),
            "type": self.engine,
            "status": "running",
            "statement": sql,
            "statement_raw": sql,
            "variables": [],
            "properties": {"settings": []},
            "database": self.database,
            "result": {},
        }

        res = self._post(f"/notebook/api/execute/{self.engine}", self._payload(snippet, notebook_name))
        if res.get("status") != 0:
            raise RuntimeError(f"提交失败：{res.get('message') or res}")
        handle = res["handle"]
        snippet["result"] = {"handle": handle}
        print(f"✓ 已提交，等待执行…")

        waited = 0.0
        while True:
            st = self._post("/notebook/api/check_status", self._payload(snippet, notebook_name))
            status = (st.get("query_status") or {}).get("status")
            if status == "available":
                break
            if status in ("failed", "expired"):
                raise RuntimeError(f"执行失败：{st.get('message') or st}")
            time.sleep(POLL_INTERVAL)
            waited += POLL_INTERVAL
            if waited % 20 < POLL_INTERVAL:
                print(f"  … 已等待 {int(waited)}s（状态 {status}）")

        print("✓ 执行完成，开始取数")
        return snippet, notebook_name

    def fetch(self, snippet, notebook_name):
        cols, rows, start_over = None, [], True
        while True:
            data = self._payload(snippet, notebook_name)
            data["rows"] = FETCH_ROWS
            data["startOver"] = json.dumps(start_over)
            res = self._post("/notebook/api/fetch_result_data", data, timeout=300)
            if res.get("status") != 0:
                raise RuntimeError(f"取数失败：{res.get('message') or res}")
            result = res["result"]
            if cols is None:
                cols = [m["name"].split(".")[-1] for m in result.get("meta", [])]
            batch = result.get("data") or []
            rows.extend(batch)
            print(f"  … 已取 {len(rows)} 行")
            if not result.get("has_more") or not batch:
                break
            start_over = False
        return cols, rows

    def close(self, snippet, notebook_name):
        try:
            self._post("/notebook/api/close_statement", self._payload(snippet, notebook_name), timeout=30)
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser(description="把 SQL 文件丢给 Hue 执行并导出 CSV")
    ap.add_argument("sql_file", help="要执行的 .sql 文件")
    ap.add_argument("-o", "--out", default="hue_result.csv", help="输出 CSV 路径")
    ap.add_argument("--url", default=os.environ.get("HUE_URL", DEFAULT_URL))
    ap.add_argument("--engine", default="impala", choices=["impala", "hive"])
    ap.add_argument("--database", default="drt")
    ap.add_argument("--dry-run", action="store_true", help="只打印 SQL，不连服务器")
    args = ap.parse_args()

    sql = extract_statement(args.sql_file)
    if args.dry_run:
        print(sql)
        return

    user = os.environ.get("HUE_USER")
    password = os.environ.get("HUE_PASSWORD")
    if not user or not password:
        sys.exit("请先设置环境变量 HUE_USER / HUE_PASSWORD（别把密码写进文件提交上去）")

    hue = Hue(args.url, user, password, args.engine, args.database)
    hue.login()
    name = os.path.basename(args.sql_file)
    snippet, nb = hue.run(sql, name)
    try:
        cols, rows = hue.fetch(snippet, nb)
    finally:
        hue.close(snippet, nb)

    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print(f"✓ 共 {len(rows)} 行，已写入 {args.out}")


if __name__ == "__main__":
    main()
