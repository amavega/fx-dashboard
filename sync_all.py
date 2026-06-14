"""
GitHub Actions 汇率同步脚本 — 纯标准库，零依赖
================================================
功能：
  1. 从外管局 (SAFE) 同步 USD/CNY 中间价
  2. 从中信银行同步 USD/CNY 结汇价 OHLC
  3. 写入企业微信智能表格 Webhook
  4. 保存本地 JSON（data/ 目录）
  5. 生成 HTML 图表到 docs/（GitHub Pages）

用法：
  python sync_all.py              # 增量同步（默认）
  python sync_all.py --full       # 全量重新采集（2025年至今）
  python sync_all.py --chart-only # 仅重新生成图表

环境变量（可选，覆盖默认值）：
  SAFE_WEBHOOK    → 外管局智能表格 Webhook URL
  CITIC_WEBHOOK   → 中信银行智能表格 Webhook URL
"""

import json
import os
import re
import sys
import time
import hashlib
import hmac
import calendar
from datetime import datetime, timedelta, date as date_type
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib import request as urllib_request
from urllib.error import URLError, HTTPError
from pathlib import Path

# ============================================================
# 路径常量
# ============================================================
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DOCS_DIR = BASE_DIR / "docs"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)

SAFE_DATA_FILE = DATA_DIR / "fx_data.json"
CITIC_DATA_FILE = DATA_DIR / "citic_fx_data.json"
SAFE_CHART_FILE = DOCS_DIR / "fx_chart.html"
CITIC_CHART_FILE = DOCS_DIR / "citic_fx_chart.html"

# ============================================================
# 配置
# ============================================================
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# --- SAFE 外管局 ---
SAFE_SOURCE_URL = "https://www.safe.gov.cn/AppStructured/hlw/RMBQuery.do"
SAFE_VERIFY_URL = "https://www.safe.gov.cn/safe/rmbhlzjj/index.html"
SAFE_FIELD_DATE = "fhYWZY"
SAFE_FIELD_RATE = "fJL49j"
SAFE_WEBHOOK = os.environ.get(
    "SAFE_WEBHOOK",
    "https://qyapi.weixin.qq.com/cgi-bin/wedoc/smartsheet/webhook"
    "?key=6sqnAG2N7o0Q5MY4bvhTH6Wa1VzpKU6zdhxiJUWHeUGFtItDcwBK4z9isIogk72XmjVkJsBBQ55Du2ID99ahp34QZEeVSIz3KJkAEIpsQe8r",
)

# --- CITIC 中信银行 ---
CITIC_API = "https://etrade.citicbank.com/portalweb/cms/getForeignExchHis.htm"
CITIC_VERIFY_URL = "https://www.citicbank.com/common/financialnews/news/202408/t20240829_3540192.html"
CITIC_BANK_NUM = "001"
CITIC_CUR_NAME = "USDCNY"
CITIC_PAGE_SIZE = 10
CITIC_MAX_WORKERS = 8
CITIC_API_MAX_SPAN = 180
CITIC_FIELD_DATE = "f04Gwj"
CITIC_FIELD_OPEN = "fcHWoQ"
CITIC_FIELD_CLOSE = "fUtTQV"
CITIC_FIELD_HIGH = "fWakzs"
CITIC_FIELD_LOW = "fNIMnH"
CITIC_WEBHOOK = os.environ.get(
    "CITIC_WEBHOOK",
    "https://qyapi.weixin.qq.com/cgi-bin/wedoc/smartsheet/webhook"
    "?key=4ap0MTeSUEiz3uO1AhptZLovq3w41807WDaUuek5ngsBhG5oRAXpkDQ54wdT1CdK"
    "GLYbbW4qUs5Wr5ufK6eJrpSA0lCh7zx2OoqDEzJDrqHU",
)

DATA_START_YEAR = 2025

# ============================================================
# 日志
# ============================================================
LOG_LINES = []


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = "[%s] %s" % (ts, msg)
    print(line)
    LOG_LINES.append(line)


# ============================================================
# HTTP 工具函数
# ============================================================

def http_get(url, params=None, headers=None, timeout=30):
    if params:
        from urllib.parse import urlencode
        url = url + ("&" if "?" in url else "?") + urlencode(params)
    hdrs = dict(HTTP_HEADERS)
    if headers:
        hdrs.update(headers)
    req = urllib_request.Request(url, headers=hdrs)
    try:
        with urllib_request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        log("  HTTP GET 失败 [%s]: %s" % (url[:100], e))
        raise


def http_post_json(url, json_data, timeout=30):
    data = json.dumps(json_data).encode("utf-8")
    req = urllib_request.Request(url, data=data, headers={
        "Content-Type": "application/json; charset=utf-8",
        **HTTP_HEADERS,
    })
    try:
        with urllib_request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log("  HTTP POST 失败 [%s]: %s" % (url[:80], e))
        raise


# ============================================================
# 数据存取
# ============================================================

def load_json(path, default=None):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default if default is not None else {"records": [], "last_updated": None, "total": 0}


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ============================================================
# 外管局 SAFE — 中间价同步
# ============================================================

def safe_date_to_timestamp(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    utc_dt = dt - timedelta(hours=8)
    return str(int(calendar.timegm(utc_dt.timetuple()) * 1000))


def safe_fetch_data(start_date, end_date):
    params = {"startDate": start_date, "endDate": end_date, "method": "QueryFXMiddleRate"}
    headers = {"Referer": "https://www.safe.gov.cn/"}
    try:
        text = http_get(SAFE_SOURCE_URL, params=params, headers=headers, timeout=30)
        pattern = r'<td[^>]*>\s*(\d{4}-\d{2}-\d{2})\s*</td>\s*<td[^>]*>\s*([\d.]+)\s*</td>'
        result = {}
        for row_date, value_str in re.findall(pattern, text):
            result[row_date] = round(float(value_str) / 100, 4)
        return result
    except Exception as e:
        log("  SAFE 请求错误: %s" % e)
        return {}


def safe_webhook_post(date_str, rate):
    payload = {"add_records": [{"values": {
        SAFE_FIELD_DATE: safe_date_to_timestamp(date_str),
        SAFE_FIELD_RATE: rate,
    }}]}
    try:
        resp = http_post_json(SAFE_WEBHOOK, payload, timeout=15)
        return resp.get("errcode") == 0
    except Exception as e:
        log("  SAFE webhook 错误: %s" % e)
        return False


def sync_safe(full=False):
    log("=" * 56)
    log("[SAFE] 美元/人民币中间价 — 同步")
    log("=" * 56)

    data = load_json(SAFE_DATA_FILE)
    records = data.get("records", [])
    existing = {r["date"] for r in records}
    log("  本地已有 %d 条记录" % len(existing))

    if full or not records:
        start = date_type(DATA_START_YEAR, 1, 1)
    else:
        start = datetime.strptime(records[-1]["date"], "%Y-%m-%d").date() + timedelta(days=1)

    end = date_type.today()
    if start > end:
        log("  数据已是最新")

        # 修复：即使不需要同步也生成图表 & 返回记录
        if records:
            generate_safe_html(records)
        return records

    gap_dates = []
    d = start
    while d <= end:
        gap_dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    log("  缺口: %s ~ %s (%d 天)" % (gap_dates[0], gap_dates[-1], len(gap_dates)))
    safe_data = safe_fetch_data(gap_dates[0], gap_dates[-1])

    if not safe_data:
        log("  SAFE 无数据返回（非交易日）")
        if records:
            generate_safe_html(records)
        return records

    new_records = []
    for date_str, rate in sorted(safe_data.items()):
        if date_str not in existing:
            new_records.append({"date": date_str, "rate": rate})
            existing.add(date_str)

    if not new_records:
        log("  无新数据")
        if records:
            generate_safe_html(records)
        return records

    log("  发现 %d 天新数据" % len(new_records))
    webhook_ok = 0
    for rec in new_records:
        log("    写入: %s = %.4f" % (rec["date"], rec["rate"]))
        if safe_webhook_post(rec["date"], rec["rate"]):
            webhook_ok += 1
        time.sleep(0.3)

    records.extend(new_records)
    records.sort(key=lambda r: r["date"])
    data["records"] = records
    data["total"] = len(records)
    data["last_updated"] = records[-1]["date"]
    save_json(SAFE_DATA_FILE, data)
    generate_safe_html(records)
    log("  Webhook: %d/%d 成功, 总记录: %d" % (webhook_ok, len(new_records), len(records)))
    return records


# ============================================================
# 中信银行 CITIC — OHLC 同步
# ============================================================

def citic_fetch_page(begin_date, end_date, page_num, retries=3):
    from urllib.parse import urlencode
    params = {
        "bankNum": CITIC_BANK_NUM, "curName": CITIC_CUR_NAME,
        "beginDate": begin_date, "endDate": end_date,
        "pageNum": page_num, "pageSize": CITIC_PAGE_SIZE,
    }
    url = CITIC_API + "?" + urlencode(params)
    for attempt in range(retries):
        try:
            text = http_get(url, headers={"Referer": "https://www.citicbank.com/"}, timeout=30)
            data = json.loads(text)
            if data.get("retCode") == "AAAAAAA":
                return data
        except Exception:
            if attempt < retries - 1:
                time.sleep(0.5 * (attempt + 1))
    return None


def citic_fetch_range(begin_date, end_date):
    first_page = citic_fetch_page(begin_date, end_date, 1)
    if not first_page:
        return []
    total = first_page.get("totalCount", 0)
    if total == 0:
        return []
    page_count = first_page.get("pageCount", 0)
    all_records = list(first_page["content"]["resultList"])
    if page_count > 1:
        with ThreadPoolExecutor(max_workers=CITIC_MAX_WORKERS) as ex:
            futures = {ex.submit(citic_fetch_page, begin_date, end_date, p): p
                       for p in range(2, page_count + 1)}
            for future in as_completed(futures):
                d = future.result()
                if d and d.get("content"):
                    all_records.extend(d["content"]["resultList"])
    all_records.sort(key=lambda r: r["issueTime"])
    return all_records


def citic_compute_ohlc(records):
    day_groups = {}
    for r in records:
        try:
            parts = r["issueTime"].split()
            date_str = parts[0].replace(".", "-")
            time_str = parts[1] if len(parts) > 1 else "00:00:00"
            price = float(r["cstSellPrice"]) / 100.0
        except (ValueError, KeyError, IndexError):
            continue
        day_groups.setdefault(date_str, []).append({"time": time_str, "price": price})

    ohlc_list = []
    for ds in sorted(day_groups.keys()):
        entries = day_groups[ds]
        prices = [e["price"] for e in entries]
        ohlc_list.append({
            "date": ds,
            "open": round(entries[0]["price"], 4),
            "close": round(entries[-1]["price"], 4),
            "high": round(max(prices), 4),
            "low": round(min(prices), 4),
            "count": len(entries),
        })
    return ohlc_list


def citic_collect_ohlc(begin_date, end_date):
    begin_dt = datetime.strptime(begin_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end_dt - begin_dt).days + 1
    if total_days <= 0:
        return []

    if total_days <= CITIC_API_MAX_SPAN:
        raw = citic_fetch_range(begin_date, end_date)
        return citic_compute_ohlc(raw) if raw else []

    all_ohlc = []
    chunk_start = begin_dt
    while chunk_start <= end_dt:
        chunk_end = min(chunk_start + timedelta(days=CITIC_API_MAX_SPAN - 1), end_dt)
        cs, ce = chunk_start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        log("  CITIC chunk: %s ~ %s" % (cs, ce))
        raw = citic_fetch_range(cs, ce)
        if raw:
            all_ohlc.extend(citic_compute_ohlc(raw))
        chunk_start = chunk_end + timedelta(days=1)

    seen = set()
    result = []
    for rec in sorted(all_ohlc, key=lambda r: r["date"]):
        if rec["date"] not in seen:
            seen.add(rec["date"])
            result.append(rec)
    return result


def citic_webhook_post(ohlc_list):
    if not ohlc_list:
        return 0
    payload_records = []
    for rec in ohlc_list:
        payload_records.append({"values": {
            CITIC_FIELD_DATE: rec["date"],
            CITIC_FIELD_OPEN: rec["open"],
            CITIC_FIELD_CLOSE: rec["close"],
            CITIC_FIELD_HIGH: rec["high"],
            CITIC_FIELD_LOW: rec["low"],
        }})
    try:
        resp = http_post_json(CITIC_WEBHOOK, {"add_records": payload_records}, timeout=30)
        if resp.get("errcode") == 0:
            return len(resp.get("add_records", []))
        else:
            log("  CITIC webhook 错误: %s" % resp.get("errmsg"))
            return 0
    except Exception as e:
        log("  CITIC webhook 异常: %s" % e)
        return 0


def sync_citic(full=False):
    log("=" * 56)
    log("[CITIC] 中信银行 USD/CNY 结汇价 OHLC — 同步")
    log("=" * 56)

    data = load_json(CITIC_DATA_FILE, {
        "source": "中信银行官方牌价",
        "source_url": CITIC_VERIFY_URL,
        "api_url": CITIC_API,
        "currency": "USD/CNY",
        "rate_type": "客户结汇价(cstSellPrice)",
        "unit": "1美元兑人民币",
        "records": [],
        "total": 0,
        "last_updated": None,
    })
    records = data.get("records", [])
    existing = {r["date"]: r for r in records}
    log("  本地已有 %d 条记录" % len(existing))

    if full or not records:
        gap_begin = "%d-01-01" % DATA_START_YEAR
    else:
        last_date = datetime.strptime(records[-1]["date"], "%Y-%m-%d")
        if last_date.date() >= date_type.today():
            log("  数据已是最新")

            # 修复：即使不需要同步也生成图表 & 返回记录
            if records:
                generate_citic_html(records)
            return records
        gap_begin = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")

    gap_end = datetime.now().strftime("%Y-%m-%d")
    log("  缺口: %s ~ %s" % (gap_begin, gap_end))
    new_ohlc = citic_collect_ohlc(gap_begin, gap_end)

    if not new_ohlc:
        log("  缺口内无交易日数据")

        # 修复：即使无新数据也生成图表 & 返回记录
        if records:
            generate_citic_html(records)
        return records

    log("  API 返回 %d 天 OHLC" % len(new_ohlc))
    truly_new = [r for r in new_ohlc if r["date"] not in existing]

    if truly_new:
        for r in truly_new[:5]:
            log("    %s O=%.4f H=%.4f L=%.4f C=%.4f (n=%d)" % (
                r["date"], r["open"], r["high"], r["low"], r["close"], r["count"]))
        if len(truly_new) > 5:
            log("    ... 还有 %d 天" % (len(truly_new) - 5))

        batch_size = 200
        total_written = 0
        for i in range(0, len(truly_new), batch_size):
            batch = truly_new[i:i + batch_size]
            n = citic_webhook_post(batch)
            total_written += n
        log("  Webhook: %d/%d 写入成功" % (total_written, len(truly_new)))

        for rec in truly_new:
            existing[rec["date"]] = rec
        all_records = sorted(existing.values(), key=lambda r: r["date"])
    else:
        all_records = records
        log("  无新记录")

    data["records"] = all_records
    data["total"] = len(all_records)
    data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_json(CITIC_DATA_FILE, data)
    generate_citic_html(all_records)
    log("  总记录: %d 条, 新增: %d 天" % (len(all_records), len(truly_new)))
    return all_records


# ============================================================
# HTML 图表生成
# ============================================================

def generate_safe_html(records):
    """生成外管局中间价 HTML 图表（嵌入到 docs/fx_chart.html）"""
    if not records:
        log("  SAFE 无记录，跳过图表生成")
        return

    dates = [r["date"] for r in records]
    rates = [r["rate"] for r in records]
    max_idx = len(dates) - 1

    html = _build_safe_chart_html(dates, rates)
    with open(SAFE_CHART_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    log("  SAFE 图表已生成: %s (%d 条)" % (SAFE_CHART_FILE, len(records)))


def generate_citic_html(records):
    """生成中信银行 OHLC HTML 图表（嵌入到 docs/citic_fx_chart.html）"""
    if not records:
        log("  CITIC 无记录，跳过图表生成")
        return

    dates = [r["date"] for r in records]
    opens = [r["open"] for r in records]
    closes = [r["close"] for r in records]
    highs = [r["high"] for r in records]
    lows = [r["low"] for r in records]
    counts = [r["count"] for r in records]

    html = _build_citic_chart_html(dates, opens, highs, lows, closes, counts)
    with open(CITIC_CHART_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    log("  CITIC 图表已生成: %s (%d 条)" % (CITIC_CHART_FILE, len(records)))


# ── SAFE 中间价 HTML 模板 ──

def _build_safe_chart_html(dates, rates):
    dates_json = json.dumps(dates, ensure_ascii=False)
    rates_json = json.dumps(rates, ensure_ascii=False)
    max_idx = len(dates) - 1

    return '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>美元/人民币中间价走势图</title>
<script src="chart.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;background:#f0f2f5;padding:24px}
.container{max-width:1100px;margin:0 auto;background:#fff;border-radius:16px;box-shadow:0 2px 12px rgba(0,0,0,.08);padding:32px}
h1{font-size:22px;color:#1a1a1a;margin-bottom:4px}
.subtitle{font-size:13px;color:#888;margin-bottom:24px}
.stats{display:flex;gap:16px;margin-bottom:24px;flex-wrap:wrap}
.stat-card{flex:1;min-width:160px;background:#f7f8fa;border-radius:12px;padding:16px 20px}
.stat-card .label{font-size:12px;color:#888;margin-bottom:6px}
.stat-card .value{font-size:24px;font-weight:700;color:#1a1a1a}
.stat-card .value.up{color:#e85d5d}
.stat-card .value.down{color:#2ecc71}
.stat-card .delta{font-size:12px;margin-top:4px;color:#aaa}
.slider-section{margin-bottom:16px;padding:16px 20px;background:#f7f8fa;border-radius:12px}
.slider-labels{display:flex;justify-content:space-between;margin-bottom:10px}
.slider-labels span{font-size:13px;font-weight:600;color:#1764d9}
.slider-labels .range-label{font-size:12px;font-weight:400;color:#888}
.range-slider{position:relative;height:40px;display:flex;align-items:center}
.range-slider input[type="range"]{position:absolute;width:100%;height:6px;-webkit-appearance:none;appearance:none;background:none;pointer-events:none;margin:0;padding:0}
.range-slider input[type="range"]::-webkit-slider-runnable-track{height:6px;background:none;border:none}
.range-slider input[type="range"]::-webkit-slider-thumb{-webkit-appearance:none;appearance:none;width:22px;height:22px;border-radius:50%;background:#1764d9;border:3px solid #fff;box-shadow:0 2px 6px rgba(0,0,0,.2);cursor:pointer;pointer-events:all;margin-top:-8px;transition:transform .1s}
.range-slider input[type="range"]::-webkit-slider-thumb:hover{transform:scale(1.15)}
.range-slider input[type="range"]::-webkit-slider-thumb:active{transform:scale(1.25)}
.range-slider .track-bg{position:absolute;left:0;right:0;height:6px;background:#e0e0e0;border-radius:3px;z-index:0}
.range-slider .track-active{position:absolute;height:6px;background:#1764d9;border-radius:3px;z-index:1;transition:left .1s,width .1s}
.date-query{margin-bottom:16px;padding:16px 20px;background:linear-gradient(135deg,#f0f4ff 0%,#f7f8fa 100%);border-radius:12px;border:1px solid #dce6fb;display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.date-query label{font-size:13px;font-weight:600;color:#333;white-space:nowrap}
.date-query input[type="date"]{padding:8px 14px;border:1px solid #d0d5dd;border-radius:8px;font-size:14px;color:#333;background:#fff;outline:none;transition:border-color .2s}
.date-query input[type="date"]:focus{border-color:#1764d9;box-shadow:0 0 0 3px rgba(23,100,217,.1)}
.date-query .btn-query{padding:8px 20px;border:none;border-radius:8px;background:#1764d9;color:#fff;font-size:13px;font-weight:600;cursor:pointer;transition:background .2s}
.date-query .btn-query:hover{background:#1253b3}
.date-query .query-result{font-size:14px;margin-left:8px;display:flex;align-items:center;gap:10px}
.date-query .query-result .rate-val{font-size:22px;font-weight:700;color:#1764d9}
.date-query .query-result .no-data{font-size:13px;color:#999}
.date-query .verify-link{font-size:12px;color:#1764d9;text-decoration:none;border-bottom:1px dashed #1764d9;white-space:nowrap}
.date-query .verify-link:hover{border-bottom-style:solid}
.controls{display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap}
.controls button{padding:6px 16px;border:1px solid #ddd;border-radius:20px;background:#fff;cursor:pointer;font-size:13px;color:#555;transition:all .2s}
.controls button.active,.controls button:hover{background:#1764d9;color:#fff;border-color:#1764d9}
.chart-wrap{position:relative;height:420px}
.footer{margin-top:18px;font-size:12px;color:#bbb;text-align:right}
.back-link{display:inline-block;margin-bottom:12px;font-size:13px;color:#1764d9;text-decoration:none}
.back-link:hover{text-decoration:underline}
</style>
</head>
<body>
<div class="container">
<a class="back-link" href="index.html">← 返回首页</a>
<h1>美元 / 人民币中间价走势</h1>
<div class="subtitle" id="subtitle"></div>

<div class="stats">
<div class="stat-card"><div class="label">最新汇率</div><div class="value" id="stat-latest">—</div><div class="delta" id="stat-delta">—</div></div>
<div class="stat-card"><div class="label">期间最高</div><div class="value up" id="stat-max">—</div><div class="delta" id="stat-max-date">—</div></div>
<div class="stat-card"><div class="label">期间最低</div><div class="value down" id="stat-min">—</div><div class="delta" id="stat-min-date">—</div></div>
<div class="stat-card"><div class="label">期间波动</div><div class="value" id="stat-range">—</div><div class="delta">高点 - 低点</div></div>
</div>

<div class="slider-section">
  <div class="slider-labels">
    <span id="slider-start-label"></span>
    <span class="range-label">拖拽两端选择时间区间</span>
    <span id="slider-end-label"></span>
  </div>
  <div class="range-slider" id="range-slider">
    <div class="track-bg"></div>
    <div class="track-active" id="track-active"></div>
    <input type="range" id="slider-start" min="0" max="''' + str(max_idx) + '''" value="0" step="1">
    <input type="range" id="slider-end" min="0" max="''' + str(max_idx) + '''" value="''' + str(max_idx) + '''" step="1">
  </div>
</div>

<div class="date-query">
  <label for="date-picker">查询某一日中间价：</label>
  <input type="date" id="date-picker">
  <button class="btn-query" onclick="queryDate()">查询</button>
  <span class="query-result" id="query-result"></span>
</div>

<div class="controls" id="controls"></div>
<div class="chart-wrap"><canvas id="chart"></canvas></div>
<div class="footer">
  数据来源：国家外汇管理局（SAFE） | 更新至 <span id="update-date"></span>
</div>
</div>

<script>
const allDates = ''' + dates_json + ''';
const allRates = ''' + rates_json + ''';

function computeStats(dates, rates) {
  const maxRate = Math.max(...rates);
  const minRate = Math.min(...rates);
  const maxIdx = rates.indexOf(maxRate);
  const minIdx = rates.indexOf(minRate);
  const latest = rates[rates.length - 1];
  const prev = rates.length >= 2 ? rates[rates.length - 2] : latest;
  const diff = latest - prev;
  return { maxRate, minRate, maxIdx, minIdx, latest, prev, diff };
}

function updateStats(dates, rates) {
  if (rates.length === 0) return;
  const s = computeStats(dates, rates);
  document.getElementById("stat-latest").textContent = s.latest.toFixed(4);
  const d = s.diff;
  document.getElementById("stat-delta").textContent = "较前日 " + (d >= 0 ? "+" : "") + d.toFixed(4);
  document.getElementById("stat-delta").style.color = d >= 0 ? "#e85d5d" : "#2ecc71";
  document.getElementById("stat-max").textContent = s.maxRate.toFixed(4);
  document.getElementById("stat-max-date").textContent = dates[s.maxIdx];
  document.getElementById("stat-min").textContent = s.minRate.toFixed(4);
  document.getElementById("stat-min-date").textContent = dates[s.minIdx];
  document.getElementById("stat-range").textContent = Math.abs(s.maxRate - s.minRate).toFixed(4);
  document.getElementById("subtitle").textContent = dates[0] + " ~ " + dates[dates.length - 1] + " | 共 " + dates.length + " 个交易日";
}

updateStats(allDates, allRates);
document.getElementById("update-date").textContent = allDates[allDates.length - 1];

const ctx = document.getElementById("chart").getContext("2d");
let chart = new Chart(ctx, {
  type: "line",
  data: {
    labels: allDates,
    datasets: [{
      data: allRates,
      borderColor: "#1764d9",
      backgroundColor: "rgba(23,100,217,0.08)",
      fill: true,
      tension: 0.3,
      pointRadius: 0,
      pointHoverRadius: 5,
      pointHoverBackgroundColor: "#1764d9",
      borderWidth: 2,
    }]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: "rgba(0,0,0,0.72)",
        padding: 12,
        callbacks: {
          title: function(items) { return "日期: " + items[0].label; },
          label: function(item) { return "汇率: " + item.raw.toFixed(4); },
        }
      },
    },
    scales: {
      x: { ticks: { maxTicksLimit: 15, font: { size: 11 }, color: "#aaa" }, grid: { display: false } },
      y: { position: "right", ticks: { font: { size: 11 }, color: "#aaa", callback: function(v) { return v.toFixed(4); } }, grid: { color: "rgba(0,0,0,0.05)" } }
    }
  }
});

function applyFilter(startIdx, endIdx) {
  const filteredDates = allDates.slice(startIdx, endIdx + 1);
  const filteredRates = allRates.slice(startIdx, endIdx + 1);
  chart.data.labels = filteredDates;
  chart.data.datasets[0].data = filteredRates;
  chart.update();
  updateStats(filteredDates, filteredRates);
}

const sliderStart = document.getElementById("slider-start");
const sliderEnd = document.getElementById("slider-end");
const trackActive = document.getElementById("track-active");
const startLabel = document.getElementById("slider-start-label");
const endLabel = document.getElementById("slider-end-label");
const maxIdx = allDates.length - 1;

function updateSliderUI() {
  let s = parseInt(sliderStart.value);
  let e = parseInt(sliderEnd.value);
  if (s > e) {
    if (sliderStart.dataset.moving === "true") { s = e; sliderStart.value = s; }
    else { e = s; sliderEnd.value = e; }
  }
  const pctL = (s / maxIdx) * 100;
  const pctR = (e / maxIdx) * 100;
  trackActive.style.left = pctL + "%";
  trackActive.style.width = (pctR - pctL) + "%";
  startLabel.textContent = allDates[s];
  endLabel.textContent = allDates[e];
}

sliderStart.addEventListener("input", function() {
  sliderStart.dataset.moving = "true";
  updateSliderUI();
  applyFilter(parseInt(sliderStart.value), parseInt(sliderEnd.value));
  sliderStart.dataset.moving = "";
  resetPresetButtons();
});

sliderEnd.addEventListener("input", function() {
  updateSliderUI();
  applyFilter(parseInt(sliderStart.value), parseInt(sliderEnd.value));
  resetPresetButtons();
});

function setSliderRange(startIdx, endIdx) {
  sliderStart.value = startIdx;
  sliderEnd.value = endIdx;
  updateSliderUI();
}

updateSliderUI();

var ranges = [
  { label: "全部", handler: function() { setSliderRange(0, maxIdx); applyFilter(0, maxIdx); } },
  { label: "近30天", handler: function() { var i = Math.max(0, maxIdx - 30); setSliderRange(i, maxIdx); applyFilter(i, maxIdx); } },
  { label: "近90天", handler: function() { var i = Math.max(0, maxIdx - 90); setSliderRange(i, maxIdx); applyFilter(i, maxIdx); } },
  { label: "近180天", handler: function() { var i = Math.max(0, maxIdx - 180); setSliderRange(i, maxIdx); applyFilter(i, maxIdx); } },
  { label: "2026年", handler: function() {
    var s = null, e = null;
    for (var i = 0; i < allDates.length; i++) { if (allDates[i].startsWith("2026")) { if (s === null) s = i; e = i; } }
    if (s !== null) { setSliderRange(s, e); applyFilter(s, e); }
  }},
  { label: "2025年", handler: function() {
    var s = null, e = null;
    for (var i = 0; i < allDates.length; i++) { if (allDates[i].startsWith("2025")) { if (s === null) s = i; e = i; } }
    if (s !== null) { setSliderRange(s, e); applyFilter(s, e); }
  }},
];

var controls = document.getElementById("controls");
ranges.forEach(function(r, i) {
  var btn = document.createElement("button");
  btn.textContent = r.label;
  if (i === 0) btn.classList.add("active");
  btn.onclick = function() { setActiveButton(btn); r.handler(); };
  controls.appendChild(btn);
});

function setActiveButton(btn) {
  controls.querySelectorAll("button").forEach(function(b) { b.classList.remove("active"); });
  btn.classList.add("active");
}

function resetPresetButtons() {
  controls.querySelectorAll("button").forEach(function(b) { b.classList.remove("active"); });
}

const datePicker = document.getElementById("date-picker");
const dateRateMap = {};
for (var i = 0; i < allDates.length; i++) { dateRateMap[allDates[i]] = { rate: allRates[i], idx: i }; }
datePicker.setAttribute("min", allDates[0]);
datePicker.setAttribute("max", allDates[allDates.length - 1]);
datePicker.value = allDates[allDates.length - 1];

function queryDate() {
  var date = datePicker.value;
  var resultEl = document.getElementById("query-result");
  var entry = dateRateMap[date];
  if (entry) {
    var prevEntry = dateRateMap[allDates[entry.idx - 1]];
    var delta = prevEntry ? entry.rate - prevEntry.rate : 0;
    var deltaStr = "";
    if (delta !== 0 && prevEntry) {
      var sign = delta >= 0 ? "+" : "";
      var color = delta >= 0 ? "#e85d5d" : "#2ecc71";
      deltaStr = ' <span style="font-size:12px;color:' + color + '">(' + sign + delta.toFixed(4) + ')</span>';
    }
    resultEl.innerHTML = '<span class="rate-val">' + entry.rate.toFixed(4) + '</span>' + deltaStr +
      ' <a class="verify-link" href="''' + SAFE_VERIFY_URL + '''" target="_blank" rel="noopener">&#x1f517; 外管局核实</a>';
    highlightDate(entry.idx);
  } else {
    resultEl.innerHTML = '<span class="no-data">该日期无中间价数据</span>' +
      ' <a class="verify-link" href="''' + SAFE_VERIFY_URL + '''" target="_blank" rel="noopener">&#x1f517; 外管局查询</a>';
  }
}

function highlightDate(idx) {
  var bgColors = new Array(allDates.length).fill("transparent");
  bgColors[idx] = "#e85d5d";
  chart.data.datasets[0].pointRadius = new Array(allDates.length).fill(0);
  chart.data.datasets[0].pointRadius[idx] = 6;
  chart.data.datasets[0].pointBackgroundColor = bgColors;
  chart.data.datasets[0].pointBorderColor = bgColors;
  chart.update();
  clearTimeout(window._highlightTimer);
  window._highlightTimer = setTimeout(function() {
    chart.data.datasets[0].pointRadius = 0;
    chart.data.datasets[0].pointBackgroundColor = "#1764d9";
    chart.data.datasets[0].pointBorderColor = "#1764d9";
    chart.update();
  }, 3000);
}

datePicker.addEventListener("keydown", function(e) { if (e.key === "Enter") queryDate(); });
</script>
</body>
</html>'''


# ── CITIC OHLC HTML 模板 ──

def _build_citic_chart_html(dates, opens, highs, lows, closes, counts):
    dates_json = json.dumps(dates, ensure_ascii=False)
    opens_json = json.dumps(opens)
    highs_json = json.dumps(highs)
    lows_json = json.dumps(lows)
    closes_json = json.dumps(closes)
    counts_json = json.dumps(counts)

    return '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>中信银行 USD/CNY 结汇价 OHLC 走势</title>
<script src="chart.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Microsoft YaHei",sans-serif;background:#f0f2f5;color:#333;padding:10px;min-height:100vh}
.container{max-width:1100px;margin:0 auto;background:#fff;border-radius:14px;padding:20px 22px;box-shadow:0 2px 12px rgba(0,0,0,.06)}
h1{font-size:20px;text-align:center;margin-bottom:4px;color:#1a1a2e}
.subtitle{text-align:center;font-size:12px;color:#888;margin-bottom:14px}
.back-link{display:inline-block;margin-bottom:12px;font-size:13px;color:#1764d9;text-decoration:none}
.back-link:hover{text-decoration:underline}
.stats{display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap;justify-content:center}
.stat-card{flex:1;min-width:100px;max-width:180px;padding:12px 14px;border-radius:10px;text-align:center;background:linear-gradient(135deg,#f8f9fc,#eef0f6);border:1px solid #e0e3ea}
.stat-card .label{font-size:11px;color:#888;margin-bottom:4px}
.stat-card .value{font-size:20px;font-weight:700;color:#1764d9}
.stat-card.down .value{color:#2ecc71}
.stat-card.up .value{color:#e85d5d}
.date-query{margin-bottom:12px;padding:14px 18px;background:linear-gradient(135deg,#f0f4ff 0%,#f7f8fa 100%);border-radius:10px;border:1px solid #dce6fb;display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.date-query label{font-size:13px;font-weight:600;color:#333;white-space:nowrap}
.date-query input[type="date"]{padding:7px 12px;border:1px solid #d0d5dd;border-radius:8px;font-size:13px;color:#333;background:#fff;outline:none}
.date-query input[type="date"]:focus{border-color:#1764d9;box-shadow:0 0 0 3px rgba(23,100,217,.1)}
.date-query .btn-query{padding:7px 18px;border:none;border-radius:8px;background:#e8313e;color:#fff;font-size:13px;font-weight:600;cursor:pointer;transition:background .2s}
.date-query .btn-query:hover{background:#d3090f}
.date-query .query-result{font-size:13px;margin-left:6px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.date-query .query-result .rate-val{font-size:20px;font-weight:700;color:#e8313e}
.date-query .query-result .no-data{font-size:12px;color:#999}
.date-query .verify-link{font-size:11px;color:#e8313e;text-decoration:none;border-bottom:1px dashed #e8313e;white-space:nowrap}
.ohlc-mini{display:flex;gap:6px;flex-wrap:wrap}
.ohlc-mini span{padding:2px 8px;border-radius:4px;font-size:12px;background:#f0f0f0;white-space:nowrap}
.ohlc-mini .o{color:#2196F3}.ohlc-mini .h{color:#e85d5d}.ohlc-mini .l{color:#2ecc71}.ohlc-mini .c{color:#FF9800}
.controls{display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap;justify-content:center}
.controls button{padding:6px 16px;border:1px solid #d0d5dd;border-radius:18px;background:#fff;color:#555;font-size:12px;cursor:pointer;transition:all .2s}
.controls button:hover{border-color:#e8313e;color:#e8313e}
.controls button.active{background:#e8313e;color:#fff;border-color:#e8313e}
.slider-wrap{margin:0 10px 18px 10px;position:relative;user-select:none;-webkit-user-select:none}
.slider-labels{display:flex;justify-content:space-between;margin-bottom:6px;font-size:11px;color:#888}
.slider-labels .range-indicator{font-weight:600;color:#e8313e}
.slider-container{position:relative;height:36px}
.slider-container input[type="range"]{position:absolute;width:100%;height:6px;top:15px;left:0;-webkit-appearance:none;appearance:none;background:transparent;pointer-events:none;z-index:2}
.slider-container input[type="range"]::-webkit-slider-thumb{-webkit-appearance:none;appearance:none;width:24px;height:24px;border-radius:50%;background:#e8313e;border:2px solid #fff;box-shadow:0 2px 6px rgba(0,0,0,.2);cursor:pointer;pointer-events:all;transition:transform .15s}
.slider-container input[type="range"]::-webkit-slider-thumb:hover{transform:scale(1.15)}
.slider-container input[type="range"]::-webkit-slider-thumb:active{transform:scale(1.25)}
.track-bg{position:absolute;left:0;right:0;top:15px;height:6px;background:#e0e3ea;border-radius:3px;z-index:1}
.track-active{position:absolute;top:15px;height:6px;background:#e8313e;border-radius:3px;z-index:1;transition:left .1s,right .1s}
.legend-toggles{display:flex;gap:12px;justify-content:center;margin-bottom:10px;flex-wrap:wrap}
.legend-toggles label{font-size:12px;cursor:pointer;display:flex;align-items:center;gap:4px;user-select:none}
.legend-toggles input{accent-color:#e8313e}
.chart-wrapper{position:relative;width:100%;height:420px;overflow:hidden}
@media(max-width:768px){.container{padding:12px 10px;border-radius:10px}h1{font-size:17px}.stat-card{min-width:70px;padding:8px 10px}.stat-card .value{font-size:16px}.controls button{padding:5px 12px;font-size:11px}.date-query{padding:10px 12px;gap:6px}}
</style>
</head>
<body>
<div class="container">
  <a class="back-link" href="index.html">← 返回首页</a>
  <h1>中信银行 美元结汇价 OHLC</h1>
  <div class="subtitle">数据来源：中信银行官方外汇历史牌价 &middot; 客户结汇价(cstSellPrice) &middot; 1 USD/CNY</div>
  <div class="stats" id="stats"></div>
  <div class="date-query">
    <label for="date-picker">查询某一日：</label>
    <input type="date" id="date-picker">
    <button class="btn-query" onclick="queryDate()">查询</button>
    <span class="query-result" id="query-result"></span>
  </div>
  <div class="legend-toggles">
    <label><input type="checkbox" checked onchange="toggleLine(0,this.checked)"> <span style="color:#2196F3;font-weight:600">开盘价</span></label>
    <label><input type="checkbox" checked onchange="toggleLine(1,this.checked)"> <span style="color:#e85d5d;font-weight:600">最高价</span></label>
    <label><input type="checkbox" checked onchange="toggleLine(2,this.checked)"> <span style="color:#2ecc71;font-weight:600">最低价</span></label>
    <label><input type="checkbox" checked onchange="toggleLine(3,this.checked)"> <span style="color:#FF9800;font-weight:600">收盘价</span></label>
  </div>
  <div class="controls" id="controls"></div>
  <div class="slider-wrap">
    <div class="slider-labels"><span id="slider-range-text">显示全部数据</span></div>
    <div class="slider-container" id="slider-container">
      <div class="track-bg"></div>
      <div class="track-active" id="track-active"></div>
      <input type="range" id="slider-start" min="0" max="''' + str(len(dates) - 1) + '''" value="0" step="1">
      <input type="range" id="slider-end" min="0" max="''' + str(len(dates) - 1) + '''" value="''' + str(len(dates) - 1) + '''" step="1">
    </div>
  </div>
  <div class="chart-wrapper"><canvas id="chart"></canvas></div>
</div>

<script>
const allDates = ''' + dates_json + ''';
const allOpens = ''' + opens_json + ''';
const allHighs = ''' + highs_json + ''';
const allLows = ''' + lows_json + ''';
const allCloses = ''' + closes_json + ''';
const allCounts = ''' + counts_json + ''';

const dateIndexMap = {};
for (let i = 0; i < allDates.length; i++) { dateIndexMap[allDates[i]] = i; }

let chart = null;

function formatRate(v) { return v.toFixed(4); }

function getFilteredData(startIdx, endIdx) {
  const slice = (arr) => arr.slice(startIdx, endIdx + 1);
  return { dates: slice(allDates), opens: slice(allOpens), highs: slice(allHighs), lows: slice(allLows), closes: slice(allCloses), counts: slice(allCounts) };
}

function updateStats(startIdx, endIdx) {
  const closesSub = allCloses.slice(startIdx, endIdx + 1);
  const highsSub = allHighs.slice(startIdx, endIdx + 1);
  const lowsSub = allLows.slice(startIdx, endIdx + 1);
  const latest = closesSub[closesSub.length - 1];
  const prev = closesSub.length > 1 ? closesSub[closesSub.length - 2] : null;
  const delta = prev !== null ? latest - prev : 0;
  const max = Math.max(...highsSub);
  const min = Math.min(...lowsSub);
  const range = max - min;
  const avg = closesSub.reduce((a,b) => a+b, 0) / closesSub.length;
  const upClass = delta >= 0 ? 'up' : 'down';
  const sign = delta >= 0 ? '+' : '';
  document.getElementById('stats').innerHTML = `
    <div class="stat-card"><div class="label">最新收盘</div><div class="value">${{formatRate(latest)}}</div></div>
    <div class="stat-card ${{upClass}}"><div class="label">较前日</div><div class="value">${{sign}}${{delta.toFixed(4)}}</div></div>
    <div class="stat-card"><div class="label">区间最高</div><div class="value">${{formatRate(max)}}</div></div>
    <div class="stat-card"><div class="label">区间最低</div><div class="value">${{formatRate(min)}}</div></div>
    <div class="stat-card"><div class="label">波动幅度</div><div class="value">${{formatRate(range)}}</div></div>
    <div class="stat-card"><div class="label">日均值</div><div class="value">${{formatRate(avg)}}</div></div>`;
}

function updateChart(startIdx, endIdx) {
  const d = getFilteredData(startIdx, endIdx);
  if (chart) {
    chart.data.labels = d.dates;
    chart.data.datasets[0].data = d.opens;
    chart.data.datasets[1].data = d.highs;
    chart.data.datasets[2].data = d.lows;
    chart.data.datasets[3].data = d.closes;
    chart.update('none');
  }
  updateStats(startIdx, endIdx);
}

function initChart() {
  const d = getFilteredData(0, allDates.length - 1);
  const ctx = document.getElementById('chart').getContext('2d');
  chart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: d.dates,
      datasets: [
        { label: '开盘价', data: d.opens, borderColor: '#2196F3', backgroundColor: 'rgba(33,150,243,0.05)', borderWidth: 1.5, tension: 0.3, pointRadius: 0, pointHoverRadius: 4, borderDash: [4,2] },
        { label: '最高价', data: d.highs, borderColor: '#e85d5d', backgroundColor: 'transparent', borderWidth: 1.5, tension: 0.3, pointRadius: 0, pointHoverRadius: 4, borderDash: [2,2] },
        { label: '最低价', data: d.lows, borderColor: '#2ecc71', backgroundColor: 'transparent', borderWidth: 1.5, tension: 0.3, pointRadius: 0, pointHoverRadius: 4, borderDash: [2,2] },
        { label: '收盘价', data: d.closes, borderColor: '#FF9800', backgroundColor: 'rgba(255,152,0,0.03)', borderWidth: 2, tension: 0.3, pointRadius: 0, pointHoverRadius: 5 }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: true, aspectRatio: 2.5,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { title: function(items) { return '日期: ' + items[0].label; }, afterBody: function(items) { const idx = dateIndexMap[items[0].label]; if (idx !== undefined && allCounts[idx]) return '当日报价: ' + allCounts[idx] + ' 次'; return ''; } } }
      },
      scales: { x: { ticks: { maxTicksLimit: 15, maxRotation: 45, font: { size: 10 } } }, y: { ticks: { callback: function(v) { return v.toFixed(4); }, font: { size: 10 } } } }
    }
  });
}

function toggleLine(idx, visible) { if (chart) { chart.setDatasetVisibility(idx, visible); chart.update('none'); } }

const presets = [
  { label: '全部', days: 99999 }, { label: '近7天', days: 7 }, { label: '近30天', days: 30 }, { label: '近90天', days: 90 }, { label: '近180天', days: 180 },
  { label: '1月', month: 1 }, { label: '2月', month: 2 }, { label: '3月', month: 3 }, { label: '4月', month: 4 }, { label: '5月', month: 5 }, { label: '6月', month: 6 }
];

function applyPreset(preset) {
  const total = allDates.length;
  let startIdx, endIdx;
  if (preset.month) {
    const targetMonth = String(preset.month).padStart(2, '0');
    startIdx = allDates.findIndex(d => d.startsWith('2026-' + targetMonth));
    endIdx = allDates.findLastIndex(d => d.startsWith('2026-' + targetMonth));
    if (startIdx === -1) startIdx = 0;
    if (endIdx === -1) endIdx = total - 1;
  } else if (preset.days >= 99999) { startIdx = 0; endIdx = total - 1; }
  else { endIdx = total - 1; startIdx = Math.max(0, endIdx - preset.days + 1); }
  applyFilter(startIdx, endIdx);
  highlightPresetButton(preset);
}

function applyFilter(startIdx, endIdx) { updateChart(startIdx, endIdx); updateSliders(startIdx, endIdx); }

function highlightPresetButton(preset) {
  document.querySelectorAll('#controls button').forEach(b => b.classList.remove('active'));
  const key = preset.month ? ('month_' + preset.month) : ('days_' + (preset.days || 99999));
  const target = document.querySelector('#controls button[data-key="' + key + '"]');
  if (target) target.classList.add('active');
}

function resetPresetButtons() { document.querySelectorAll('#controls button').forEach(b => b.classList.remove('active')); }

const sliderStart = document.getElementById('slider-start');
const sliderEnd = document.getElementById('slider-end');
const trackActive = document.getElementById('track-active');
const sliderRangeText = document.getElementById('slider-range-text');
const totalCount = allDates.length;

function updateSliders(startIdx, endIdx) {
  sliderStart.value = startIdx; sliderEnd.value = endIdx;
  const leftPct = (startIdx / (totalCount - 1)) * 100;
  const rightPct = ((totalCount - 1 - endIdx) / (totalCount - 1)) * 100;
  trackActive.style.left = leftPct + '%'; trackActive.style.right = rightPct + '%';
  sliderRangeText.innerHTML = '<span class="range-indicator">' + allDates[startIdx] + '</span> ~ <span class="range-indicator">' + allDates[endIdx] + '</span> (' + (endIdx - startIdx + 1) + '天)';
}

sliderStart.addEventListener('input', function() {
  let s = parseInt(this.value); let e = parseInt(sliderEnd.value);
  if (s >= e) { this.value = e - 1; s = e - 1; if (s < 0) { this.value = 0; s = 0; } }
  applyFilter(s, e); resetPresetButtons();
});

sliderEnd.addEventListener('input', function() {
  let e = parseInt(this.value); let s = parseInt(sliderStart.value);
  if (e <= s) { this.value = s + 1; e = s + 1; if (e >= totalCount) { this.value = totalCount - 1; e = totalCount - 1; } }
  applyFilter(s, e); resetPresetButtons();
});

const datePicker = document.getElementById('date-picker');
datePicker.setAttribute('min', allDates[0]);
datePicker.setAttribute('max', allDates[allDates.length - 1]);
datePicker.value = allDates[allDates.length - 1];

function queryDate() {
  const date = datePicker.value;
  const idx = dateIndexMap[date];
  const resultEl = document.getElementById('query-result');
  if (idx !== undefined) {
    const openVal = allOpens[idx], highVal = allHighs[idx], lowVal = allLows[idx], closeVal = allCloses[idx], countVal = allCounts[idx];
    const prevClose = idx > 0 ? allCloses[idx - 1] : null;
    const delta = prevClose !== null ? closeVal - prevClose : 0;
    const color = delta >= 0 ? '#e85d5d' : '#2ecc71', sign = delta >= 0 ? '+' : '';
    resultEl.innerHTML = `<div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
      <span class="rate-val">${{formatRate(closeVal)}}</span>
      <span style="font-size:11px;color:${{color}}">(${{sign}}${{delta.toFixed(4)}})</span>
      <span style="font-size:10px;color:#999">${{countVal}}次报价</span>
      <a class="verify-link" href="''' + CITIC_VERIFY_URL + '''" target="_blank" rel="noopener">&#x1f517; 中信官网核实</a></div>
      <div class="ohlc-mini"><span class="o">开 ${{formatRate(openVal)}}</span><span class="h">高 ${{formatRate(highVal)}}</span><span class="l">低 ${{formatRate(lowVal)}}</span><span class="c">收 ${{formatRate(closeVal)}}</span></div>`;
    highlightDate(idx);
  } else {
    resultEl.innerHTML = '<span class="no-data">该日期无数据</span> <a class="verify-link" href="''' + CITIC_VERIFY_URL + '''" target="_blank" rel="noopener">&#x1f517; 中信官网查询</a>';
  }
}

function highlightDate(idx) {
  if (!chart) return;
  const nums = new Array(allDates.length).fill(0); nums[idx] = 6;
  const bg = new Array(allDates.length).fill('transparent'); bg[idx] = '#e8313e';
  chart.data.datasets.forEach(ds => { ds.pointRadius = nums; ds.pointBackgroundColor = bg; ds.pointBorderColor = bg; });
  chart.update('none');
  clearTimeout(window._hlTimer);
  window._hlTimer = setTimeout(() => { chart.data.datasets.forEach(ds => { ds.pointRadius = 0; ds.pointBackgroundColor = '#e8313e'; }); chart.update('none'); }, 3000);
}

datePicker.addEventListener('keydown', function(e) { if (e.key === 'Enter') queryDate(); });

const controlsDiv = document.getElementById('controls');
presets.forEach(p => {
  const key = p.month ? ('month_' + p.month) : ('days_' + (p.days || 99999));
  const btn = document.createElement('button');
  btn.textContent = p.label; btn.setAttribute('data-key', key);
  btn.addEventListener('click', () => applyPreset(p));
  controlsDiv.appendChild(btn);
});

initChart();
updateStats(0, allDates.length - 1);
updateSliders(0, allDates.length - 1);
applyPreset({ days: 99999 });
</script>
</body>
</html>'''


# ============================================================
# 主入口
# ============================================================

def generate_index_html():
    """生成首页导航页面"""
    safe = load_json(SAFE_DATA_FILE)
    citic = load_json(CITIC_DATA_FILE)
    safe_records = safe.get("records", [])
    citic_records = citic.get("records", [])
    sc = len(safe_records)
    sl = safe_records[-1]["date"] if safe_records else "N/A"
    cc = len(citic_records)
    cl = citic_records[-1]["date"] if citic_records else "N/A"
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>汇率看板</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px}
.container{max-width:600px;width:100%;background:#fff;border-radius:20px;box-shadow:0 20px 60px rgba(0,0,0,.15);padding:40px 32px;text-align:center}
h1{font-size:26px;color:#1a1a2e;margin-bottom:6px}
.sub{font-size:13px;color:#888;margin-bottom:32px}
.cards{display:flex;flex-direction:column;gap:16px}
.card{display:block;padding:24px;border-radius:14px;text-decoration:none;text-align:left;transition:transform .2s,box-shadow .2s;color:#fff}
.card:hover{transform:translateY(-3px);box-shadow:0 8px 25px rgba(0,0,0,.15)}
.card-safe{background:linear-gradient(135deg,#1764d9,#4a90e2)}
.card-citic{background:linear-gradient(135deg,#e8313e,#f06b75)}
.card-title{font-size:18px;font-weight:700;margin-bottom:6px}
.card-desc{font-size:12px;opacity:.85;margin-bottom:12px}
.card-stats{display:flex;gap:20px;font-size:13px;opacity:.9}
.card-stats span{display:block}
.card-stats strong{font-size:16px}
.footer{margin-top:32px;font-size:11px;color:#aaa}
</style>
</head>
<body>
<div class="container">
<h1>汇率看板</h1>
<div class="sub">USD/CNY 实时数据 · GitHub Pages 自动更新</div>

<div class="cards">
<a class="card card-safe" href="fx_chart.html">
  <div class="card-title">美元 / 人民币中间价走势</div>
  <div class="card-desc">数据来源：国家外汇管理局（SAFE）</div>
  <div class="card-stats">
    <span>记录数<br><strong>''' + str(sc) + '''</strong></span>
    <span>最新日期<br><strong>''' + sl + '''</strong></span>
  </div>
</a>

<a class="card card-citic" href="citic_fx_chart.html">
  <div class="card-title">中信银行 结汇价 OHLC</div>
  <div class="card-desc">数据来源：中信银行官方外汇牌价</div>
  <div class="card-stats">
    <span>记录数<br><strong>''' + str(cc) + '''</strong></span>
    <span>最新日期<br><strong>''' + cl + '''</strong></span>
  </div>
</a>
</div>

<div class="footer">
  每天北京时间 11:00 自动更新 · 更新于 ''' + now + '''
</div>
</div>
</body>
</html>'''

    index_path = DOCS_DIR / "index.html"
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html)
    log("  首页已生成: %s" % index_path)


def main():
    log("=" * 60)
    log("GitHub Actions 汇率同步 — 启动")
    log("=" * 60)

    args = sys.argv[1:]
    chart_only = "--chart-only" in args
    full = "--full" in args

    errors = []

    if chart_only:
        log("模式: 仅重新生成 HTML 图表")
        safe_records = load_json(SAFE_DATA_FILE).get("records", [])
        citic_records = load_json(CITIC_DATA_FILE).get("records", [])
        generate_safe_html(safe_records)
        generate_citic_html(citic_records)
        generate_index_html()
    else:
        if full:
            log("模式: 全量重新采集 (2025年至今)")

        # Step 1: SAFE
        try:
            safe_records = sync_safe(full=full)
        except Exception as e:
            msg = "SAFE 同步异常: %s" % e
            log(msg)
            errors.append(msg)

        # Step 2: CITIC
        try:
            citic_records = sync_citic(full=full)
        except Exception as e:
            msg = "CITIC 同步异常: %s" % e
            log(msg)
            errors.append(msg)

        # Step 3: 首页
        generate_index_html()

    log("")
    if errors:
        log("同步完成（有 %d 个错误）" % len(errors))
        for e in errors:
            log("  ! %s" % e)
    else:
        log("同步完成 — 全部成功")

    return len(errors)


if __name__ == "__main__":
    sys.exit(main())
