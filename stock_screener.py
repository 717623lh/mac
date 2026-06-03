from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime as dt
import queue
import sqlite3
import sys
import threading
import time
import traceback
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Iterable, List, Optional, Sequence, Tuple

import tkinter as tk
from tkinter import messagebox, ttk


APP_NAME = "A股实时均线筛选"
APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"


# 在 PyInstaller 打包后，数据库应存在用户可写目录下
def _get_db_path() -> Path:
    if getattr(sys, "frozen", False):
        if sys.platform == "darwin":
            base = Path.home() / "Library" / "Application Support" / "AStockScreener"
        else:
            base = Path.home() / ".AStockScreener"
        base.mkdir(parents=True, exist_ok=True)
        db_path = base / "stock_history.sqlite3"
        # 首次运行时，把应用包内预置的数据库复制到用户目录
        if not db_path.exists():
            bundled_db = DATA_DIR / "stock_history.sqlite3"
            if bundled_db.exists():
                import shutil
                shutil.copy2(str(bundled_db), str(db_path))
        return db_path
    # 开发模式下用项目目录
    return DATA_DIR / "stock_history.sqlite3"


DB_PATH = _get_db_path()
CHINA_TZ = dt.timezone(dt.timedelta(hours=8))
HISTORY_DAYS = 20
MIN_HISTORY_DAYS = 13
HISTORY_LOOKBACK_DAYS = 120
DEFAULT_INTERVAL_SECONDS = 15
DEFAULT_HISTORY_WORKERS = 6


def china_today() -> dt.date:
    return dt.datetime.now(CHINA_TZ).date()


def timestamp_text() -> str:
    return dt.datetime.now(CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S")


def normalize_code(value: object) -> str:
    text = str(value).strip()
    lower_text = text.lower()
    for prefix in ("sh", "sz", "bj"):
        if lower_text.startswith(prefix) and text[len(prefix) :].isdigit():
            text = text[len(prefix) :]
            break
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text.zfill(6) if text.isdigit() else text


def pick_column(columns: Iterable[str], candidates: Sequence[str]) -> str:
    column_map = {str(col).strip().lower(): str(col) for col in columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in column_map:
            return column_map[key]
    raise KeyError("未找到列: " + "/".join(candidates))


def mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


@dataclasses.dataclass(frozen=True)
class StockQuote:
    code: str
    name: str
    price: float
    prev_close: Optional[float] = None


@dataclasses.dataclass(frozen=True)
class ScreeningResult:
    code: str
    name: str
    price: float
    updated_at: str


@dataclasses.dataclass(frozen=True)
class ScreeningStats:
    total_quotes: int
    usable_quotes: int
    missing_history: int
    matched: int
    updated_at: str


class StockDataClient:
    def __init__(
        self, adjust: str = "", timeout: float = 10.0, retry_times: int = 3
    ) -> None:
        self.adjust = adjust
        self.timeout = timeout
        self.retry_times = retry_times
        self._code_name_cache = None
        self._spot_source: Optional[str] = None
        self._prefer_direct_history = False

    def _akshare(self):
        try:
            from akshare.stock.stock_info import stock_info_a_code_name  # type: ignore
            from akshare.stock.stock_zh_a_sina import stock_zh_a_spot  # type: ignore
            from akshare.stock_feature.stock_hist_em import (  # type: ignore
                stock_zh_a_hist,
                stock_zh_a_spot_em,
            )
        except ImportError as exc:
            raise RuntimeError(
                "当前 Python 环境未安装 AKShare。请先运行: python -m pip install -r requirements.txt"
            ) from exc
        ak = SimpleNamespace(
            stock_info_a_code_name=stock_info_a_code_name,
            stock_zh_a_hist=stock_zh_a_hist,
            stock_zh_a_spot=stock_zh_a_spot,
            stock_zh_a_spot_em=stock_zh_a_spot_em,
        )
        return ak

    def _request_with_retries(self, func: Callable[[], object]) -> object:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.retry_times + 1):
            try:
                return func()
            except Exception as exc:
                last_error = exc
                if attempt >= self.retry_times:
                    break
                time.sleep(1.2 * attempt)
        if last_error:
            raise last_error
        raise RuntimeError("AKShare 请求失败")

    def fetch_spot_quotes(self) -> List[StockQuote]:
        import pandas as pd

        ak = self._akshare()
        df = self._fetch_spot_frame(ak)
        code_col = pick_column(df.columns, ["代码", "code", "symbol"])
        name_col = pick_column(df.columns, ["名称", "name"])
        price_col = pick_column(df.columns, ["最新价", "最新", "现价", "price", "trade"])

        prev_close_col = None
        try:
            prev_close_col = pick_column(df.columns, ["昨收", "昨收价", "prev_close"])
        except KeyError:
            prev_close_col = None

        work = df[[code_col, name_col, price_col]].copy()
        work["code"] = work[code_col].map(normalize_code)
        work["name"] = work[name_col].astype(str).str.strip()
        work["price"] = pd.to_numeric(work[price_col], errors="coerce")

        if prev_close_col:
            work["prev_close"] = pd.to_numeric(df[prev_close_col], errors="coerce")
        else:
            work["prev_close"] = None

        work = work.dropna(subset=["price"])
        work = work[work["price"] > 0]

        quotes: List[StockQuote] = []
        for row in work.itertuples(index=False):
            prev_close = None
            raw_prev = getattr(row, "prev_close", None)
            if raw_prev is not None and pd.notna(raw_prev):
                prev_close = float(raw_prev)
            quotes.append(
                StockQuote(
                    code=str(row.code),
                    name=str(row.name),
                    price=float(row.price),
                    prev_close=prev_close,
                )
            )
        return quotes

    def _fetch_spot_frame(self, ak):
        errors: List[str] = []
        sources = [
            (
                "stock_zh_a_spot_em",
                lambda: self._request_with_retries(getattr(ak, "stock_zh_a_spot_em")),
            ),
            (
                "stock_zh_a_spot",
                lambda: self._request_with_retries(getattr(ak, "stock_zh_a_spot")),
            ),
            ("eastmoney_curl_fallback", self._fetch_spot_frame_direct_em),
            ("tencent_fallback", lambda: self._fetch_spot_frame_tencent(ak)),
        ]
        if self._spot_source:
            sources.sort(key=lambda item: 0 if item[0] == self._spot_source else 1)

        for source_name, source_func in sources:
            try:
                frame = source_func()
                self._spot_source = source_name
                return frame
            except Exception as exc:
                errors.append("{}: {}".format(source_name, exc))
                if source_name == self._spot_source:
                    self._spot_source = None
        raise RuntimeError("所有 AKShare 实时行情接口均失败: " + " | ".join(errors))

    def _fetch_spot_frame_direct_em(self):
        import pandas as pd
        from curl_cffi import requests as curl_requests

        url = "https://82.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1",
            "pz": "100",
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
            "fields": "f2,f12,f14,f18",
        }
        rows = []
        total = None
        page = 1

        while total is None or len(rows) < total:
            params["pn"] = str(page)

            def request_page():
                return curl_requests.get(
                    url, params=params, timeout=self.timeout, impersonate="chrome"
                )

            response = self._request_with_retries(request_page)
            response.raise_for_status()
            data_json = response.json()
            data = data_json.get("data") or {}
            diff = data.get("diff") or []
            if not diff:
                break
            rows.extend(diff)
            total = int(data.get("total") or len(rows))
            page += 1

        return pd.DataFrame(
            {
                "代码": [item.get("f12") for item in rows],
                "名称": [item.get("f14") for item in rows],
                "最新价": [item.get("f2") for item in rows],
                "昨收": [item.get("f18") for item in rows],
            }
        )

    def _fetch_spot_frame_tencent(self, ak):
        import pandas as pd
        import requests

        if self._code_name_cache is None:
            self._code_name_cache = self._request_with_retries(
                lambda: ak.stock_info_a_code_name()
            )

        code_df = self._code_name_cache.copy()
        code_col = pick_column(code_df.columns, ["代码", "code", "symbol"])
        name_col = pick_column(code_df.columns, ["名称", "name"])
        code_df["code"] = code_df[code_col].map(normalize_code)
        code_df["name"] = code_df[name_col].astype(str).str.strip()
        code_df = code_df.dropna(subset=["code"])
        name_map = dict(zip(code_df["code"], code_df["name"]))

        symbols = [self._tencent_symbol(code) for code in code_df["code"]]
        rows = []
        batch_size = 650
        for start in range(0, len(symbols), batch_size):
            batch = symbols[start : start + batch_size]
            url = "https://qt.gtimg.cn/q=" + ",".join(batch)

            def request_batch():
                return requests.get(url, timeout=self.timeout)

            response = self._request_with_retries(request_batch)
            response.raise_for_status()
            response.encoding = "GBK"
            for line in response.text.split(";"):
                if '="' not in line:
                    continue
                payload = line.split('="', 1)[1].strip().strip('"')
                fields = payload.split("~")
                if len(fields) < 5:
                    continue
                code = normalize_code(fields[2])
                rows.append(
                    {
                        "代码": code,
                        "名称": fields[1] or name_map.get(code, ""),
                        "最新价": fields[3],
                        "昨收": fields[4],
                    }
                )

        return pd.DataFrame(rows)

    def _tencent_symbol(self, code: str) -> str:
        if code.startswith("6"):
            return "sh" + code
        if code.startswith(("4", "8", "9")):
            return "bj" + code
        return "sz" + code

    def fetch_history_closes(
        self, code: str, before_date: dt.date
    ) -> List[Tuple[str, float]]:
        import pandas as pd

        ak = self._akshare()
        end_date = before_date.strftime("%Y%m%d")
        start_date = (before_date - dt.timedelta(days=HISTORY_LOOKBACK_DAYS)).strftime(
            "%Y%m%d"
        )
        kwargs = {
            "symbol": code,
            "period": "daily",
            "start_date": start_date,
            "end_date": end_date,
            "adjust": self.adjust,
        }

        def request_history():
            try:
                return ak.stock_zh_a_hist(**kwargs, timeout=self.timeout)
            except TypeError as exc:
                if "timeout" not in str(exc):
                    raise
                return ak.stock_zh_a_hist(**kwargs)

        if self._prefer_direct_history:
            try:
                df = self._fetch_history_frame_direct_em(code, start_date, end_date)
            except Exception:
                df = self._fetch_history_frame_tencent(code)
        else:
            try:
                df = self._request_with_retries(request_history)
            except Exception:
                self._prefer_direct_history = True
                try:
                    df = self._fetch_history_frame_direct_em(code, start_date, end_date)
                except Exception:
                    df = self._fetch_history_frame_tencent(code)

        if df is None or df.empty:
            return []

        date_col = pick_column(df.columns, ["日期", "date", "交易日"])
        close_col = pick_column(df.columns, ["收盘", "close", "收盘价"])
        work = df[[date_col, close_col]].copy()
        work["trade_date"] = pd.to_datetime(work[date_col], errors="coerce").dt.date
        work["close"] = pd.to_numeric(work[close_col], errors="coerce")
        work = work.dropna(subset=["trade_date", "close"])
        work = work[work["trade_date"] < before_date]
        work = work.sort_values("trade_date").tail(HISTORY_DAYS)

        return [
            (row.trade_date.isoformat(), float(row.close))
            for row in work.itertuples(index=False)
        ]

    def _fetch_history_frame_direct_em(
        self, code: str, start_date: str, end_date: str
    ):
        import pandas as pd
        from curl_cffi import requests as curl_requests

        market_code = "1" if code.startswith("6") else "0"
        adjust_map = {"qfq": "1", "hfq": "2", "": "0"}
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "klt": "101",
            "fqt": adjust_map.get(self.adjust, "0"),
            "secid": "{}.{}".format(market_code, code),
            "beg": start_date,
            "end": end_date,
        }

        def request_history():
            return curl_requests.get(
                "https://push2his.eastmoney.com/api/qt/stock/kline/get",
                params=params,
                timeout=self.timeout,
                impersonate="chrome",
            )

        response = self._request_with_retries(request_history)
        response.raise_for_status()
        data_json = response.json()
        data = data_json.get("data") or {}
        klines = data.get("klines") or []
        if not klines:
            return pd.DataFrame()

        rows = [item.split(",") for item in klines]
        temp_df = pd.DataFrame(rows)
        temp_df["股票代码"] = code
        temp_df.columns = [
            "日期",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
            "股票代码",
        ]
        return temp_df

    def _fetch_history_frame_tencent(self, code: str):
        import pandas as pd
        import requests

        symbol = self._tencent_symbol(code)
        params = {"param": "{},day,,,{:d},".format(symbol, HISTORY_DAYS + 15)}

        def request_history():
            return requests.get(
                "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
                params=params,
                timeout=self.timeout,
            )

        response = self._request_with_retries(request_history)
        response.raise_for_status()
        data_json = response.json()
        stock_data = (data_json.get("data") or {}).get(symbol) or {}
        rows = stock_data.get("day") or stock_data.get("qfqday") or []
        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(
            {
                "日期": [row[0] for row in rows],
                "收盘": [row[2] for row in rows],
            }
        )


class HistoryCache:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), timeout=30, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS history_close (
                    code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    close REAL NOT NULL,
                    PRIMARY KEY (code, trade_date)
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_code_date "
                "ON history_close(code, trade_date)"
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS history_refresh (
                    code TEXT NOT NULL,
                    before_date TEXT NOT NULL,
                    refreshed_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    PRIMARY KEY (code, before_date)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def store_closes(self, code: str, closes: Sequence[Tuple[str, float]]) -> None:
        if not closes:
            return
        rows = [(code, trade_date, close) for trade_date, close in closes]
        with self._lock, self._conn:
            self._conn.executemany(
                "INSERT OR REPLACE INTO history_close(code, trade_date, close) "
                "VALUES (?, ?, ?)",
                rows,
            )

    def mark_refreshed(self, code: str, before_date: dt.date, status: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO history_refresh"
                "(code, before_date, refreshed_at, status) VALUES (?, ?, ?, ?)",
                (code, before_date.isoformat(), timestamp_text(), status),
            )

    def is_refreshed(self, code: str, before_date: dt.date) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM history_refresh WHERE code=? AND before_date=?",
                (code, before_date.isoformat()),
            ).fetchone()
        return row is not None

    def get_last_closes(
        self, code: str, before_date: dt.date, limit: int = HISTORY_DAYS
    ) -> List[float]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT close FROM (
                    SELECT trade_date, close
                    FROM history_close
                    WHERE code=? AND trade_date < ?
                    ORDER BY trade_date DESC
                    LIMIT ?
                )
                ORDER BY trade_date ASC
                """,
                (code, before_date.isoformat(), limit),
            ).fetchall()
        return [float(row[0]) for row in rows]

    def has_min_history(self, code: str, before_date: dt.date) -> bool:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT 1
                    FROM history_close
                    WHERE code=? AND trade_date < ?
                    ORDER BY trade_date DESC
                    LIMIT ?
                )
                """,
                (code, before_date.isoformat(), MIN_HISTORY_DAYS),
            ).fetchone()
        return bool(row and int(row[0]) >= MIN_HISTORY_DAYS)

    def count_codes_with_history(self, before_date: dt.date) -> int:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT code
                    FROM history_close
                    WHERE trade_date < ?
                    GROUP BY code
                    HAVING COUNT(*) >= ?
                )
                """,
                (before_date.isoformat(), MIN_HISTORY_DAYS),
            ).fetchone()
        return int(row[0]) if row else 0

    def set_meta(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", (key, value)
            )

    def get_meta(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key=?", (key,)
            ).fetchone()
        return str(row[0]) if row else None


def quote_passes_filter(
    price: float, closes: Sequence[float], prev_close: Optional[float] = None
) -> bool:
    if len(closes) < MIN_HISTORY_DAYS:
        return False

    previous_close = prev_close if prev_close is not None else closes[-1]
    a = mean(list(closes[-4:]) + [price])
    b = mean(list(closes[-7:]) + [price])
    c = mean(list(closes[-12:]) + [price])
    d = mean(closes[-5:])
    e = mean(closes[-8:])
    f = mean(closes[-13:])

    return (
        a >= d
        and b >= e
        and c >= f
        and price >= a
        and price >= b
        and price >= c
        and a >= previous_close
        and b >= previous_close
        and c >= previous_close
    )


def screen_quotes(
    quotes: Sequence[StockQuote],
    history_getter: Callable[[str], List[float]],
) -> Tuple[List[ScreeningResult], ScreeningStats]:
    updated_at = timestamp_text()
    results: List[ScreeningResult] = []
    missing_history = 0
    usable_quotes = 0

    for quote in quotes:
        closes = history_getter(quote.code)
        if len(closes) < MIN_HISTORY_DAYS:
            missing_history += 1
            continue
        usable_quotes += 1
        if quote_passes_filter(quote.price, closes, quote.prev_close):
            results.append(
                ScreeningResult(
                    code=quote.code,
                    name=quote.name,
                    price=quote.price,
                    updated_at=updated_at,
                )
            )

    results.sort(key=lambda item: item.code)
    return results, ScreeningStats(
        total_quotes=len(quotes),
        usable_quotes=usable_quotes,
        missing_history=missing_history,
        matched=len(results),
        updated_at=updated_at,
    )


class ScreeningWorker(threading.Thread):
    def __init__(
        self,
        outbox: "queue.Queue[Tuple[str, object]]",
        interval_getter: Callable[[], int],
        workers_getter: Callable[[], int],
        stop_event: threading.Event,
        run_event: threading.Event,
        refresh_now_event: threading.Event,
        refresh_history_event: threading.Event,
        force_history_event: threading.Event,
    ) -> None:
        super().__init__(daemon=True)
        self.outbox = outbox
        self.interval_getter = interval_getter
        self.workers_getter = workers_getter
        self.stop_event = stop_event
        self.run_event = run_event
        self.refresh_now_event = refresh_now_event
        self.refresh_history_event = refresh_history_event
        self.force_history_event = force_history_event
        self.client = StockDataClient()
        self.cache = HistoryCache(DB_PATH)
        self.quotes: List[StockQuote] = []

    def post(self, kind: str, payload: object) -> None:
        self.outbox.put((kind, payload))

    def run(self) -> None:
        self.post("status", "启动中，正在获取实时行情...")
        next_refresh = 0.0
        try:
            while not self.stop_event.is_set():
                if not self.run_event.is_set():
                    time.sleep(0.2)
                    continue

                if self.refresh_history_event.is_set():
                    self.refresh_history_event.clear()
                    try:
                        self._refresh_history(force=self.force_history_event.is_set())
                    except RuntimeError as exc:
                        self.post("error", ("依赖或接口错误", str(exc)))
                        self.run_event.clear()
                    except Exception as exc:
                        self.post("status", "历史缓存刷新失败：{}".format(exc))
                    self.force_history_event.clear()
                    next_refresh = 0.0

                now = time.monotonic()
                if self.refresh_now_event.is_set() or now >= next_refresh:
                    self.refresh_now_event.clear()
                    try:
                        self._screen_once()
                    except RuntimeError as exc:
                        self.post("error", ("依赖或接口错误", str(exc)))
                        self.run_event.clear()
                    except Exception as exc:
                        self.post("status", "本轮行情刷新失败：{}".format(exc))
                    next_refresh = time.monotonic() + max(5, self.interval_getter())

                time.sleep(0.2)
        except Exception as exc:
            self.post("error", ("程序运行出错", str(exc) + "\n\n" + traceback.format_exc()))
        finally:
            self.cache.close()

    def _ensure_quotes(self) -> List[StockQuote]:
        if not self.quotes:
            self.post("status", "正在获取 A 股实时行情列表...")
            self.quotes = self.client.fetch_spot_quotes()
        return self.quotes

    def _screen_once(self) -> None:
        before_date = china_today()
        self.post("status", "正在刷新实时行情...")
        self.quotes = self.client.fetch_spot_quotes()

        if self._needs_initial_history_refresh(self.quotes, before_date):
            self.post("status", "历史缓存未准备好，开始更新最近 20 个交易日收盘价...")
            self._refresh_history(force=False)

        self.post("status", "正在计算筛选条件...")
        results, stats = screen_quotes(
            self.quotes,
            lambda code: self.cache.get_last_closes(code, before_date, HISTORY_DAYS),
        )
        self.post("results", (results, stats))
        self.post(
            "status",
            "已刷新：{}，可计算 {} 只，符合 {} 只".format(
                stats.updated_at, stats.usable_quotes, stats.matched
            ),
        )

    def _needs_initial_history_refresh(
        self, quotes: Sequence[StockQuote], before_date: dt.date
    ) -> bool:
        if not quotes:
            return False
        enough = self.cache.count_codes_with_history(before_date)
        return enough < min(200, len(quotes))

    def _refresh_history(self, force: bool) -> None:
        before_date = china_today()
        quotes = self._ensure_quotes()
        unique_quotes = {quote.code: quote for quote in quotes}
        todo: List[StockQuote] = []

        for quote in unique_quotes.values():
            if force:
                todo.append(quote)
                continue
            if self.cache.is_refreshed(quote.code, before_date) and self.cache.has_min_history(
                quote.code, before_date
            ):
                continue
            todo.append(quote)

        if not todo:
            self.post("progress", (0, 0, "历史缓存已是最新"))
            return

        max_workers = max(1, min(16, self.workers_getter()))
        total = len(todo)
        completed = 0
        errors = 0
        stored = 0
        self.post(
            "progress",
            (0, total, "开始更新历史收盘价，股票数 {}".format(total)),
        )

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        future_map = {
            executor.submit(self.client.fetch_history_closes, quote.code, before_date): quote
            for quote in todo
        }
        try:
            for future in concurrent.futures.as_completed(future_map):
                if self.stop_event.is_set():
                    break
                quote = future_map[future]
                try:
                    closes = future.result()
                    if closes:
                        self.cache.store_closes(quote.code, closes)
                        self.cache.mark_refreshed(quote.code, before_date, "ok")
                        stored += 1
                    else:
                        self.cache.mark_refreshed(quote.code, before_date, "empty")
                except Exception:
                    errors += 1

                completed += 1
                if completed == total or completed % 10 == 0:
                    self.post(
                        "progress",
                        (
                            completed,
                            total,
                            "历史缓存更新中 {}/{}，成功 {}，失败 {}".format(
                                completed, total, stored, errors
                            ),
                        ),
                    )
        finally:
            executor.shutdown(wait=False)

        self.cache.set_meta("last_history_refresh", timestamp_text())
        self.post(
            "progress",
            (
                completed,
                total,
                "历史缓存完成 {}/{}，成功 {}，失败 {}".format(
                    completed, total, stored, errors
                ),
            ),
        )


class StockScreenerApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(APP_NAME)
        self.root.geometry("860x560")
        self.root.minsize(760, 460)

        self.outbox: "queue.Queue[Tuple[str, object]]" = queue.Queue()
        self.stop_event = threading.Event()
        self.run_event = threading.Event()
        self.refresh_now_event = threading.Event()
        self.refresh_history_event = threading.Event()
        self.force_history_event = threading.Event()
        self.worker: Optional[ScreeningWorker] = None

        self.interval_var = tk.IntVar(value=DEFAULT_INTERVAL_SECONDS)
        self.workers_var = tk.IntVar(value=DEFAULT_HISTORY_WORKERS)
        self.status_var = tk.StringVar(value="准备就绪")
        self.count_var = tk.StringVar(value="符合条件：0")
        self.progress_var = tk.StringVar(value="")

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(100, self._poll_outbox)

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)

        toolbar = ttk.Frame(self.root, padding=(10, 8, 10, 6))
        toolbar.grid(row=0, column=0, sticky="ew")
        toolbar.columnconfigure(8, weight=1)

        ttk.Label(toolbar, text="刷新间隔(秒)").grid(row=0, column=0, padx=(0, 4))
        interval = ttk.Spinbox(
            toolbar,
            from_=5,
            to=300,
            increment=5,
            textvariable=self.interval_var,
            width=6,
        )
        interval.grid(row=0, column=1, padx=(0, 10))

        ttk.Label(toolbar, text="历史线程").grid(row=0, column=2, padx=(0, 4))
        workers = ttk.Spinbox(
            toolbar,
            from_=1,
            to=16,
            increment=1,
            textvariable=self.workers_var,
            width=5,
        )
        workers.grid(row=0, column=3, padx=(0, 12))

        ttk.Button(toolbar, text="开始", command=self.start).grid(row=0, column=4, padx=3)
        ttk.Button(toolbar, text="暂停", command=self.pause).grid(row=0, column=5, padx=3)
        ttk.Button(toolbar, text="立即刷新", command=self.refresh_now).grid(
            row=0, column=6, padx=3
        )
        ttk.Button(toolbar, text="刷新历史", command=self.refresh_history).grid(
            row=0, column=7, padx=3
        )
        ttk.Label(toolbar, textvariable=self.count_var, anchor="e").grid(
            row=0, column=8, sticky="e"
        )

        table_frame = ttk.Frame(self.root, padding=(10, 0, 10, 8))
        table_frame.grid(row=1, column=0, sticky="nsew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        columns = ("code", "name", "price", "updated_at")
        self.tree = ttk.Treeview(
            table_frame,
            columns=columns,
            show="headings",
            selectmode="browse",
        )
        self.tree.heading("code", text="代码")
        self.tree.heading("name", text="名称")
        self.tree.heading("price", text="实时现价")
        self.tree.heading("updated_at", text="刷新时间")
        self.tree.column("code", width=120, anchor="center", stretch=False)
        self.tree.column("name", width=200, anchor="w", stretch=True)
        self.tree.column("price", width=120, anchor="e", stretch=False)
        self.tree.column("updated_at", width=180, anchor="center", stretch=False)
        self.tree.grid(row=0, column=0, sticky="nsew")

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=scrollbar.set)

        status_bar = ttk.Frame(self.root, padding=(10, 0, 10, 8))
        status_bar.grid(row=2, column=0, sticky="ew")
        status_bar.columnconfigure(0, weight=2)
        status_bar.columnconfigure(1, weight=1)
        ttk.Label(status_bar, textvariable=self.status_var, anchor="w").grid(
            row=0, column=0, sticky="ew"
        )
        ttk.Label(status_bar, textvariable=self.progress_var, anchor="e").grid(
            row=0, column=1, sticky="ew"
        )

        style = ttk.Style()
        if "vista" in style.theme_names():
            style.theme_use("vista")
        style.configure("Treeview", rowheight=28)

    def start(self) -> None:
        self.run_event.set()
        self.refresh_now_event.set()
        if self.worker is None or not self.worker.is_alive():
            self.worker = ScreeningWorker(
                outbox=self.outbox,
                interval_getter=self._safe_interval,
                workers_getter=self._safe_workers,
                stop_event=self.stop_event,
                run_event=self.run_event,
                refresh_now_event=self.refresh_now_event,
                refresh_history_event=self.refresh_history_event,
                force_history_event=self.force_history_event,
            )
            self.worker.start()
        self.status_var.set("已启动")

    def pause(self) -> None:
        self.run_event.clear()
        self.status_var.set("已暂停")

    def refresh_now(self) -> None:
        self.run_event.set()
        self.refresh_now_event.set()
        self.status_var.set("准备立即刷新")
        if self.worker is None or not self.worker.is_alive():
            self.start()

    def refresh_history(self) -> None:
        self.run_event.set()
        self.force_history_event.set()
        self.refresh_history_event.set()
        self.status_var.set("准备刷新历史缓存")
        if self.worker is None or not self.worker.is_alive():
            self.start()

    def _safe_interval(self) -> int:
        try:
            return max(5, int(self.interval_var.get()))
        except tk.TclError:
            return DEFAULT_INTERVAL_SECONDS

    def _safe_workers(self) -> int:
        try:
            return max(1, min(16, int(self.workers_var.get())))
        except tk.TclError:
            return DEFAULT_HISTORY_WORKERS

    def _poll_outbox(self) -> None:
        while True:
            try:
                kind, payload = self.outbox.get_nowait()
            except queue.Empty:
                break

            if kind == "status":
                self.status_var.set(str(payload))
            elif kind == "progress":
                current, total, text = payload  # type: ignore[misc]
                self.progress_var.set(str(text))
                if total:
                    self.count_var.set("历史缓存：{}/{}".format(current, total))
            elif kind == "results":
                results, stats = payload  # type: ignore[misc]
                self._render_results(results)
                self.count_var.set("符合条件：{}".format(stats.matched))
                self.progress_var.set(
                    "行情 {} 只，可计算 {} 只".format(stats.total_quotes, stats.usable_quotes)
                )
            elif kind == "error":
                title, detail = payload  # type: ignore[misc]
                self.status_var.set(str(title))
                messagebox.showerror(str(title), str(detail))

        self.root.after(150, self._poll_outbox)

    def _render_results(self, results: Sequence[ScreeningResult]) -> None:
        self.tree.delete(*self.tree.get_children())
        for item in results:
            self.tree.insert(
                "",
                "end",
                values=(item.code, item.name, "{:.2f}".format(item.price), item.updated_at),
            )

    def on_close(self) -> None:
        self.stop_event.set()
        self.run_event.set()
        self.root.after(100, self.root.destroy)


def main() -> None:
    root = tk.Tk()
    app = StockScreenerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
