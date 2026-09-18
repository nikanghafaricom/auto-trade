# ==============================================
# Hybrid Signal Bot - نسخه رندر (Render: دریافت، تحلیل، ارسال به کانال و همروش)
# ==============================================
import os
import time
import logging
import gc
import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, date as date_cls
from typing import Dict, Optional, List, Tuple
import pandas as pd
import ccxt
import requests
from dotenv import load_dotenv

load_dotenv()

# ==================== تنظیمات ====================
class Config:
    EXCHANGE_ID = "coinex"
    API_KEY = os.getenv("EXCHANGE_API_KEY", "")
    SECRET = os.getenv("EXCHANGE_SECRET", "")
    PASSWORD = os.getenv("EXCHANGE_PASSWORD", "")

    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")    # مخصوص کانال (سیگنال‌های خام)
    TELEGRAM_PERSONAL_ID = os.getenv("TELEGRAM_PERSONAL_ID", "") # مخصوص پی‌وی (نتیجه معاملات)
    HAMRAVESH_WEBHOOK_URL = os.getenv("HAMRAVESH_WEBHOOK_URL", "")
    SECRET_TOKEN = os.getenv("SECRET_TOKEN", "")

    SYMBOLS = [
        "BTC/USDT",
        "ETH/USDT",
        "SOL/USDT",
        "BNB/USDT",
        "XRP/USDT",
        "AVAX/USDT",
        "NEAR/USDT",
        "ADA/USDT",
        "DOGE/USDT",
        "LINK/USDT",
        "PAXG/USDT",
        "LTC/USDT",
    ]

    SYMBOL_GROUPS = {
        "BTC/USDT": "majors",
        "ETH/USDT": "majors",
        "BNB/USDT": "majors",
        "LTC/USDT": "majors",
        "SOL/USDT": "L1_alt",
        "AVAX/USDT": "L1_alt",
        "NEAR/USDT": "L1_alt",
        "ADA/USDT": "L1_alt",
        "XRP/USDT": "payments",
        "DOGE/USDT": "meme",
        "LINK/USDT": "oracle",
        "PAXG/USDT": "defensive_gold",
    }

    ENTRY_TIMEFRAME = "15m"
    TREND_TIMEFRAME = "4h"
    CHECK_INTERVAL = 300  # هر ۵ دقیقه یک‌بار

    # ---- تنظیمات لایه‌ی تحلیل و سیگنال (برگرفته از کد مرجع) ----
    GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
    CONFIRM_TIMEFRAME = "1h"
    MIN_SIGNAL_SCORE = 7.5
    ATR_PERCENTILE_MAX = 97
    MIN_JUDGE_CONFIDENCE = int(os.getenv("MIN_JUDGE_CONFIDENCE", 55))
    # فقط برای محاسبه‌ی «پیشنهاد حجم» داخل پیام سیگنال
    VIRTUAL_CAPITAL_USDT = float(os.getenv("VIRTUAL_CAPITAL_USDT", 10000))
    RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", 1.5))
    # تعداد کندل ۴ساعته برای warmup واقعی EMA200
    TREND_WARMUP_CANDLES = 300

    # ---- مدیریت ریسک، معامله‌ی مجازی و همبستگی (برگرفته از کد مرجع) ----
    MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", 4))
    MAX_TRADES_PER_GROUP = int(os.getenv("MAX_TRADES_PER_GROUP", 2))
    MAX_DAILY_LOSS_PCT = float(os.getenv("MAX_DAILY_LOSS_PCT", 3.0))
    EXCHANGE_TAKER_FEE_PCT = float(os.getenv("EXCHANGE_TAKER_FEE_PCT", 0.0))
    DEFAULT_SPREAD_PCT_FALLBACK = 0.05
    TRADE_MONITOR_INTERVAL_SECONDS = int(os.getenv("TRADE_MONITOR_INTERVAL_SECONDS", 30))
    CORRELATION_LOOKBACK_CANDLES = 100
    CORRELATION_REFRESH_HOURS = 1
    CORRELATION_THRESHOLD_MIN = 0.6
    CORRELATION_THRESHOLD_MAX = 0.8
    MAX_CORRELATED_TRADES = int(os.getenv("MAX_CORRELATED_TRADES", 2))

    def validate(self):
        if not self.TELEGRAM_BOT_TOKEN or (not self.TELEGRAM_CHANNEL_ID and not self.TELEGRAM_PERSONAL_ID):
            logger.warning("هشدار: توکن یا آیدی‌های تلگرام به درستی تنظیم نشده‌اند.")

# ==================== لاگ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("render_bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== منابع داده‌ی کلان/فرابازاری ====================
class MacroDataLayer:
    def __init__(self):
        self._fng_value: Optional[int] = None
        self._fng_last_fetch: float = 0.0
        self._fng_cache_seconds = 3600

    def get_fear_greed_index(self) -> Optional[int]:
        now = time.time()
        if self._fng_value is not None and (now - self._fng_last_fetch) < self._fng_cache_seconds:
            return self._fng_value
        try:
            resp = requests.get("https://api.alternative.me/fng/?limit=1", timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                value = int(data["data"][0]["value"])
                self._fng_value = value
                self._fng_last_fetch = now
                return value
        except Exception as e:
            logger.warning(f"دریافت شاخص ترس‌وطمع بازار ناموفق بود (نادیده گرفته می‌شه): {e}")
        return self._fng_value

# ==================== لایه تحلیل، ساختار بازار و رژیم نوسان ====================
class AnalysisLayer:
    def __init__(self, config: Config):
        self.config = config

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or len(df) < 30:
            return df
        df = df.copy()

        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))

        df['ema_fast'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema_slow'] = df['close'].ewm(span=50, adjust=False).mean()
        df['ema_trend'] = df['close'].ewm(span=200, adjust=False).mean()

        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = ema12 - ema26
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']

        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift()).abs()
        low_close = (df['low'] - df['close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df['true_range'] = tr
        df['atr'] = tr.rolling(window=14).mean()

        df['vol_sma'] = df['volume'].rolling(window=20).mean()
        df['support'] = df['low'].rolling(window=15).min()
        df['resistance'] = df['high'].rolling(window=15).max()

        return df

    def is_market_tradable(self, df_15m: pd.DataFrame) -> bool:
        if df_15m.empty or len(df_15m) < 30:
            return False
        latest = df_15m.iloc[-1]
        if pd.isna(latest['volume']) or pd.isna(latest['vol_sma']) or latest['vol_sma'] == 0:
            return False
        volume_ratio = latest['volume'] / latest['vol_sma']
        if volume_ratio < 0.6:
            return False
        return True

    def get_major_trend(self, df_4h: pd.DataFrame) -> str:
        if df_4h.empty or len(df_4h) < 100:
            return "NEUTRAL"
        latest = df_4h.iloc[-1]
        if latest['close'] > latest['ema_trend'] and latest['ema_fast'] > latest['ema_slow']:
            return "BULLISH"
        elif latest['close'] < latest['ema_trend'] and latest['ema_fast'] < latest['ema_slow']:
            return "BEARISH"
        return "NEUTRAL"

    def is_mtf_aligned(self, df_1h: pd.DataFrame, side: str) -> bool:
        if df_1h.empty or len(df_1h) < 60:
            return False
        latest = df_1h.iloc[-1]
        if pd.isna(latest.get('ema_fast')) or pd.isna(latest.get('ema_slow')):
            return False
        if side == "BUY":
            return bool(latest['ema_fast'] > latest['ema_slow'])
        return bool(latest['ema_fast'] < latest['ema_slow'])

    def market_structure(self, df: pd.DataFrame, lookback: int = 40) -> str:
        if df.empty or len(df) < lookback + 4:
            return "NEUTRAL"
        window = df.tail(lookback).reset_index(drop=True)
        swing_highs, swing_lows = [], []
        for i in range(2, len(window) - 2):
            h = window['high']
            l = window['low']
            if h[i] > h[i - 1] and h[i] > h[i - 2] and h[i] > h[i + 1] and h[i] > h[i + 2]:
                swing_highs.append(h[i])
            if l[i] < l[i - 1] and l[i] < l[i - 2] and l[i] < l[i + 1] and l[i] < l[i + 2]:
                swing_lows.append(l[i])
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            higher_high = swing_highs[-1] > swing_highs[-2]
            higher_low = swing_lows[-1] > swing_lows[-2]
            lower_high = swing_highs[-1] < swing_highs[-2]
            lower_low = swing_lows[-1] < swing_lows[-2]
            if higher_high and higher_low:
                return "BULLISH"
            if lower_high and lower_low:
                return "BEARISH"
        return "NEUTRAL"

    def atr_percentile(self, df: pd.DataFrame, window: int = 100) -> float:
        if df.empty or 'true_range' not in df or len(df) < 30:
            return 50.0
        recent = df['true_range'].tail(window).dropna()
        if recent.empty or pd.isna(df['true_range'].iloc[-1]):
            return 50.0
        current = df['true_range'].iloc[-1]
        return float((recent < current).mean() * 100)

# ==================== ردیاب سهمیه‌ی رایگان Groq ====================
class GroqQuotaTracker:
    """
    سقف‌های واقعیِ پلن رایگان Groq برای مدل openai/gpt-oss-120b: ۳۰ درخواست/دقیقه،
    ۱٬۰۰۰ درخواست/روز، ۸٬۰۰۰ توکن/دقیقه، ۲۰۰٬۰۰۰ توکن/روز.

    به‌جای شمارش دستی مصرف، از خودِ هدرهای x-ratelimit-remaining-* که Groq بعد از
    هر پاسخ برمی‌گردونه استفاده می‌کنیم.

    اولویت‌بندی: لایه‌ی قضاوت معامله آستانه‌ی توقف خیلی پایین‌تری داره؛ تنظیم پارامتر
    دوره‌ای با آستانه‌ی محافظه‌کارتر زودتر متوقف می‌شه تا سهمیه برای قضاوت بمونه.
    """
    def __init__(self):
        self.remaining_requests: Optional[int] = None
        self.remaining_tokens: Optional[int] = None
        self.last_updated: Optional[datetime] = None
        self.OPTIMIZER_MIN_REMAINING_REQUESTS = 60
        self.OPTIMIZER_MIN_REMAINING_TOKENS = 3000
        self.JUDGE_MIN_REMAINING_REQUESTS = 5
        self.JUDGE_MIN_REMAINING_TOKENS = 800

    def update_from_headers(self, headers) -> None:
        try:
            if "x-ratelimit-remaining-requests" in headers:
                self.remaining_requests = int(float(headers["x-ratelimit-remaining-requests"]))
            if "x-ratelimit-remaining-tokens" in headers:
                self.remaining_tokens = int(float(headers["x-ratelimit-remaining-tokens"]))
            self.last_updated = datetime.now()
        except (ValueError, TypeError):
            pass

    def can_optimize(self) -> bool:
        if self.remaining_requests is None or self.remaining_tokens is None:
            return True  # هنوز هیچ پاسخی نگرفتیم - خوش‌بینانه اجازه بده
        return (self.remaining_requests > self.OPTIMIZER_MIN_REMAINING_REQUESTS and
                self.remaining_tokens > self.OPTIMIZER_MIN_REMAINING_TOKENS)

    def can_judge(self) -> bool:
        if self.remaining_requests is None or self.remaining_tokens is None:
            return True
        return (self.remaining_requests > self.JUDGE_MIN_REMAINING_REQUESTS and
                self.remaining_tokens > self.JUDGE_MIN_REMAINING_TOKENS)

# ==================== هوش مصنوعی پیشرفته اختصاصی و ضد ضرر ====================
class AIParameterOptimizer:
    def __init__(self, config):
        self.config = config
        self.groq_api_key = config.GROQ_API_KEY
        self.groq_endpoint = "https://api.groq.com/openai/"

        self.blacklist: Dict[str, dict] = {}
        self.BLACKLIST_MIN_COOLDOWN_MINUTES = 20
        self.BLACKLIST_EARLY_RELEASE_PCTL = 70

        self.MAX_RETRIES_429 = 2
        self.MAX_BACKOFF_SECONDS = 8

        # مصرف این بات معمولاً حدود ۵۰۰-۷۰۰ توکن در هر فراخوانیه؛ با سقف ۸٬۰۰۰ توکن/دقیقه
        # عدد پایین‌تری برای درخواست/دقیقه انتخاب شده
        self.GROQ_TARGET_RPM = 10
        self.groq_min_interval_seconds = 60.0 / self.GROQ_TARGET_RPM
        self._last_groq_call_ts = 0.0
        # ردیاب سهمیه‌ی رایگان - بعد از هر پاسخ Groq با هدرهای واقعی خودش به‌روز می‌شه
        self.quota = GroqQuotaTracker()
        # این کالبک از بیرون ست می‌شه تا وقتی سهمیه تموم/برگشت یا اتصال قطع/برقرار شد،
        # پیام تلگرام بفرستیم
        self.on_quota_exhausted_callback: Optional[callable] = None
        self._judge_quota_alert_sent = False
        self._judge_error_alert_sent = False

        # ---- تشخیص قطعی/برقراری اتصال شبکه‌ای به Groq (جدا از تمومشدن سهمیه) ----
        self.CONNECTION_FAILURE_ALERT_THRESHOLD = 3
        self._consecutive_connection_failures = 0
        self._connection_alert_sent = False

        default_params = {
            "rsi_buy_min": 42,
            "rsi_buy_max_range_start": 48,
            "rsi_buy_max_range_end": 65,
            "rsi_sell_max": 58,
            "rsi_sell_min_range_start": 35,
            "rsi_sell_min_range_end": 52,
            "volume_mult": 1.0,
            "atr_min_filter": 0.0015,
            "cooldown_minutes": 90,
            "sl_atr_mult": 1.5,
            "tp1_mult": 1.5,
            "tp2_mult": 2.5,
            "tp3_mult": 4.0,
            "trailing_mult": 1.0,
            "stress_pctl_threshold": 88,
            "stress_size_mult": 0.6
        }

        SYMBOL_PARAM_OVERRIDES = {
            "BTC/USDT":  {"atr_min_filter": 0.0010, "sl_atr_mult": 1.3},
            "ETH/USDT":  {"atr_min_filter": 0.0012, "sl_atr_mult": 1.4},
            "BNB/USDT":  {"atr_min_filter": 0.0012, "sl_atr_mult": 1.4},
            "LTC/USDT":  {"atr_min_filter": 0.0013, "sl_atr_mult": 1.4},
            "SOL/USDT":  {"atr_min_filter": 0.0018, "sl_atr_mult": 1.7, "rsi_buy_max_range_end": 68},
            "AVAX/USDT": {"atr_min_filter": 0.0018, "sl_atr_mult": 1.7},
            "NEAR/USDT": {"atr_min_filter": 0.0018, "sl_atr_mult": 1.7},
            "ADA/USDT":  {"atr_min_filter": 0.0015},
            "XRP/USDT":  {"atr_min_filter": 0.0020, "cooldown_minutes": 110},
            "DOGE/USDT": {"atr_min_filter": 0.0025, "sl_atr_mult": 1.9, "cooldown_minutes": 120},
            "LINK/USDT": {"atr_min_filter": 0.0016, "sl_atr_mult": 1.6},
            "PAXG/USDT": {"atr_min_filter": 0.0008, "sl_atr_mult": 1.2, "tp1_mult": 1.3, "rsi_buy_max_range_end": 62},
        }

        self.symbol_states = {}
        for sym in config.SYMBOLS:
            merged_params = {**default_params, **SYMBOL_PARAM_OVERRIDES.get(sym, {})}
            self.symbol_states[sym] = {
                "last_optimized_time": None,
                "consecutive_losses": 0,
                "params": self.validate_and_clamp_params(merged_params)
            }
        # هر فراخوانی تنظیم پارامتر حدود ۷۰۰ توکن مصرف می‌کنه؛ با فاصله‌ی ۲ ساعت، مصرف این
        # بخش حدود نصف سهمیه‌ی روزانه می‌شه و باقی برای لایه‌ی قضاوت می‌مونه
        self.optimization_interval = timedelta(hours=2)

    def is_blacklisted(self, symbol: str, current_pctl: Optional[float] = None) -> bool:
        if symbol not in self.blacklist:
            return False

        entry = self.blacklist[symbol]
        now = datetime.now()

        if now >= entry["hard_release_at"]:
            del self.blacklist[symbol]
            return False

        if now - entry["blocked_at"] < timedelta(minutes=self.BLACKLIST_MIN_COOLDOWN_MINUTES):
            return True

        if current_pctl is not None and current_pctl < self.BLACKLIST_EARLY_RELEASE_PCTL:
            logger.info(f"سیستم ضد ضرر: {symbol} زودتر از موعد آزاد شد چون نوسان لحظه‌ای به حالت عادی برگشته (پرسنتایل {current_pctl:.0f})")
            del self.blacklist[symbol]
            return False

        return True

    def register_loss(self, symbol: str):
        state = self.symbol_states[symbol]
        state["consecutive_losses"] += 1
        penalty_hours = min(1.5 * state["consecutive_losses"], 6)
        now = datetime.now()
        self.blacklist[symbol] = {
            "blocked_at": now,
            "hard_release_at": now + timedelta(hours=penalty_hours)
        }
        logger.warning(f"سیستم ضد ضرر: ارز {symbol} زیر نظارت رفت - حداکثر تا {penalty_hours:.1f} ساعت مسدود می‌مونه، مگر اینکه زودتر شرایط بازار آروم بشه.")

    def register_win(self, symbol: str):
        state = self.symbol_states[symbol]
        state["consecutive_losses"] = 0
        if symbol in self.blacklist:
            del self.blacklist[symbol]

    def validate_and_clamp_params(self, new_params: dict) -> dict:
        clamped = {}
        clamped["rsi_buy_min"] = max(30, min(float(new_params.get("rsi_buy_min", 42)), 50))
        clamped["rsi_buy_max_range_start"] = max(40, min(float(new_params.get("rsi_buy_max_range_start", 48)), 55))
        clamped["rsi_buy_max_range_end"] = max(55, min(float(new_params.get("rsi_buy_max_range_end", 65)), 75))

        clamped["rsi_sell_max"] = max(50, min(float(new_params.get("rsi_sell_max", 58)), 70))
        clamped["rsi_sell_min_range_start"] = max(25, min(float(new_params.get("rsi_sell_min_range_start", 35)), 45))
        clamped["rsi_sell_min_range_end"] = max(40, min(float(new_params.get("rsi_sell_min_range_end", 52)), 60))

        clamped["volume_mult"] = max(0.7, min(float(new_params.get("volume_mult", 1.0)), 1.6))
        clamped["atr_min_filter"] = max(0.0008, min(float(new_params.get("atr_min_filter", 0.0015)), 0.004))
        clamped["cooldown_minutes"] = max(45, min(int(new_params.get("cooldown_minutes", 90)), 240))

        clamped["sl_atr_mult"] = max(1.2, min(float(new_params.get("sl_atr_mult", 1.5)), 2.5))
        clamped["tp1_mult"] = max(1.2, min(float(new_params.get("tp1_mult", 1.5)), 3.0))
        clamped["tp2_mult"] = max(2.0, min(float(new_params.get("tp2_mult", 2.5)), 5.0))
        clamped["tp3_mult"] = max(3.0, min(float(new_params.get("tp3_mult", 4.0)), 8.0))
        clamped["trailing_mult"] = max(0.8, min(float(new_params.get("trailing_mult", 1.0)), 2.0))

        clamped["stress_pctl_threshold"] = max(80, min(float(new_params.get("stress_pctl_threshold", 88)), 95))
        clamped["stress_size_mult"] = max(0.4, min(float(new_params.get("stress_size_mult", 0.6)), 0.85))
        return clamped

    def _wait_for_groq_slot(self):
        elapsed = time.time() - self._last_groq_call_ts
        remaining = self.groq_min_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _register_connection_result(self, success: bool, label: str):
        """
        ثبت نتیجه‌ی «اتصال شبکه‌ای» واقعی به Groq (نه وضعیت HTTP مثل 429). بعد از چند
        شکست پیاپی هشدار «قطع اتصال» و به‌محض اولین موفقیت بعد از اون، پیام «اتصال
        برقرار شد» به تلگرام فرستاده می‌شه.
        """
        if success:
            self._consecutive_connection_failures = 0
            if self._connection_alert_sent:
                self._connection_alert_sent = False
                if self.on_quota_exhausted_callback:
                    try:
                        self.on_quota_exhausted_callback(
                            "✅ اتصال به هوش مصنوعی (Groq) دوباره برقرار شد - "
                            "لایه‌ی قضاوت هوشمند معامله و تنظیم پارامتر دوره‌ای به حالت عادی برگشتن."
                        )
                    except Exception:
                        pass
        else:
            self._consecutive_connection_failures += 1
            if (self._consecutive_connection_failures >= self.CONNECTION_FAILURE_ALERT_THRESHOLD
                    and not self._connection_alert_sent):
                self._connection_alert_sent = True
                if self.on_quota_exhausted_callback:
                    try:
                        self.on_quota_exhausted_callback(
                            f"⚠️ اتصال به هوش مصنوعی (Groq) قطع شده (آخرین نماد: {label}).\n\n"
                            "طبق تنظیم فعلی، تا برقراری دوباره‌ی اتصال:\n"
                            "• هیچ سیگنال یا معامله‌ی جدیدی ارسال نمی‌شه (تایید AI الزامیه)\n"
                            "• تنظیم پارامتر دوره‌ای متوقفه (آخرین پارامترهای تنظیم‌شده توسط AI همچنان استفاده می‌شن)"
                        )
                    except Exception:
                        pass

    def _post_with_retry(self, url: str, payload: dict, headers: dict, timeout: int, label: str) -> Optional[requests.Response]:
        for attempt in range(self.MAX_RETRIES_429 + 1):
            self._wait_for_groq_slot()
            try:
                response = requests.post(url, json=payload, headers=headers, timeout=timeout)
            except requests.exceptions.RequestException:
                self._register_connection_result(False, label)
                raise
            finally:
                self._last_groq_call_ts = time.time()

            self._register_connection_result(True, label)
            self.quota.update_from_headers(response.headers)

            if response.status_code != 429:
                return response

            if attempt >= self.MAX_RETRIES_429:
                return response

            retry_after = response.headers.get("Retry-After") or response.headers.get("retry-after")
            try:
                wait_seconds = float(retry_after) if retry_after is not None else 2 * (attempt + 1)
            except ValueError:
                wait_seconds = 2 * (attempt + 1)
            wait_seconds = min(wait_seconds, self.MAX_BACKOFF_SECONDS)

            logger.warning(f"Groq API برای {label} پاسخ 429 داد؛ {wait_seconds:.1f} ثانیه صبر و تلاش مجدد ({attempt + 1}/{self.MAX_RETRIES_429})...")
            time.sleep(wait_seconds)

        return response

    def should_optimize(self, symbol: str) -> bool:
        if not self.quota.can_optimize():
            logger.info(f"{symbol}: تنظیم پارامتر دوره‌ای به‌خاطر کمبود سهمیه‌ی Groq این چرخه رد شد "
                        f"(باقیمانده: {self.quota.remaining_requests} درخواست / {self.quota.remaining_tokens} توکن) - "
                        f"سهمیه برای لایه‌ی قضاوت معامله نگه داشته می‌شه.")
            return False
        state = self.symbol_states[symbol]
        if state["last_optimized_time"] is None:
            return True
        return datetime.now() - state["last_optimized_time"] >= self.optimization_interval

    def optimize_symbol_parameters(self, symbol: str, df_15m: pd.DataFrame, journal=None):
        if not self.groq_api_key or df_15m.empty:
            return

        state = self.symbol_states[symbol]
        latest = df_15m.iloc[-1]

        market_metrics = {
            "symbol": symbol,
            "close_price": float(latest['close']),
            "rsi": float(latest['rsi']) if not pd.isna(latest['rsi']) else 50,
            "atr_volatility": float(latest['atr']) if not pd.isna(latest['atr']) else 0,
            "current_volume": float(latest['volume']) if not pd.isna(latest['volume']) else 0,
            "volume_sma": float(latest['vol_sma']) if not pd.isna(latest['vol_sma']) else 0,
            "support": float(latest['support']) if not pd.isna(latest['support']) else 0,
            "resistance": float(latest['resistance']) if not pd.isna(latest['resistance']) else 0,
            "consecutive_losses": state["consecutive_losses"]
        }

        side_perf_note = ""
        if journal is not None:
            buy_perf = journal.get_side_performance(symbol, "BUY")
            sell_perf = journal.get_side_performance(symbol, "SELL")
            market_metrics["recent_buy_performance"] = buy_perf
            market_metrics["recent_sell_performance"] = sell_perf
            side_perf_note = """
IMPORTANT - side-specific tuning guidance:
"recent_buy_performance" and "recent_sell_performance" show this symbol's last trades broken down by side (win_rate, avg_r, count; null/0 means not enough data yet - ignore in that case).
If BUY has recently underperformed while SELL has not (or vice versa), prefer adjusting the side-specific keys (rsi_buy_min, rsi_buy_max_range_start, rsi_buy_max_range_end for BUY; rsi_sell_max, rsi_sell_min_range_start, rsi_sell_min_range_end for SELL) rather than the shared risk keys (sl_atr_mult, tp1_mult, tp2_mult, tp3_mult, atr_min_filter, volume_mult, cooldown_minutes, trailing_mult), since those shared keys affect both sides and unnecessarily reducing them would also cut down the healthy side's signal frequency.
Never make changes so aggressive that they would effectively stop signals from being generated at all - stay within reasonable, moderate adjustments.
"""

        prompt = f"""
You are an advanced quantitative trading AI. First, analyze the following key market data and indicators for asset {symbol}:
{json.dumps(market_metrics)}
{side_perf_note}
Based on these specific conditions, dynamically tune the trading parameters to adapt to the current market regime.
Keep risk management strict to prevent losses, but allow reasonable flexibility so the bot can capture valid opportunities within safe logical boundaries.
Return ONLY valid JSON with the exact same keys as these default parameters:
{json.dumps(state["params"])}
No markdown formatting, no extra text.
"""

        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": "openai/gpt-oss-120b",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "reasoning_effort": "low",
            "max_tokens": 600
        }

        try:
            response = self._post_with_retry(f"{self.groq_endpoint}v1/chat/completions", payload, headers, timeout=25, label=symbol)
            if response.status_code == 200:
                res_data = response.json()
                content = res_data['choices'][0]['message']['content'].strip()

                if content.startswith("```"):
                    content = content.strip("`").replace("json\n", "").strip()

                if not content:
                    raise ValueError("Groq یه پاسخ خالی برگردوند (احتمالاً توکن‌های reasoning تمام سقف max_tokens رو مصرف کردن)")

                raw_params = json.loads(content)
                state["params"] = self.validate_and_clamp_params(raw_params)
                state["last_optimized_time"] = datetime.now()
                logger.info(f"پارامترهای ضد ضرر و پویای {symbol} بر اساس داده‌های روز بروزرسانی شد.")
            else:
                logger.warning(f"Groq API برای {symbol} پاسخ {response.status_code} داد؛ پارامترهای قبلی حفظ شدن.")
        except Exception as e:
            logger.error(f"خطا در بهینه‌سازی هوش مصنوعی برای {symbol}: {e}")

    def get_params(self, symbol: str) -> dict:
        return self.symbol_states[symbol]["params"]

    def _alert_judge_error(self, message: str):
        """هشدار خطای عمومی لایه‌ی قضاوت (پاسخ 200 اما غیرمنتظره/غیر-JSON، یا هر خطای
        دیگری که مربوط به تمومشدن سهمیه یا قطعی شبکه نیست)."""
        if not self._judge_error_alert_sent:
            self._judge_error_alert_sent = True
            if self.on_quota_exhausted_callback:
                try:
                    self.on_quota_exhausted_callback(f"⚠️ {message}")
                except Exception:
                    pass

    def _reset_judge_error_alert(self):
        if self._judge_error_alert_sent:
            self._judge_error_alert_sent = False
            if self.on_quota_exhausted_callback:
                try:
                    self.on_quota_exhausted_callback("✅ لایه‌ی قضاوت هوشمند معامله دوباره پاسخ سالم از Groq دریافت کرد و به حالت عادی برگشت.")
                except Exception:
                    pass

    def evaluate_trade_candidate(self, symbol: str, side: str, context: dict) -> Dict:
        # لایه‌ی قضاوت AI الزامیه. اگه به هر دلیلی (نبود کلید، تمومشدن سهمیه، قطعی شبکه،
        # پاسخ نامعتبر) به AI دسترسی نباشه، سیگنال رد می‌شه.
        default = {"approve": False, "confidence": 0, "reason": "بدون دسترسی به AI - طبق تنظیم، سیگنال رد شد (تایید AI الزامیه)"}
        if not self.groq_api_key:
            return default

        if self.quota.can_judge():
            if self._judge_quota_alert_sent:
                self._judge_quota_alert_sent = False
                if self.on_quota_exhausted_callback:
                    try:
                        self.on_quota_exhausted_callback(
                            "✅ سهمیه‌ی رایگان Groq دوباره در دسترسه - لایه‌ی قضاوت هوشمند معامله و تنظیم پارامتر دوره‌ای به حالت عادی برگشتن."
                        )
                    except Exception:
                        pass
        else:
            if not self._judge_quota_alert_sent:
                self._judge_quota_alert_sent = True
                if self.on_quota_exhausted_callback:
                    try:
                        self.on_quota_exhausted_callback(
                            "⚠️ سهمیه‌ی رایگان Groq برای امروز تموم شد.\n\n"
                            "طبق تنظیم فعلی، تا برگشتن سهمیه:\n"
                            "• هیچ سیگنال یا معامله‌ی جدیدی ارسال نمی‌شه (تایید AI الزامیه)\n"
                            "• تنظیم پارامتر دوره‌ای متوقفه (آخرین پارامترهای تنظیم‌شده توسط AI همچنان استفاده می‌شن)"
                        )
                    except Exception:
                        pass
            logger.warning(f"{symbol}: سهمیه‌ی رایگان Groq تموم شده - طبق تنظیم، سیگنال رد می‌شه (تایید AI الزامیه).")
            return {"approve": False, "confidence": 0,
                    "reason": "سهمیه‌ی رایگان Groq تموم شده - طبق تنظیم، سیگنال رد شد (تایید AI الزامیه)"}

        prompt = f"""You are a veteran discretionary crypto trader with 15+ years of experience. You deeply understand that markets are not static: regimes shift, correlations break down, momentum exhausts, and no fixed rule set can fully capture that. You are reviewing a trade candidate that ALREADY passed a strict quantitative multi-factor scoring system (trend, RSI momentum, MACD, volume, market structure, multi-timeframe alignment, volatility regime).

Your only job now is the kind of contextual judgment an elite human trader adds on top of a systematic setup: given everything below, does the broader picture actually support taking this trade right now, or is there something about the current context (exhaustion, conflicting signals, thin/erratic volume, the symbol's recent losing streak, over-extension) that says skip it even though the numbers look fine?

Trade candidate:
Symbol: {symbol}
Side: {side}
Full context: {json.dumps(context, ensure_ascii=False)}

Respond ONLY with valid JSON, no markdown, no extra text, in exactly this shape:
{{"approve": true or false, "confidence": integer 0-100, "reason": "one concise sentence in Persian explaining the judgment"}}
"""
        headers = {"Authorization": f"Bearer {self.groq_api_key}", "Content-Type": "application/json"}
        payload = {
            "model": "openai/gpt-oss-120b",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "reasoning_effort": "low",
            "max_tokens": 500
        }
        try:
            response = self._post_with_retry(f"{self.groq_endpoint}v1/chat/completions", payload, headers, timeout=20, label=symbol)
            if response.status_code != 200:
                logger.warning(f"لایه‌ی قضاوت AI برای {symbol} پاسخ {response.status_code} داد؛ به تصمیم کمی اکتفا می‌شه.")
                self._alert_judge_error(
                    f"لایه‌ی قضاوت هوشمند معامله پاسخ غیرمنتظره {response.status_code} از Groq دریافت کرد (نماد: {symbol}).\n\n"
                    "طبق تنظیم فعلی، تا رفع این مشکل هیچ سیگنال یا معامله‌ی جدیدی ارسال نمی‌شه (تایید AI الزامیه)."
                )
                return default
            content = response.json()['choices'][0]['message']['content'].strip()
            if content.startswith("```"):
                content = content.strip("`").replace("json\n", "").strip()
            if not content:
                raise ValueError("Groq یه پاسخ خالی برگردوند (احتمالاً توکن‌های reasoning تمام سقف max_tokens رو مصرف کردن)")
            result = json.loads(content)
            self._reset_judge_error_alert()
            return {
                "approve": bool(result.get("approve", True)),
                "confidence": int(result.get("confidence", 50)),
                "reason": str(result.get("reason", ""))[:300]
            }
        except Exception as e:
            logger.error(f"خطا در لایه‌ی قضاوت AI برای {symbol}: {e}")
            self._alert_judge_error(
                f"لایه‌ی قضاوت هوشمند معامله برای نماد {symbol} با خطا مواجه شد: {e}\n\n"
                "طبق تنظیم فعلی، تا رفع این مشکل هیچ سیگنال یا معامله‌ی جدیدی ارسال نمی‌شه (تایید AI الزامیه)."
            )
            return default

# ==================== موتور سیگنال امتیازی چندلایه ====================
class SignalEngine:
    def __init__(self, config: Config, ai_optimizer: AIParameterOptimizer, analysis: AnalysisLayer):
        self.config = config
        self.ai_optimizer = ai_optimizer
        self.analysis = analysis

    def _score_buy(self, latest, prev, p) -> float:
        score = 0.0
        if latest['ema_fast'] > latest['ema_slow']:
            score += 2.0
        rsi_cross = latest['rsi'] > p["rsi_buy_min"] and prev['rsi'] <= p["rsi_buy_min"]
        rsi_zone = p["rsi_buy_max_range_start"] <= latest['rsi'] <= p["rsi_buy_max_range_end"] and latest['rsi'] > prev['rsi']
        if rsi_cross:
            score += 2.5
        elif rsi_zone:
            score += 1.5
        if not pd.isna(latest.get('macd_hist', float('nan'))):
            if latest['macd_hist'] > 0:
                score += 1.0
            elif latest['macd_hist'] > prev.get('macd_hist', 0):
                score += 0.5
        vol_ratio = latest['volume'] / latest['vol_sma'] if latest['vol_sma'] else 0
        if vol_ratio >= p["volume_mult"]:
            score += 1.5
        elif vol_ratio >= p["volume_mult"] * 0.8:
            score += 0.75
        if latest['support'] > 0 and (latest['close'] - latest['support']) / latest['close'] < 0.02:
            score += 0.75
        return score

    def _score_sell(self, latest, prev, p) -> float:
        score = 0.0
        if latest['ema_fast'] < latest['ema_slow']:
            score += 2.0
        rsi_cross = latest['rsi'] < p["rsi_sell_max"] and prev['rsi'] >= p["rsi_sell_max"]
        rsi_zone = p["rsi_sell_min_range_start"] <= latest['rsi'] <= p["rsi_sell_min_range_end"] and latest['rsi'] < prev['rsi']
        if rsi_cross:
            score += 2.5
        elif rsi_zone:
            score += 1.5
        if not pd.isna(latest.get('macd_hist', float('nan'))):
            if latest['macd_hist'] < 0:
                score += 1.0
            elif latest['macd_hist'] < prev.get('macd_hist', 0):
                score += 0.5
        vol_ratio = latest['volume'] / latest['vol_sma'] if latest['vol_sma'] else 0
        if vol_ratio >= p["volume_mult"]:
            score += 1.5
        elif vol_ratio >= p["volume_mult"] * 0.8:
            score += 0.75
        if latest['resistance'] > 0 and (latest['resistance'] - latest['close']) / latest['close'] < 0.02:
            score += 0.75
        return score

    def get_rule_signal(self, symbol: str, df_15m: pd.DataFrame, df_1h: pd.DataFrame, trend_4h: str,
                         macro_context: Optional[dict] = None, peer_context: Optional[dict] = None) -> Tuple[Optional[str], dict]:
        if df_15m.empty or len(df_15m) < 30:
            return None, {}

        pctl = self.analysis.atr_percentile(df_15m)

        if self.ai_optimizer.is_blacklisted(symbol, current_pctl=pctl):
            return None, {}
        if not self.analysis.is_market_tradable(df_15m):
            return None, {}

        latest = df_15m.iloc[-1]
        prev = df_15m.iloc[-2]
        p = self.ai_optimizer.get_params(symbol)

        if pd.isna(latest['rsi']) or pd.isna(latest['ema_fast']) or pd.isna(latest['atr']):
            return None, {}
        if latest['atr'] < (latest['close'] * p["atr_min_filter"]):
            return None, {}

        macro_context = macro_context or {}
        fng = macro_context.get("fear_greed")
        btc_trend_4h = macro_context.get("btc_trend_4h")
        btc_structure = macro_context.get("btc_structure")
        funding_rate = macro_context.get("funding_rate")
        spread_pct = macro_context.get("spread_pct")
        btc_rsi = macro_context.get("btc_rsi")
        btc_macd_hist = macro_context.get("btc_macd_hist")
        btc_aligned_now = symbol != "BTC/USDT" and bool(btc_trend_4h) and btc_trend_4h == trend_4h and btc_trend_4h != "NEUTRAL"

        if spread_pct is not None and spread_pct > 0.8:
            logger.info(f"{symbol}: اسپرد لحظه‌ای غیرعادی ({spread_pct:.2f}%) - سیگنال رد شد")
            return None, {}

        if pctl > self.config.ATR_PERCENTILE_MAX:
            logger.info(f"{symbol}: نوسان غیرعادی (پرسنتایل {pctl:.0f}) - رد شد")
            return None, {}

        structure = self.analysis.market_structure(df_15m)

        # ---- فقط پوزیشن Long/BUY (اسپات): امتیاز فروش/شورت از مسیر تصمیم‌گیری حذف شد ----
        buy_score = self._score_buy(latest, prev, p) if trend_4h in ["BULLISH", "NEUTRAL"] else 0.0

        if trend_4h == "BULLISH":
            buy_score += 1.0

        if structure == "BULLISH":
            buy_score += 1.5

        if buy_score > 0 and self.analysis.is_mtf_aligned(df_1h, "BUY"):
            buy_score += 1.5

        if symbol != "BTC/USDT" and btc_trend_4h and btc_structure:
            if btc_trend_4h == "BEARISH" and btc_structure == "BEARISH":
                buy_score -= 1.0
            elif btc_trend_4h == "BULLISH" and btc_structure == "BULLISH":
                buy_score += 1.0

        if fng is not None:
            if fng <= 20:
                buy_score += 0.5
            elif fng >= 80:
                buy_score -= 0.5

        if funding_rate is not None:
            if funding_rate > 0.0005:
                buy_score -= 0.5
            elif funding_rate < -0.0005:
                buy_score += 0.5

        if btc_aligned_now and btc_rsi is not None:
            if btc_trend_4h == "BULLISH":
                if 50 <= btc_rsi <= 68:
                    buy_score += 0.3
                elif btc_rsi > 75:
                    buy_score -= 0.3

        if btc_aligned_now and btc_macd_hist is not None:
            if btc_trend_4h == "BULLISH" and btc_macd_hist > 0:
                buy_score += 0.2

        btc_reference_trade = macro_context.get("btc_reference_trade")
        if symbol not in ("BTC/USDT", "PAXG/USDT") and btc_aligned_now and btc_reference_trade:
            ref_side = btc_reference_trade.get("side")
            ref_health = btc_reference_trade.get("health", 0) or 0
            if ref_side == "BUY":
                if ref_health > 0.4:
                    buy_score += 0.5
                elif ref_health < -0.4:
                    buy_score -= 0.5

        threshold = self.config.MIN_SIGNAL_SCORE
        vol_ratio = latest['volume'] / latest['vol_sma'] if latest['vol_sma'] else 0
        diagnostics = {
            "structure": structure,
            "trend_4h": trend_4h,
            "rsi": round(float(latest['rsi']), 1),
            "macd_hist": round(float(latest['macd_hist']), 6) if not pd.isna(latest.get('macd_hist', float('nan'))) else None,
            "volume_vs_avg_ratio": round(float(vol_ratio), 2),
            "atr_percentile_100candles": round(pctl, 1),
            "mtf_1h_aligned": self.analysis.is_mtf_aligned(df_1h, "BUY"),
            "consecutive_losses_this_symbol": self.ai_optimizer.symbol_states[symbol]["consecutive_losses"],
            "fear_greed_index": fng,
            "btc_macro_trend_4h": btc_trend_4h,
            "btc_macro_structure": btc_structure,
            "funding_rate": funding_rate,
            "spread_pct": round(spread_pct, 3) if spread_pct is not None else None,
            "btc_reference_trade": btc_reference_trade,
            "btc_aligned_now": btc_aligned_now,
            "btc_rsi": round(btc_rsi, 1) if btc_rsi is not None else None,
            "btc_macd_hist": round(btc_macd_hist, 6) if btc_macd_hist is not None else None,
        }
        macro_log = (f"FNG={fng} BTC_trend={btc_trend_4h}/{btc_structure} BTC_RSI={diagnostics['btc_rsi']} "
                     f"BTC_MACD={diagnostics['btc_macd_hist']} funding={funding_rate} "
                     f"spread={diagnostics['spread_pct']} btc_ref={btc_reference_trade} aligned={btc_aligned_now}")
        if buy_score >= threshold:
            diagnostics["quant_score"] = round(buy_score, 2)
            logger.info(f"{symbol}: امتیاز خرید {buy_score:.2f} (آستانه {threshold}) | ساختار: {structure} | {macro_log}")
            return "BUY", diagnostics

        return None, {}

    def calculate_trade_levels(self, symbol: str, side: str, latest: pd.Series, macro_context: Optional[dict] = None) -> dict:
        """محاسبه‌ی قیمت ورود، حد ضرر، تارگت‌ها و پیشنهاد حجم - همان منطق کد مرجع (send_signal)."""
        price = float(latest['close'])
        atr = float(latest['atr']) if not pd.isna(latest['atr']) else price * 0.01

        p = self.ai_optimizer.get_params(symbol)

        if side == "BUY":
            stop_loss = min(float(latest['support']), price - (p["sl_atr_mult"] * atr))
            risk = price - stop_loss
            tp1 = round(price + (p["tp1_mult"] * risk), 4)
            tp2 = round(price + (p["tp2_mult"] * risk), 4)
            tp3 = round(price + (p["tp3_mult"] * risk), 4)
            stop_loss = round(stop_loss, 4)
        else:
            stop_loss = max(float(latest['resistance']), price + (p["sl_atr_mult"] * atr))
            risk = stop_loss - price
            tp1 = round(price - (p["tp1_mult"] * risk), 4)
            tp2 = round(price - (p["tp2_mult"] * risk), 4)
            tp3 = round(price - (p["tp3_mult"] * risk), 4)
            stop_loss = round(stop_loss, 4)

        btc_volatility_pctl = (macro_context or {}).get("btc_volatility_pctl")
        stress_active = btc_volatility_pctl is not None and btc_volatility_pctl >= p["stress_pctl_threshold"]
        size_mult = p["stress_size_mult"] if stress_active else 1.0

        risk_amount = self.config.VIRTUAL_CAPITAL_USDT * (self.config.RISK_PER_TRADE_PCT / 100) * size_mult
        risk_per_unit = abs(price - stop_loss)
        qty = (risk_amount / risk_per_unit) if risk_per_unit > 0 else 0.0

        return {
            "price": price, "tp1": tp1, "tp2": tp2, "tp3": tp3, "sl": stop_loss,
            "qty": qty, "notional": qty * price, "atr": atr,
            "rr_ratio": p["tp1_mult"],
            "stress_active": stress_active, "size_mult": size_mult,
            "btc_volatility_pctl": btc_volatility_pctl
        }

# ==================== ماتریس همبستگی داینامیک ====================
class CorrelationManager:
    """
    جایگزین گروه‌بندی ثابت SYMBOL_GROUPS برای تصمیم اکسپوژر همبسته. هر
    CORRELATION_REFRESH_HOURS یک‌بار، همبستگی بازدهی نمادها روی تایم‌فریم ورودی
    بازمحاسبه می‌شه. آستانه‌ی همبستگی بین MIN (در استرس بازار) تا MAX (در آرامش بازار)
    بر اساس پرسنتایل نوسان لحظه‌ای بیت‌کوین حرکت می‌کنه. بدون فراخوانی اضافه‌ی Groq.
    در نبود داده‌ی کافی، به گروه‌بندی ثابت برمی‌گرده.
    """
    def __init__(self, config: Config, data_layer):
        self.config = config
        self.data = data_layer
        self.matrix: Optional[pd.DataFrame] = None
        self.last_computed: Optional[datetime] = None

    def should_refresh(self) -> bool:
        if self.matrix is None or self.last_computed is None:
            return True
        return datetime.now() - self.last_computed >= timedelta(hours=self.config.CORRELATION_REFRESH_HOURS)

    def refresh(self):
        closes = {}
        for symbol in self.config.SYMBOLS:
            df = self.data.fetch_ohlcv_df(symbol, self.config.ENTRY_TIMEFRAME, limit=self.config.CORRELATION_LOOKBACK_CANDLES)
            if df.empty or len(df) < 30:
                continue
            closes[symbol] = df.set_index('timestamp')['close']
        if len(closes) < 2:
            logger.warning("ماتریس همبستگی: داده‌ی کافی برای حداقل ۲ نماد در دسترس نبود - این چرخه رد شد.")
            return
        try:
            price_df = pd.DataFrame(closes).dropna()
            if len(price_df) < 20:
                return
            returns = price_df.pct_change().dropna()
            self.matrix = returns.corr()
            self.last_computed = datetime.now()
            logger.info("ماتریس همبستگی داینامیک بازمحاسبه شد.")
        except Exception as e:
            logger.error(f"خطا در محاسبه‌ی ماتریس همبستگی: {e}")

    def get_dynamic_threshold(self, btc_volatility_pctl: Optional[float]) -> float:
        lo, hi = self.config.CORRELATION_THRESHOLD_MIN, self.config.CORRELATION_THRESHOLD_MAX
        if btc_volatility_pctl is None:
            return (lo + hi) / 2
        frac = max(0.0, min(1.0, btc_volatility_pctl / 100))
        return hi - frac * (hi - lo)

    def correlated_count(self, symbol: str, active_trades: Dict, threshold: float) -> int:
        if self.matrix is None or symbol not in self.matrix:
            group = self.config.SYMBOL_GROUPS.get(symbol, "other")
            return sum(1 for t in active_trades.values() if self.config.SYMBOL_GROUPS.get(t['symbol'], "other") == group)
        count = 0
        for t in active_trades.values():
            other = t['symbol']
            if other == symbol:
                continue
            if other in self.matrix.index:
                corr = self.matrix.loc[symbol, other]
                if pd.notna(corr) and abs(corr) >= threshold:
                    count += 1
        return count

# ==================== مدیریت ریسک و سرمایه ====================
class RiskManager:
    def __init__(self, config: Config):
        self.config = config

    def calculate_position_size(self, entry: float, stop: float, size_mult: float = 1.0) -> float:
        risk_amount = self.config.VIRTUAL_CAPITAL_USDT * (self.config.RISK_PER_TRADE_PCT / 100) * size_mult
        risk_per_unit = abs(entry - stop)
        if risk_per_unit <= 0:
            return 0.0
        return risk_amount / risk_per_unit

    def can_open_trade(self, symbol: str, active_trades: Dict, correlation_manager: Optional["CorrelationManager"] = None,
                        btc_volatility_pctl: Optional[float] = None) -> Tuple[bool, str]:
        if correlation_manager is not None:
            threshold = correlation_manager.get_dynamic_threshold(btc_volatility_pctl)
            corr_count = correlation_manager.correlated_count(symbol, active_trades, threshold)
            if corr_count >= self.config.MAX_CORRELATED_TRADES:
                return False, f"به سقف اکسپوژر همبسته رسیدیم (آستانه‌ی لحظه‌ای {threshold:.2f}، ریسک همبستگی)"
        else:
            group = self.config.SYMBOL_GROUPS.get(symbol, "other")
            group_count = sum(
                1 for t in active_trades.values()
                if self.config.SYMBOL_GROUPS.get(t['symbol'], "other") == group
            )
            if group_count >= self.config.MAX_TRADES_PER_GROUP:
                return False, f"به سقف اکسپوژر گروه {group} رسیدیم (ریسک همبستگی)"

        return True, ""

    def kill_switch_triggered(self, journal: "TradeJournal") -> bool:
        today_pnl = journal.get_today_realized_pnl_usdt()
        loss_limit = -(self.config.VIRTUAL_CAPITAL_USDT * self.config.MAX_DAILY_LOSS_PCT / 100)
        return today_pnl <= loss_limit

# ==================== ژورنال معاملات ====================
class TradeJournal:
    def __init__(self, path: str = "trade_history.json"):
        self.path = path
        self.records: List[Dict] = self._load()

    def _load(self) -> List[Dict]:
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return []
        return []

    def _save(self):
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.records, f, indent=2)
        except Exception as e:
            logger.error(f"خطا در ذخیره ژورنال معاملات: {e}")

    def record(self, symbol: str, side: str, reason: str, pnl_usdt: float, pnl_pct: float, r_multiple: float, closed_pct: float):
        self.records.append({
            "date": date_cls.today().isoformat(),
            "timestamp": datetime.now().isoformat(),
            "symbol": symbol,
            "side": side,
            "reason": reason,
            "pnl_usdt": round(pnl_usdt, 2),
            "pnl_pct": round(pnl_pct, 3),
            "r_multiple": round(r_multiple, 2),
            "closed_pct": closed_pct
        })
        self._save()

    def get_today_realized_pnl_usdt(self) -> float:
        today = date_cls.today().isoformat()
        return sum(r["pnl_usdt"] for r in self.records if r["date"] == today)

    def get_side_performance(self, symbol: str, side: str, lookback: int = 15) -> Dict:
        side_records = [r for r in self.records if r["symbol"] == symbol and r["side"] == side]
        if not side_records:
            return {"count": 0, "win_rate": None, "avg_r": None, "total_pnl_usdt": 0.0}
        recent = side_records[-lookback:]
        wins = [r for r in recent if r["pnl_usdt"] > 0]
        return {
            "count": len(recent),
            "win_rate": round((len(wins) / len(recent)) * 100, 1),
            "avg_r": round(sum(r["r_multiple"] for r in recent) / len(recent), 2),
            "total_pnl_usdt": round(sum(r["pnl_usdt"] for r in recent), 2)
        }

    def get_side_stats_for_date(self, for_date: str) -> Dict[str, Dict]:
        day_records = [r for r in self.records if r["date"] == for_date]
        result = {}
        for side in ["BUY", "SELL"]:
            side_recs = [r for r in day_records if r["side"] == side]
            if not side_recs:
                result[side] = {"count": 0, "win_rate": 0.0, "avg_r": 0.0, "total_pnl": 0.0}
                continue
            wins = [r for r in side_recs if r["pnl_usdt"] > 0]
            result[side] = {
                "count": len(side_recs),
                "win_rate": round((len(wins) / len(side_recs)) * 100, 1),
                "avg_r": round(sum(r["r_multiple"] for r in side_recs) / len(side_recs), 2),
                "total_pnl": round(sum(r["pnl_usdt"] for r in side_recs), 2)
            }
        return result

    def build_daily_summary(self, for_date: str) -> Optional[str]:
        day_records = [r for r in self.records if r["date"] == for_date]
        if not day_records:
            return None
        total_pnl = sum(r["pnl_usdt"] for r in day_records)
        wins = [r for r in day_records if r["pnl_usdt"] > 0]
        losses = [r for r in day_records if r["pnl_usdt"] <= 0]
        win_rate = (len(wins) / len(day_records)) * 100 if day_records else 0
        avg_r = sum(r["r_multiple"] for r in day_records) / len(day_records) if day_records else 0

        side_stats = self.get_side_stats_for_date(for_date)
        buy_s, sell_s = side_stats["BUY"], side_stats["SELL"]

        return f"""
📊 **گزارش عملکرد روزانه ({for_date})**

🔢 تعداد رخدادهای بسته‌شده: {len(day_records)}
✅ برد: {len(wins)} | ❌ باخت: {len(losses)}
🎯 نرخ برد کل: {win_rate:.1f}%
📈 سود/زیان کل: {total_pnl:+.2f} USDT
📐 میانگین R به‌ازای هر رخداد: {avg_r:+.2f}R

🟢 **BUY (Long):** {buy_s['count']} رخداد | نرخ برد {buy_s['win_rate']:.1f}% | میانگین R {buy_s['avg_r']:+.2f} | PnL {buy_s['total_pnl']:+.2f} USDT
🔴 **SELL (Short):** {sell_s['count']} رخداد | نرخ برد {sell_s['win_rate']:.1f}% | میانگین R {sell_s['avg_r']:+.2f} | PnL {sell_s['total_pnl']:+.2f} USDT
"""

# ==================== ماژول معامله مجازی: حجم واقعی، پله‌ای، تریلینگ واقعی ====================
class PaperTrader:
    def __init__(self, config: Config, ai_optimizer: AIParameterOptimizer, journal: TradeJournal):
        self.config = config
        self.ai_optimizer = ai_optimizer
        self.journal = journal
        self.file_path = "paper_trades.json"
        self.active_trades = self._load_trades()
        # دو ترد (چرخه‌ی اصلی + مانیتورینگ لحظه‌ای) هم‌زمان به active_trades دسترسی دارن
        self.lock = threading.Lock()

    def _load_trades(self) -> Dict:
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_trades(self):
        try:
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(self.active_trades, f, indent=4)
        except Exception as e:
            logger.error(f"خطا در ذخیره معاملات مجازی: {e}")

    def snapshot_active_trades(self) -> Dict:
        with self.lock:
            return dict(self.active_trades)

    def get_reference_trade_health(self, reference_symbol: str, recent_hours: float = 3.0) -> Optional[Dict]:
        with self.lock:
            open_trades = [t for t in self.active_trades.values() if t['symbol'] == reference_symbol]
        if open_trades:
            trade = sorted(open_trades, key=lambda t: t['open_time'])[-1]
            side = trade['side']
            entry = trade['entry']
            original_sl = trade['original_sl']
            risk = abs(entry - original_sl)
            if risk <= 0:
                return None
            if trade.get('tp1_hit'):
                return {"side": side, "health": 1.0}
            if side == "BUY":
                favorable = trade['highest_since_entry'] - entry
                unfavorable = entry - trade['lowest_since_entry']
            else:
                favorable = entry - trade['lowest_since_entry']
                unfavorable = trade['highest_since_entry'] - entry
            health = (favorable - unfavorable) / risk
            return {"side": side, "health": max(-1.0, min(1.0, health))}

        recent_records = [
            r for r in self.journal.records
            if r["symbol"] == reference_symbol
            and datetime.now() - datetime.fromisoformat(r["timestamp"]) <= timedelta(hours=recent_hours)
        ]
        if not recent_records:
            return None
        last = sorted(recent_records, key=lambda r: r["timestamp"])[-1]
        return {"side": last["side"], "health": 1.0 if last["pnl_usdt"] > 0 else -1.0}

    def open_virtual_trade(self, symbol: str, side: str, entry_price: float, tp1: float, tp2: float, tp3: float,
                            sl: float, qty: float, atr_at_entry: float, spread_pct_at_entry: Optional[float] = None):
        trade_id = f"{symbol}_{int(time.time())}"
        with self.lock:
            self.active_trades[trade_id] = {
                "symbol": symbol,
                "side": side,
                "entry": entry_price,
                "tp1": tp1, "tp2": tp2, "tp3": tp3,
                "sl": sl,
                "original_sl": sl,
                "qty": qty,
                "atr_at_entry": atr_at_entry,
                "spread_pct_at_entry": spread_pct_at_entry,
                "remaining_pct": 100,
                "tp1_hit": False,
                "tp2_hit": False,
                "highest_since_entry": entry_price,
                "lowest_since_entry": entry_price,
                "open_time": datetime.now().strftime('%Y-%m-%d %H:%M')
            }
            self._save_trades()

    def _close_partial(self, trade: Dict, exit_price: float, reason: str, closed_pct: float, register_result: bool = True):
        side = trade['side']
        entry = trade['entry']
        price_diff = (exit_price - entry) if side == "BUY" else (entry - exit_price)

        # مدل‌سازی کارمزد + اسلیپیج (نصف اسپرد زمان ورود برای هر طرف)
        spread_estimate_pct = trade.get('spread_pct_at_entry')
        if spread_estimate_pct is None:
            spread_estimate_pct = self.config.DEFAULT_SPREAD_PCT_FALLBACK
        cost_pct_per_side = (self.config.EXCHANGE_TAKER_FEE_PCT + (spread_estimate_pct / 2)) / 100
        execution_cost_per_unit = (entry + exit_price) * cost_pct_per_side
        price_diff -= execution_cost_per_unit

        risk_per_unit = abs(entry - trade['original_sl'])
        r_multiple = (price_diff / risk_per_unit) if risk_per_unit > 0 else 0.0
        qty_closed = trade['qty'] * (closed_pct / 100)
        pnl_usdt = price_diff * qty_closed
        pnl_pct = (price_diff / entry) * 100

        if register_result:
            if price_diff > 0:
                self.ai_optimizer.register_win(trade['symbol'])
            else:
                self.ai_optimizer.register_loss(trade['symbol'])

        self.journal.record(trade['symbol'], side, reason, pnl_usdt, pnl_pct, r_multiple, closed_pct)

        emoji = "✅" if pnl_usdt > 0 else ("⚪" if pnl_usdt == 0 else "❌")
        msg = f"""
{emoji} **گزارش معامله محافظت‌شده**

📌 **ارز:** {trade['symbol']} ({side})
📎 **علت:** {reason}
📈 **سود/زیان این مرحله (بعد از کارمزد/اسلیپیج تخمینی):** {pnl_pct:+.2f}% ({pnl_usdt:+.2f} USDT)
📐 **R Multiple:** {r_multiple:+.2f}R
📦 **درصد بسته‌شده:** {closed_pct}%
"""
        TelegramNotifier.send_to_personal(msg)

    def update_and_check_trades(self, data_layer):
        with self.lock:
            if not self.active_trades:
                return

            for trade_id, trade in list(self.active_trades.items()):
                try:
                    df = data_layer.fetch_ohlcv_df(trade['symbol'], "1m", limit=5)
                    if df.empty:
                        continue
                    latest_high = float(df['high'].max())
                    latest_low = float(df['low'].min())
                    side = trade['side']

                    trade['highest_since_entry'] = max(trade['highest_since_entry'], latest_high)
                    trade['lowest_since_entry'] = min(trade['lowest_since_entry'], latest_low)

                    hit_sl = (side == "BUY" and latest_low <= trade['sl']) or (side == "SELL" and latest_high >= trade['sl'])
                    if hit_sl:
                        is_breakeven = trade['tp1_hit'] and abs(trade['sl'] - trade['entry']) / trade['entry'] < 0.001
                        reason = "بسته‌شدن با سود قفل‌شده (Break-even)" if is_breakeven else "برخورد به حد ضرر"
                        self._close_partial(trade, trade['sl'], reason, trade['remaining_pct'], register_result=not is_breakeven)
                        del self.active_trades[trade_id]
                        continue

                    hit_tp3 = (side == "BUY" and latest_high >= trade['tp3']) or (side == "SELL" and latest_low <= trade['tp3'])
                    if hit_tp3:
                        self._close_partial(trade, trade['tp3'], "برخورد به TP3 (خروج کامل)", trade['remaining_pct'])
                        del self.active_trades[trade_id]
                        continue

                    hit_tp2 = (side == "BUY" and latest_high >= trade['tp2']) or (side == "SELL" and latest_low <= trade['tp2'])
                    if hit_tp2 and not trade['tp2_hit']:
                        self._close_partial(trade, trade['tp2'], "برخورد به TP2 (بستن جزئی ۳۰٪)", 30)
                        trade['remaining_pct'] -= 30
                        trade['tp2_hit'] = True

                    hit_tp1 = (side == "BUY" and latest_high >= trade['tp1']) or (side == "SELL" and latest_low <= trade['tp1'])
                    if hit_tp1 and not trade['tp1_hit']:
                        self._close_partial(trade, trade['tp1'], "برخورد به TP1 (بستن جزئی ۵۰٪ + SL به سر به سر)", 50)
                        trade['remaining_pct'] -= 50
                        trade['tp1_hit'] = True
                        trade['sl'] = trade['entry']

                    if trade['tp1_hit'] and trade['remaining_pct'] > 0:
                        p = self.ai_optimizer.get_params(trade['symbol'])
                        atr = trade['atr_at_entry']
                        if side == "BUY":
                            new_trail = trade['highest_since_entry'] - (p["trailing_mult"] * atr)
                            trade['sl'] = max(trade['sl'], round(new_trail, 6))
                        else:
                            new_trail = trade['lowest_since_entry'] + (p["trailing_mult"] * atr)
                            trade['sl'] = min(trade['sl'], round(new_trail, 6))

                    self._save_trades()

                except Exception as e:
                    logger.error(f"خطا در بررسی معامله مجازی {trade_id}: {e}")

# ==================== ارسال‌کننده پیام به تلگرام ====================
class TelegramNotifier:
    @staticmethod
    def send_to_channel(symbol: str, side: str, latest: pd.Series, trend_4h: str, levels: dict,
                         judge_reason: str = "", judge_confidence: int = 0):
        config = Config()
        if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHANNEL_ID:
            return True  # کانال تنظیم نشده: مثل قبل، بقیه‌ی مسیر ادامه پیدا می‌کنه
        try:
            emoji = "🟢" if side == "BUY" else "🔴"
            direction = "LONG" if side == "BUY" else "SHORT"

            price = levels["price"]
            tp1, tp2, tp3, stop_loss = levels["tp1"], levels["tp2"], levels["tp3"], levels["sl"]
            qty, notional, rr_ratio = levels["qty"], levels["notional"], levels["rr_ratio"]

            stress_note = ""
            if levels["stress_active"]:
                stress_note = f"\n⚠️ **استرس بازار شناسایی شد** (نوسان بیت‌کوین در پرسنتایل {levels['btc_volatility_pctl']:.0f}) - حجم پوزیشن به {levels['size_mult']*100:.0f}٪ حجم عادی کاهش یافت\n"

            message = f"""
{emoji} **ANTI-LOSS ULTRA SIGNAL: {side} / {direction}**

📍 **Symbol:** {symbol}
⏱ **Timeframe:** {config.ENTRY_TIMEFRAME} (Trend 4H: {trend_4h})

💵 **Entry Price:** {price:,}

🎯 **Dynamic Targets (مدیریت پله‌ای):**
  1️⃣ TP1 (بستن ۵۰٪ + SL به سر به سر): {tp1:,}
  2️⃣ TP2 (بستن ۳۰٪ دیگر): {tp2:,}
  3️⃣ TP3 (خروج کامل، تریلینگ فعال): {tp3:,}

🛑 **Stop-Loss:** {stop_loss:,}
⚖️ **R:R تا TP1:** 1:{rr_ratio:.2f}
{stress_note}
💰 **پیشنهاد حجم (ریسک {config.RISK_PER_TRADE_PCT}% سرمایه):**
  مقدار: {qty:.6f} | ارزش: {notional:,.2f} USDT

📊 **Metrics:** RSI: {latest['rsi']:.1f} | Market Guardrails Active
🧠 **قضاوت AI (اطمینان {judge_confidence}%):** {judge_reason if judge_reason else '—'}
⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}
"""
            url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": config.TELEGRAM_CHANNEL_ID,
                "text": message,
                "parse_mode": "Markdown"
            }
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code != 200:
                logger.error(f"ارسال سیگنال ناموفق بود: {r.status_code} {r.text}")
                return False
            logger.info("پیام سیگنال با موفقیت به کانال تلگرام ارسال شد.")
            return True
        except Exception as e:
            logger.error(f"خطا در ارسال پیام به کانال تلگرام: {e}")
            return False

    @staticmethod
    def send_system_status(text: str):
        config = Config()
        if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHANNEL_ID:
            return
        try:
            url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
            r = requests.post(url, json={"chat_id": config.TELEGRAM_CHANNEL_ID, "text": text, "parse_mode": "Markdown"}, timeout=10)
            if r.status_code != 200:
                logger.error(f"ارسال پیام وضعیت ناموفق بود: {r.status_code} {r.text}")
        except Exception as e:
            logger.error(f"خطای ارسال پیام به تلگرام: {e}")

    @staticmethod
    def send_to_personal(message: str):
        config = Config()
        if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_PERSONAL_ID:
            return
        try:
            url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": config.TELEGRAM_PERSONAL_ID,
                "text": message,
                "parse_mode": "Markdown"
            }
            requests.post(url, json=payload, timeout=10)
            logger.info("پیام گزارش به پی‌وی تلگرام ارسال شد.")
        except Exception as e:
            logger.error(f"خطا در ارسال پیام به پی‌وی تلگرام: {e}")

# ==================== وب‌سرور رندر ====================
class RenderWebhookHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"Render Signal Generator is alive and running!")

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()

    def do_POST(self):
        try:
            auth_token = self.headers.get("X-Secret-Token")
            config = Config()
            
            if config.SECRET_TOKEN and auth_token != config.SECRET_TOKEN:
                logger.warning("تلاش برای دسترسی غیرمجاز به وب‌هوک رندر با توکن اشتباه.")
                self.send_response(403)
                self.end_headers()
                return

            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            
            if not post_data:
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok"}).encode('utf-8'))
                return

            data = json.loads(post_data.decode('utf-8'))
            action = data.get("action")

            if action == "ping":
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "pong"}).encode('utf-8'))
                return

            if action == "close_trade":
                symbol = data.get("symbol")
                exit_price = data.get("exit_price")
                pnl = data.get("pnl")
                logger.info(f"گزارش بسته شدن معامله دریافت شد: {symbol} | نتیجه: {pnl}%")
                
                emoji = "✅" if pnl >= 0 else "❌"
                status_text = "سود" if pnl >= 0 else "زیان"
                
                close_msg = (
                    f"{emoji} **گزارش نتیجه نهایی معامله (اسپات)** {emoji}\n\n"
                    f"💎 نماد: `{symbol}`\n"
                    f"💵 قیمت خروج: `{exit_price}`\n"
                    f"📊 نتیجه: **{status_text} با {pnl:+.2f}%**\n"
                    f"🏷 صرافی: `والکس (Wallex)`"
                )
                TelegramNotifier.send_to_personal(close_msg)

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "received"}).encode('utf-8'))

        except Exception as e:
            logger.error(f"خطا در پردازش وب‌هوک برگشتی در رندر: {e}")
            self.send_response(500)
            self.end_headers()

    def log_message(self, format, *args):
        return

def start_render_server():
    port = int(os.environ.get("PORT", 10000))
    try:
        server = HTTPServer(("0.0.0.0", port), RenderWebhookHandler)
        server.serve_forever()
    except Exception as e:
        logger.error(f"خطا در اجرای وب‌سرور رندر: {e}")

threading.Thread(target=start_render_server, daemon=True).start()

# ==================== صرافی (کوین‌اکس) ====================
class PublicMarketDataFetcher:
    def __init__(self, config: Config):
        try:
            exchange_class = getattr(ccxt, config.EXCHANGE_ID)
            self.exchange = exchange_class({
                'apiKey': config.API_KEY,
                'secret': config.SECRET,
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'}
            })
            logger.info(f"اتصال به صرافی {config.EXCHANGE_ID} با موفقیت برقرار شد.")
        except Exception as e:
            logger.error(f"خطا در ایجاد اتصال صرافی: {e}")
            self.exchange = None

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 100) -> list:
        if not self.exchange:
            return []
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            return ohlcv
        except Exception as e:
            logger.error(f"خطا در دریافت کندل‌های {symbol} ({timeframe}): {e}")
            return []

    # ---- افزوده‌شده برای لایه‌ی تحلیل جدید (بدون تغییر در fetch_ohlcv بالا) ----
    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int = 150) -> pd.DataFrame:
        ohlcv = self.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv:
            return pd.DataFrame()
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df

    def fetch_funding_rate(self, symbol: str) -> Optional[float]:
        if not self.exchange:
            return None
        try:
            funding_symbol = symbol.replace("/USDT", "/USDT:USDT")
            data = self.exchange.fetch_funding_rate(funding_symbol)
            rate = data.get("fundingRate") if data else None
            return float(rate) if rate is not None else None
        except Exception:
            return None

    def fetch_spread_pct(self, symbol: str) -> Optional[float]:
        if not self.exchange:
            return None
        try:
            ob = self.exchange.fetch_order_book(symbol, limit=5)
            best_bid = ob['bids'][0][0] if ob.get('bids') else None
            best_ask = ob['asks'][0][0] if ob.get('asks') else None
            if not best_bid or not best_ask:
                return None
            mid = (best_bid + best_ask) / 2
            if mid <= 0:
                return None
            return float((best_ask - best_bid) / mid * 100)
        except Exception:
            return None

def verify_and_notify_startup(config: Config):
    if not config.HAMRAVESH_WEBHOOK_URL:
        logger.warning("آدرس وب‌هوک همروش تنظیم نشده است؛ اتصال کامل تایید نمی‌شود.")
        return

    max_retries = 10
    delay = 6

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"تلاش {attempt}/{max_retries} برای بررسی اتصال چرخه کامل رندر <-> همروش...")
            headers = {"X-Secret-Token": config.SECRET_TOKEN}
            response = requests.post(config.HAMRAVESH_WEBHOOK_URL, json={"action": "ping"}, headers=headers, timeout=5)
            
            if response.status_code == 200:
                logger.info("اتصال چرخه کامل رندر و همروش با موفقیت برقرار شد و تایید گردید.")
                startup_msg = "🚀 ربات هیبرید با موفقیت روشن شد: تمام چرخه‌ها (رندر، همروش، تحلیل و صرافی) کاملاً متصل و عملیاتی هستند."
                TelegramNotifier.send_to_personal(startup_msg)
                return
        except Exception as e:
            logger.warning(f"تلاش {attempt}: هنوز ارتباط کامل برقرار نشده است ({e})")
        time.sleep(delay)
    
    logger.error("خطا: چرخه‌های رندر و همروش به طور کامل متصل نشدند؛ پیام راه‌اندازی ارسال نگردید.")

class RenderSignalSystem:
    def __init__(self):
        self.config = Config()
        self.config.validate()
        self.data_fetcher = PublicMarketDataFetcher(self.config)
        self.analysis = AnalysisLayer(self.config)
        self.macro_data = MacroDataLayer()
        self.ai_optimizer = AIParameterOptimizer(self.config)
        self.signal_engine = SignalEngine(self.config, self.ai_optimizer, self.analysis)
        self.risk_manager = RiskManager(self.config)
        self.journal = TradeJournal()
        self.paper_trader = PaperTrader(self.config, self.ai_optimizer, self.journal)
        self.correlation_manager = CorrelationManager(self.config, self.data_fetcher)
        # وقتی سهمیه‌ی Groq تموم/برگشت یا اتصال قطع/برقرار شد، از همین مسیر به تلگرام خبر بده
        self.ai_optimizer.on_quota_exhausted_callback = self._send_crash_alert
        self.running = True
        self.last_signal_time: Dict[str, datetime] = {}
        self.last_summary_date: Optional[str] = date_cls.today().isoformat()
        self.kill_switch_warned_today = False
        # واچ‌داگ قطعی داده/API
        self.consecutive_full_cycle_failures = 0
        self.data_outage_alert_sent = False

    def _send_crash_alert(self, text: str):
        try:
            TelegramNotifier.send_to_personal(f"🚨 **هشدار سیستم** 🚨\n\n{text}")
        except Exception:
            logger.error("حتی ارسال هشدار خطا هم ناموفق بود.")

    def send_signal_to_hamravesh(self, payload: dict):
        if not self.config.HAMRAVESH_WEBHOOK_URL:
            return
        try:
            headers = {"X-Secret-Token": self.config.SECRET_TOKEN}
            requests.post(self.config.HAMRAVESH_WEBHOOK_URL, json=payload, headers=headers, timeout=15)
            logger.info(f"سیگنال نماد {payload.get('symbol')} با موفقیت به همروش ارسال شد.")
        except Exception as e:
            logger.error(f"خطا در ارسال سیگنال به همروش: {e}")

    def _start_trade_monitor_thread(self):
        """ترد مستقل مانیتورینگ معاملات باز (هر TRADE_MONITOR_INTERVAL_SECONDS ثانیه)، بدون فراخوانی Groq."""
        def monitor_loop():
            while self.running:
                try:
                    self.paper_trader.update_and_check_trades(self.data_fetcher)
                except Exception as e:
                    logger.error(f"خطا در ترد مانیتورینگ لحظه‌ای معاملات: {e}")
                    self._send_crash_alert(f"خطا در ترد مانیتورینگ معاملات باز:\n`{e}`")
                time.sleep(self.config.TRADE_MONITOR_INTERVAL_SECONDS)

        threading.Thread(target=monitor_loop, daemon=True, name="TradeMonitor").start()
        logger.info(f"ترد مانیتورینگ لحظه‌ای معاملات فعال شد (هر {self.config.TRADE_MONITOR_INTERVAL_SECONDS} ثانیه)")

    def _build_macro_context(self) -> dict:
        context = {"fear_greed": None, "btc_trend_4h": None, "btc_structure": None, "btc_volatility_pctl": None,
                   "btc_reference_trade": None, "btc_rsi": None, "btc_macd_hist": None}
        try:
            context["fear_greed"] = self.macro_data.get_fear_greed_index()
        except Exception as e:
            logger.warning(f"خطا در دریافت شاخص ترس‌وطمع: {e}")

        try:
            context["btc_reference_trade"] = self.paper_trader.get_reference_trade_health("BTC/USDT")
        except Exception as e:
            logger.warning(f"خطا در دریافت وضعیت معامله‌ی مرجع بیت‌کوین: {e}")

        try:
            btc_df_4h = self.data_fetcher.fetch_ohlcv_df("BTC/USDT", self.config.TREND_TIMEFRAME, limit=self.config.TREND_WARMUP_CANDLES)
            btc_df_4h = self.analysis.calculate_indicators(btc_df_4h)
            context["btc_trend_4h"] = self.analysis.get_major_trend(btc_df_4h)

            btc_df_15m = self.data_fetcher.fetch_ohlcv_df("BTC/USDT", self.config.ENTRY_TIMEFRAME)
            btc_df_15m = self.analysis.calculate_indicators(btc_df_15m)
            context["btc_structure"] = self.analysis.market_structure(btc_df_15m)
            context["btc_volatility_pctl"] = self.analysis.atr_percentile(btc_df_15m)
            if not btc_df_15m.empty:
                btc_latest = btc_df_15m.iloc[-1]
                if not pd.isna(btc_latest.get('rsi', float('nan'))):
                    context["btc_rsi"] = float(btc_latest['rsi'])
                if not pd.isna(btc_latest.get('macd_hist', float('nan'))):
                    context["btc_macd_hist"] = float(btc_latest['macd_hist'])
        except Exception as e:
            logger.warning(f"خطا در ساخت زمینه‌ی کلان بیت‌کوین: {e}")

        return context

    def process_symbol(self, symbol: str, macro_context: Optional[dict] = None) -> bool:
        """خروجی: True یعنی دریافت داده‌ی این نماد موفق بود (حتی اگه سیگنالی صادر نشد)،
        False یعنی fetch داده شکست خورد - برای واچ‌داگ قطعی API."""
        try:
            df_15m = self.data_fetcher.fetch_ohlcv_df(symbol, self.config.ENTRY_TIMEFRAME)
            df_15m = self.analysis.calculate_indicators(df_15m)
            if df_15m.empty:
                return False

            if self.ai_optimizer.should_optimize(symbol):
                logger.info(f"بروزرسانی پارامترهای ضد ضرر هوش مصنوعی برای {symbol}...")
                self.ai_optimizer.optimize_symbol_parameters(symbol, df_15m, journal=self.journal)

            df_1h = self.data_fetcher.fetch_ohlcv_df(symbol, self.config.CONFIRM_TIMEFRAME)
            df_1h = self.analysis.calculate_indicators(df_1h)

            df_4h = self.data_fetcher.fetch_ohlcv_df(symbol, self.config.TREND_TIMEFRAME, limit=self.config.TREND_WARMUP_CANDLES)
            df_4h = self.analysis.calculate_indicators(df_4h)
            trend = self.analysis.get_major_trend(df_4h)

            symbol_macro_context = dict(macro_context or {})
            symbol_macro_context["funding_rate"] = self.data_fetcher.fetch_funding_rate(symbol)
            symbol_macro_context["spread_pct"] = self.data_fetcher.fetch_spread_pct(symbol)

            rule_signal, diagnostics = self.signal_engine.get_rule_signal(symbol, df_15m, df_1h, trend, symbol_macro_context)
            if not rule_signal:
                return True

            now = datetime.now()
            p = self.ai_optimizer.get_params(symbol)
            cooldown = p.get("cooldown_minutes", 90)
            if symbol in self.last_signal_time:
                if now - self.last_signal_time[symbol] < timedelta(minutes=cooldown):
                    return True

            if self.risk_manager.kill_switch_triggered(self.journal):
                if not self.kill_switch_warned_today:
                    logger.warning("کلید قطع ضرر روزانه فعال شد - تا فردا سیگنال جدیدی صادر نمی‌شه")
                    TelegramNotifier.send_system_status("🛑 **کلید قطع ضرر روزانه فعال شد.** برای امروز دیگه معامله‌ی جدیدی باز نمی‌شه.")
                    self.kill_switch_warned_today = True
                return True

            active_trades_snapshot = self.paper_trader.snapshot_active_trades()
            can_open, reason = self.risk_manager.can_open_trade(
                symbol, active_trades_snapshot,
                correlation_manager=self.correlation_manager,
                btc_volatility_pctl=(macro_context or {}).get("btc_volatility_pctl")
            )
            if not can_open:
                logger.info(f"{symbol}: سیگنال {rule_signal} رد شد - {reason}")
                return True

            diagnostics["open_positions_count"] = len(active_trades_snapshot)
            diagnostics["today_realized_pnl_usdt"] = round(self.journal.get_today_realized_pnl_usdt(), 2)
            judge = self.ai_optimizer.evaluate_trade_candidate(symbol, rule_signal, diagnostics)
            if not judge["approve"] or judge["confidence"] < self.config.MIN_JUDGE_CONFIDENCE:
                logger.info(f"{symbol}: سیگنال {rule_signal} توسط لایه‌ی قضاوت AI رد شد (اطمینان {judge['confidence']}%) - {judge['reason']}")
                return True

            latest = df_15m.iloc[-1]
            levels = self.signal_engine.calculate_trade_levels(symbol, rule_signal, latest, symbol_macro_context)

            sent_ok = TelegramNotifier.send_to_channel(symbol, rule_signal, latest, trend, levels,
                                                        judge_reason=judge["reason"], judge_confidence=judge["confidence"])

            if sent_ok:
                payload = {
                    "action": "execute_trade",
                    "symbol": symbol,
                    "side": rule_signal,
                    "price": levels["price"],
                    "trend": trend,
                    "tp1": levels["tp1"],
                    "tp2": levels["tp2"],
                    "tp3": levels["tp3"],
                    "sl": levels["sl"]
                }
                self.send_signal_to_hamravesh(payload)

                self.paper_trader.open_virtual_trade(
                    symbol=symbol,
                    side=rule_signal,
                    entry_price=levels["price"],
                    tp1=levels["tp1"],
                    tp2=levels["tp2"],
                    tp3=levels["tp3"],
                    sl=levels["sl"],
                    qty=levels["qty"],
                    atr_at_entry=levels["atr"],
                    spread_pct_at_entry=symbol_macro_context.get("spread_pct")
                )
                self.last_signal_time[symbol] = now

            return True

        except Exception as e:
            logger.error(f"خطا در پردازش {symbol}: {e}")
            return False

    def _check_daily_rollover(self):
        today = date_cls.today().isoformat()
        if self.last_summary_date and today != self.last_summary_date:
            summary = self.journal.build_daily_summary(self.last_summary_date)
            if summary:
                TelegramNotifier.send_system_status(summary)
            self.last_summary_date = today
            self.kill_switch_warned_today = False

    def run_once(self):
        logger.info("----- شروع آنالیز ایمن و ضد ضرر بازار -----")
        self._check_daily_rollover()

        if self.correlation_manager.should_refresh():
            self.correlation_manager.refresh()

        macro_context = self._build_macro_context()
        fetch_failures = 0
        for symbol in self.config.SYMBOLS:
            ok = self.process_symbol(symbol, macro_context)
            if not ok:
                fetch_failures += 1
            time.sleep(1.5)

        if fetch_failures == len(self.config.SYMBOLS):
            self.consecutive_full_cycle_failures += 1
        else:
            self.consecutive_full_cycle_failures = 0
            self.data_outage_alert_sent = False

        if self.consecutive_full_cycle_failures >= 2 and not self.data_outage_alert_sent:
            self._send_crash_alert(
                "دریافت داده برای همه‌ی نمادها در چند چرخه‌ی متوالی ناموفق بود - "
                "احتمالاً اتصال صرافی/اینترنت مشکل داره. ربات به تلاش ادامه می‌ده."
            )
            self.data_outage_alert_sent = True

    def run_loop(self):
        logger.info("بخش رندر (Render Signal Generator) با موفقیت فعال شد.")
        threading.Thread(target=verify_and_notify_startup, args=(self.config,), daemon=True).start()

        self._start_trade_monitor_thread()

        while self.running:
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"خطای پیش‌بینی‌نشده در چرخه‌ی اصلی: {e}")
                self._send_crash_alert(
                    f"خطای پیش‌بینی‌نشده در چرخه‌ی اصلی ربات:\n`{e}`\n\n"
                    "ربات همچنان روشنه و چرخه‌ی بعدی رو امتحان می‌کنه."
                )
            gc.collect()
            logger.info(f"پایان چرخه بررسی بازار. انتظار برای دور بعدی ({self.config.CHECK_INTERVAL} ثانیه)...")
            time.sleep(self.config.CHECK_INTERVAL)

    def stop(self):
        self.running = False
        logger.info("بخش رندر متوقف شد.")

if __name__ == "__main__":
    system = RenderSignalSystem()
    try:
        system.run_loop()
    except KeyboardInterrupt:
        system.stop()
