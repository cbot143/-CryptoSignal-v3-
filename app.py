"""
=============================================================
  CryptoSignal v3.0 — UPGRADED
  Backend: Flask + Binance public API + River Online ML

  v3 IMPROVEMENTS:
    ✦ ZEC (Zcash) added to pairs
    ✦ SSE (Server-Sent Events) for real-time data streaming
    ✦ Active user counter (tracks live sessions)
    ✦ AI learning flow visible in real-time via SSE
    ✦ Fixed online River ML (correct learn_one / predict_proba_one)
    ✦ AI training log broadcast to all clients
    ✦ /api/users endpoint for live user count
    ✦ /api/stream SSE endpoint for real-time updates
=============================================================
"""

from flask import Flask, render_template, jsonify, request, Response
import requests
import numpy as np
import pandas as pd
import time
import threading
import queue
from datetime import datetime, timezone
from collections import deque
import pickle
import os
import json
import uuid

# River (online machine learning)
from river import (
    linear_model,
    naive_bayes,
    tree,
    ensemble,
    preprocessing,
    metrics
)

app = Flask(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BINANCE_BASE = "https://api.binance.com/api/v3"
KLINE_LIMIT  = 300

PAIRS = {
    "BTC":  "BTCUSDT",
    "ETH":  "ETHUSDT",
    "BNB":  "BNBUSDT",
    "SOL":  "SOLUSDT",
    "PAXG": "PAXGUSDT",
    "DOGE": "DOGEUSDT",
    "ZEC":  "ZECUSDT",   # ← NEW
}

TIMEFRAMES = ["15m", "1h", "4h", "1d"]

signal_history = {p: {tf: deque(maxlen=200) for tf in TIMEFRAMES} for p in PAIRS}
signal_log     = deque(maxlen=300)

_cache      = {}
_cache_lock = threading.Lock()
CACHE_TTL   = 15

streaks = {p: {tf: {"type": None, "count": 0} for tf in TIMEFRAMES} for p in PAIRS}

# ─────────────────────────────────────────────
# ACTIVE USER TRACKER
# ─────────────────────────────────────────────
active_sessions = {}          # {session_id: last_seen_timestamp}
sessions_lock   = threading.Lock()
SESSION_TIMEOUT = 90          # seconds — consider user inactive after this

def register_session(sid: str):
    with sessions_lock:
        active_sessions[sid] = time.time()

def cleanup_sessions():
    """Remove sessions that haven't pinged in SESSION_TIMEOUT seconds."""
    with sessions_lock:
        cutoff = time.time() - SESSION_TIMEOUT
        stale  = [sid for sid, ts in active_sessions.items() if ts < cutoff]
        for sid in stale:
            del active_sessions[sid]

def get_active_user_count() -> int:
    cleanup_sessions()
    with sessions_lock:
        return len(active_sessions)

# ─────────────────────────────────────────────
# SSE BROADCAST SYSTEM
# ─────────────────────────────────────────────
sse_clients      = {}          # {client_id: queue.Queue}
sse_clients_lock = threading.Lock()

def sse_broadcast(event_type: str, data: dict):
    """Broadcast a Server-Sent Event to all connected SSE clients."""
    payload = json.dumps({"type": event_type, "data": data, "ts": time.time()})
    with sse_clients_lock:
        dead = []
        for cid, q in sse_clients.items():
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(cid)
        for cid in dead:
            del sse_clients[cid]

def sse_add_client(cid: str) -> queue.Queue:
    q = queue.Queue(maxsize=50)
    with sse_clients_lock:
        sse_clients[cid] = q
    return q

def sse_remove_client(cid: str):
    with sse_clients_lock:
        sse_clients.pop(cid, None)

# ─────────────────────────────────────────────
# AI MODEL MANAGER (online ensemble, persistent)
# ─────────────────────────────────────────────
MODEL_PATH    = "ai_model.pkl"
AI_STATS_PATH = "ai_stats.json"

# Rolling log of recent AI training events for the UI
ai_train_log = deque(maxlen=50)

class AIModelManager:
    """
    Online ensemble that learns incrementally using River.
    Broadcasts training events via SSE so the UI can show real-time learning.
    """
    def __init__(self):
        self.models          = []
        self.scaler          = preprocessing.StandardScaler()
        self.initialized     = False
        self.trained_samples = 0
        self.accuracy        = metrics.Accuracy()
        self.pair_stats      = {}
        self._load_or_init()

    def _load_or_init(self):
        if os.path.exists(MODEL_PATH):
            try:
                with open(MODEL_PATH, 'rb') as f:
                    data = pickle.load(f)
                self.models          = data['models']
                self.scaler          = data['scaler']
                self.trained_samples = data.get('trained_samples', 0)
                self.pair_stats      = data.get('pair_stats', {})
                self.initialized     = True
                print(f"[AI] Loaded model — {self.trained_samples} samples")
            except Exception as e:
                print(f"[AI] Load failed ({e}), re-initialising")
                self._init_models()
        else:
            self._init_models()

    def _init_models(self):
        self.models = [
            linear_model.LogisticRegression(l2=0.01),
            naive_bayes.GaussianNB(),
            tree.HoeffdingTreeClassifier(),
            linear_model.Perceptron(l2=0.01),
            ensemble.AdaBoostClassifier(
                model=tree.HoeffdingTreeClassifier(),
                n_models=5, seed=42
            ),
        ]
        self.initialized     = True
        self.trained_samples = 0
        self.pair_stats      = {}
        self._save()
        print("[AI] New ensemble created")

    def _save(self):
        try:
            with open(MODEL_PATH, 'wb') as f:
                pickle.dump({
                    'models':          self.models,
                    'scaler':          self.scaler,
                    'trained_samples': self.trained_samples,
                    'pair_stats':      self.pair_stats,
                }, f)
            with open(AI_STATS_PATH, 'w') as f:
                json.dump({
                    'trained_samples': self.trained_samples,
                    'global_accuracy': round(self.accuracy.get() * 100, 2),
                    'pair_stats':      self.pair_stats,
                    'saved_at':        datetime.now(timezone.utc).isoformat(),
                }, f, indent=2)
        except Exception as e:
            print(f"[AI] Save failed: {e}")

    def extract_features(self, sig: dict) -> dict:
        price  = sig.get('price', 0)
        vwap   = sig.get('vwap', price)
        ich    = sig.get('ichimoku', {})
        willr  = sig.get('williams_r', -50)

        pvwap = (price / vwap - 1.0) if vwap and vwap > 0 else 0.0

        senkou_a = ich.get('senkou_a', price)
        senkou_b = ich.get('senkou_b', price)
        cloud_thickness = (senkou_a - senkou_b) / price if price > 0 else 0.0
        price_vs_cloud  = (price - max(senkou_a, senkou_b)) / price if price > 0 else 0.0

        return {
            'rsi':              sig.get('rsi', 50),
            'macd_hist':        sig.get('macd_hist', 0),
            'bb_pos':           sig.get('bb_pos', 50) / 100.0,
            'momentum':         sig.get('momentum_pct', 0),
            'vol_ratio':        min(sig.get('vol_ratio', 1), 10.0),
            'stoch_k':          sig.get('stoch_k', 50),
            'atr':              sig.get('atr', 0),
            'vola':             sig.get('vola_pct', 0),
            'ema9_21_diff':     sig.get('ema9', 0) - sig.get('ema21', 0),
            'price_vs_ema50':   (price / sig.get('ema50', price) - 1.0) if sig.get('ema50') else 0.0,
            'price_vs_vwap':    pvwap,
            'williams_r':       willr,
            'cloud_thickness':  cloud_thickness,
            'price_vs_cloud':   price_vs_cloud,
            'tenkan_kijun_diff': (ich.get('tenkan', price) - ich.get('kijun', price)) / price if price > 0 else 0.0,
        }

    def predict_proba(self, features: dict) -> float:
        if not self.initialized or self.trained_samples < 10:
            return 0.5
        try:
            x     = self.scaler.transform_one(features)
            probs = []
            for model in self.models:
                try:
                    pd_ = model.predict_proba_one(x)
                    if isinstance(pd_, dict):
                        probs.append(pd_.get(1, 0.5))
                    else:
                        probs.append(0.5)
                except Exception:
                    continue
            return sum(probs) / len(probs) if probs else 0.5
        except Exception:
            return 0.5

    def update(self, features: dict, outcome: int, pair: str = "", tf: str = ""):
        key = f"{pair}_{tf}"

        # Track global accuracy
        if self.trained_samples >= 10:
            prob = self.predict_proba(features)
            pred = 1 if prob >= 0.5 else 0
            self.accuracy.update(pred, outcome)

        # Track per-pair-tf stats
        if key not in self.pair_stats:
            self.pair_stats[key] = {"wins": 0, "losses": 0, "total": 0, "accuracy": 0}
        ps = self.pair_stats[key]
        ps["total"] += 1
        if outcome == 1:
            ps["wins"] += 1
        else:
            ps["losses"] += 1
        ps["accuracy"] = round(ps["wins"] / ps["total"] * 100, 1) if ps["total"] > 0 else 0

        # --- Online learning: scaler then models ---
        self.scaler = self.scaler.learn_one(features)
        x = self.scaler.transform_one(features)
        for model in self.models:
            try:
                model.learn_one(x, outcome)
            except Exception:
                continue

        self.trained_samples += 1
        label      = "WIN" if outcome == 1 else "LOSS"
        global_acc = round(self.accuracy.get() * 100, 1) if self.trained_samples >= 10 else 0.0

        log_entry = {
            "sample":  self.trained_samples,
            "pair":    pair,
            "tf":      tf,
            "outcome": label,
            "global_acc": global_acc,
            "pair_acc":   ps["accuracy"],
            "time":    datetime.now(timezone.utc).strftime("%H:%M:%S"),
        }
        ai_train_log.appendleft(log_entry)

        # ── Broadcast AI learning event via SSE ──
        sse_broadcast("ai_learn", {
            **log_entry,
            "trained_samples": self.trained_samples,
            "ai_weight_pct":   round(min((self.trained_samples - 30) / 200, 0.20) * 100, 1)
                               if self.trained_samples >= 30 else 0,
        })

        print(f"[AI] Sample #{self.trained_samples} {pair}/{tf} → {label} | Acc: {global_acc:.1f}%")

        if self.trained_samples % 10 == 0:
            self._save()


ai_model = AIModelManager()

# ─────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────
def cache_key(symbol, interval):
    return f"{symbol}_{interval}"

def fetch_klines(symbol: str, interval: str, limit: int = KLINE_LIMIT) -> pd.DataFrame:
    ck = cache_key(symbol, interval)
    with _cache_lock:
        if ck in _cache:
            ts, df = _cache[ck]
            if time.time() - ts < CACHE_TTL:
                return df

    url    = f"{BINANCE_BASE}/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    resp   = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    raw    = resp.json()

    df = pd.DataFrame(raw, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","quote_vol","trades","taker_base","taker_quote","ignore"
    ])
    for col in ["open","high","low","close","volume"]:
        df[col] = df[col].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df.set_index("open_time", inplace=True)

    with _cache_lock:
        _cache[ck] = (time.time(), df)
    return df

def fetch_ticker(symbol: str) -> dict:
    ck = f"ticker_{symbol}"
    with _cache_lock:
        if ck in _cache:
            ts, data = _cache[ck]
            if time.time() - ts < CACHE_TTL:
                return data

    url  = f"{BINANCE_BASE}/ticker/24hr"
    resp = requests.get(url, params={"symbol": symbol}, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    with _cache_lock:
        _cache[ck] = (time.time(), data)
    return data

# ─────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────
def calc_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_macd(series: pd.Series, fast=12, slow=26, signal=9):
    ema_fast    = calc_ema(series, fast)
    ema_slow    = calc_ema(series, slow)
    macd_line   = ema_fast - ema_slow
    signal_line = calc_ema(macd_line, signal)
    histogram   = macd_line - signal_line
    return macd_line, signal_line, histogram

def calc_bollinger(series: pd.Series, period: int = 20, std_dev: float = 2.0):
    mid = series.rolling(period).mean()
    std = series.rolling(period).std()
    return mid + std_dev * std, mid, mid - std_dev * std

def calc_stoch_rsi(series: pd.Series, rsi_period=14, stoch_period=14, k=3, d=3):
    rsi     = calc_rsi(series, rsi_period)
    rsi_min = rsi.rolling(stoch_period).min()
    rsi_max = rsi.rolling(stoch_period).max()
    stoch   = 100 * (rsi - rsi_min) / (rsi_max - rsi_min + 1e-10)
    k_line  = stoch.rolling(k).mean()
    d_line  = k_line.rolling(d).mean()
    return k_line, d_line

def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def calc_vwap(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3
    vwap    = (typical * df["volume"]).cumsum() / df["volume"].cumsum()
    return vwap

def calc_ichimoku(df: pd.DataFrame, tenkan=9, kijun=26, senkou_b_period=52, chikou=26):
    high, low, close = df["high"], df["low"], df["close"]
    tenkan_sen  = (high.rolling(tenkan).max()             + low.rolling(tenkan).min())             / 2
    kijun_sen   = (high.rolling(kijun).max()              + low.rolling(kijun).min())              / 2
    senkou_a    = ((tenkan_sen + kijun_sen) / 2).shift(kijun)
    senkou_b    = ((high.rolling(senkou_b_period).max()   + low.rolling(senkou_b_period).min()) / 2).shift(kijun)
    chikou_span = close.shift(-chikou)
    return {"tenkan": tenkan_sen, "kijun": kijun_sen,
            "senkou_a": senkou_a, "senkou_b": senkou_b, "chikou": chikou_span}

def calc_williams_r(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high_max = df["high"].rolling(period).max()
    low_min  = df["low"].rolling(period).min()
    return -100 * (high_max - df["close"]) / (high_max - low_min + 1e-10)

# ─────────────────────────────────────────────
# INDICATOR WEIGHTS
# ─────────────────────────────────────────────
INDICATOR_WEIGHTS = {
    "rsi": 3, "macd": 3, "ema": 3, "ema50": 1, "bb": 2,
    "momentum": 2, "stoch": 2, "volume": 2,
    "vwap": 2, "ichimoku": 3, "williams_r": 2,
}

# ─────────────────────────────────────────────
# SIGNAL ENGINE
# ─────────────────────────────────────────────
def generate_signal(df: pd.DataFrame) -> dict:
    closes  = df["close"]
    volumes = df["volume"]
    price   = closes.iloc[-1]

    rsi_series             = calc_rsi(closes)
    rsi_val                = float(rsi_series.iloc[-1])
    macd_line, sig_l, hist = calc_macd(closes)
    hist_last, hist_prev   = float(hist.iloc[-1]), float(hist.iloc[-2])
    ema9   = calc_ema(closes, 9);  ema9_l,  ema9_p  = float(ema9.iloc[-1]),  float(ema9.iloc[-2])
    ema21  = calc_ema(closes, 21); ema21_l, ema21_p = float(ema21.iloc[-1]), float(ema21.iloc[-2])
    ema50  = calc_ema(closes, 50)
    bb_u, bb_m, bb_l = calc_bollinger(closes)
    bb_pos = (price - float(bb_l.iloc[-1])) / (float(bb_u.iloc[-1]) - float(bb_l.iloc[-1]) + 1e-10)
    momentum_pct  = (float(closes.iloc[-1]) - float(closes.iloc[-10])) / float(closes.iloc[-10]) * 100
    recent        = closes.tail(20)
    vola_pct      = float(recent.std() / recent.mean() * 100)
    stoch_k, stoch_d = calc_stoch_rsi(closes)
    sk_last, sd_last = float(stoch_k.iloc[-1]), float(stoch_d.iloc[-1])
    avg_vol   = float(volumes.tail(20).mean())
    vol_ratio = float(volumes.iloc[-1]) / avg_vol if avg_vol > 0 else 1.0
    atr_series = calc_atr(df)
    atr_val    = float(atr_series.iloc[-1]) if not np.isnan(atr_series.iloc[-1]) else 0

    vwap_series = calc_vwap(df)
    vwap_val    = float(vwap_series.iloc[-1])

    ich_data = calc_ichimoku(df)
    ichi = {k: float(v.iloc[-1]) if not pd.isna(v.iloc[-1]) else price for k, v in ich_data.items()}

    willr_series = calc_williams_r(df)
    willr_val    = float(willr_series.iloc[-1]) if not np.isnan(willr_series.iloc[-1]) else -50.0

    bull, bear   = 0, 0
    reasons, warnings = [], []

    # RSI
    if rsi_val < 25:
        bull += 3; reasons.append(f"RSI strongly oversold ({rsi_val:.1f}) — high bounce probability")
    elif rsi_val < 35:
        bull += 2; reasons.append(f"RSI oversold ({rsi_val:.1f})")
    elif rsi_val > 75:
        bear += 3; reasons.append(f"RSI strongly overbought ({rsi_val:.1f}) — reversal risk")
    elif rsi_val > 65:
        bear += 2; reasons.append(f"RSI overbought ({rsi_val:.1f})")
    elif rsi_val > 55:
        bull += 1
    elif rsi_val < 45:
        bear += 1

    # MACD
    if hist_last > 0 and hist_prev <= 0:
        bull += 3; reasons.append("MACD bullish crossover — strongest momentum signal")
    elif hist_last < 0 and hist_prev >= 0:
        bear += 3; reasons.append("MACD bearish crossover — momentum turned negative")
    elif hist_last > 0 and hist_last > hist_prev:
        bull += 2; reasons.append("MACD histogram expanding — momentum accelerating up")
    elif hist_last < 0 and hist_last < hist_prev:
        bear += 2; reasons.append("MACD histogram deepening — momentum accelerating down")
    elif hist_last > 0:
        bull += 1
    else:
        bear += 1

    # EMA cross
    if ema9_l > ema21_l and ema9_p <= ema21_p:
        bull += 3; reasons.append("Golden cross — EMA9 just crossed above EMA21")
    elif ema9_l < ema21_l and ema9_p >= ema21_p:
        bear += 3; reasons.append("Death cross — EMA9 just crossed below EMA21")
    elif ema9_l > ema21_l:
        bull += 1
    else:
        bear += 1

    # EMA50 trend
    ema50_val = float(ema50.iloc[-1])
    if price > ema50_val:
        bull += 1
    else:
        bear += 1

    # Bollinger
    if bb_pos < 0.02:
        bull += 3; reasons.append("Price pierced below Bollinger lower band — strong reversal setup")
    elif bb_pos < 0.1:
        bull += 2; reasons.append("Price near Bollinger lower band — potential support bounce")
    elif bb_pos > 0.98:
        bear += 3; reasons.append("Price pierced above Bollinger upper band — overbought extreme")
    elif bb_pos > 0.9:
        bear += 2; reasons.append("Price near Bollinger upper band — overbought zone")

    # Momentum
    if momentum_pct > 4:
        bull += 2; reasons.append(f"Strong bullish momentum +{momentum_pct:.1f}% (10-candle)")
    elif momentum_pct > 1.5:
        bull += 1
    elif momentum_pct < -4:
        bear += 2; reasons.append(f"Strong bearish momentum {momentum_pct:.1f}% (10-candle)")
    elif momentum_pct < -1.5:
        bear += 1

    # StochRSI
    if sk_last < 15 and sd_last < 15:
        bull += 2; reasons.append(f"StochRSI extremely oversold (K={sk_last:.0f}) — reversal zone")
    elif sk_last < 25 and sd_last < 25:
        bull += 1
    elif sk_last > 85 and sd_last > 85:
        bear += 2; reasons.append(f"StochRSI extremely overbought (K={sk_last:.0f}) — exhaustion zone")
    elif sk_last > 75 and sd_last > 75:
        bear += 1

    # Volume
    if vol_ratio > 2.5:
        if bull > bear:
            bull += 2; reasons.append(f"Massive volume surge {vol_ratio:.1f}x — confirms bullish pressure")
        else:
            bear += 2; reasons.append(f"Massive volume surge {vol_ratio:.1f}x — confirms selling pressure")
    elif vol_ratio > 1.8:
        if bull >= bear:
            bull += 1
        else:
            bear += 1

    # VWAP
    price_vs_vwap_pct = (price / vwap_val - 1.0) * 100 if vwap_val > 0 else 0
    if price > vwap_val * 1.005:
        bull += 2; reasons.append(f"Price {price_vs_vwap_pct:.2f}% above VWAP — institutional buyers in control")
    elif price > vwap_val:
        bull += 1; reasons.append(f"Price above VWAP (+{price_vs_vwap_pct:.2f}%) — bullish bias")
    elif price < vwap_val * 0.995:
        bear += 2; reasons.append(f"Price {price_vs_vwap_pct:.2f}% below VWAP — sellers dominating")
    else:
        bear += 1

    # Ichimoku Cloud
    cloud_top     = max(ichi['senkou_a'], ichi['senkou_b'])
    cloud_bot     = min(ichi['senkou_a'], ichi['senkou_b'])
    cloud_bullish = ichi['senkou_a'] > ichi['senkou_b']
    if price > cloud_top:
        bull += 3; reasons.append("Price above Ichimoku cloud — strong bullish trend confirmed")
    elif price > cloud_bot:
        bull += 1; reasons.append("Price inside Ichimoku cloud — trend unclear, watch breakout")
    else:
        bear += 3; reasons.append("Price below Ichimoku cloud — strong bearish trend confirmed")
    if ichi['tenkan'] > ichi['kijun']:
        bull += 1
    else:
        bear += 1
    try:
        past_price = float(closes.iloc[-27]) if len(closes) >= 27 else price
        if ichi['chikou'] > past_price:
            bull += 1
        else:
            bear += 1
    except Exception:
        pass

    # Williams %R
    if willr_val < -90:
        bull += 2; reasons.append(f"Williams %R extreme oversold ({willr_val:.1f}) — maximum reversal probability")
    elif willr_val < -80:
        bull += 1; reasons.append(f"Williams %R oversold ({willr_val:.1f})")
    elif willr_val > -10:
        bear += 2; reasons.append(f"Williams %R extreme overbought ({willr_val:.1f}) — reversal imminent")
    elif willr_val > -20:
        bear += 1; reasons.append(f"Williams %R overbought ({willr_val:.1f})")

    # Volatility warnings
    if vola_pct > 6:
        warnings.append(f"EXTREME volatility ({vola_pct:.1f}%) — size positions very carefully")
    elif vola_pct > 4:
        warnings.append(f"High volatility ({vola_pct:.1f}%) — use tight stops")
    elif vola_pct > 2.5:
        warnings.append(f"Elevated volatility ({vola_pct:.1f}%)")

    # Score
    diff  = bull - bear
    total = bull + bear
    score = round(min(max((bull / max(total, 1)) * 100, 0), 100))

    if diff >= 8:
        sig_type = "STRONG_BUY";  sig_label = "STRONG BUY"
        plain    = "Very strong bullish confluence across 10 indicators. High-probability long setup."
        color    = "success"
    elif diff >= 4:
        sig_type = "BUY";         sig_label = "BUY"
        plain    = "Bullish setup forming with majority indicators aligned upward."
        color    = "success"
    elif diff <= -8:
        sig_type = "STRONG_SELL"; sig_label = "STRONG SELL"
        plain    = "Very strong bearish confluence across multiple indicators. Avoid longs."
        color    = "danger"
    elif diff <= -4:
        sig_type = "SELL";        sig_label = "SELL"
        plain    = "Bearish pressure building. Downside indicators outweigh upside."
        color    = "danger"
    else:
        sig_type = "NEUTRAL";     sig_label = "NEUTRAL"
        plain    = "Mixed signals. Market is consolidating. Wait for a decisive breakout."
        color    = "neutral"

    recent_high = float(df["high"].tail(20).max())
    recent_low  = float(df["low"].tail(20).min())

    indicator_states = {
        "rsi":        _rsi_state(rsi_val),
        "macd":       _macd_state(hist_last, hist_prev),
        "ema":        _ema_state(ema9_l, ema21_l, ema9_p, ema21_p),
        "bb":         _bb_state(bb_pos),
        "stoch":      _stoch_state(sk_last, sd_last),
        "momentum":   _momentum_state(momentum_pct),
        "volume":     _volume_state(vol_ratio),
        "vwap":       _vwap_state(price, vwap_val),
        "ichimoku":   _ichimoku_state(price, cloud_top, cloud_bot, cloud_bullish),
        "williams_r": _willr_state(willr_val),
    }

    sig = {
        "signal_type":       sig_type,
        "signal_label":      sig_label,
        "plain_english":     plain,
        "color":             color,
        "score":             score,
        "bull":              bull,
        "bear":              bear,
        "diff":              diff,
        "rsi":               round(rsi_val, 2),
        "macd_hist":         round(hist_last, 6),
        "macd_line":         round(float(macd_line.iloc[-1]), 6),
        "signal_line":       round(float(sig_l.iloc[-1]), 6),
        "bb_pos":            round(bb_pos * 100, 1),
        "bb_upper":          round(float(bb_u.iloc[-1]), 4),
        "bb_lower":          round(float(bb_l.iloc[-1]), 4),
        "bb_mid":            round(float(bb_m.iloc[-1]), 4),
        "ema9":              round(ema9_l, 4),
        "ema21":             round(ema21_l, 4),
        "ema50":             round(ema50_val, 4),
        "momentum_pct":      round(momentum_pct, 3),
        "vola_pct":          round(vola_pct, 3),
        "vol_ratio":         round(vol_ratio, 2),
        "stoch_k":           round(sk_last, 2),
        "stoch_d":           round(sd_last, 2),
        "atr":               round(atr_val, 4),
        "price":             round(price, 6),
        "recent_high":       round(recent_high, 4),
        "recent_low":        round(recent_low, 4),
        "vwap":              round(vwap_val, 4),
        "price_vs_vwap_pct": round(price_vs_vwap_pct, 3),
        "williams_r":        round(willr_val, 2),
        "ichimoku": {
            "tenkan":        round(ichi['tenkan'], 4),
            "kijun":         round(ichi['kijun'], 4),
            "senkou_a":      round(ichi['senkou_a'], 4),
            "senkou_b":      round(ichi['senkou_b'], 4),
            "cloud_top":     round(cloud_top, 4),
            "cloud_bot":     round(cloud_bot, 4),
            "cloud_bullish": cloud_bullish,
        },
        "reasons":           reasons[:6],
        "warnings":          warnings,
        "indicator_states":  indicator_states,
        "timestamp":         datetime.now(timezone.utc).isoformat(),
    }

    # AI Confidence
    features   = ai_model.extract_features(sig)
    ai_conf    = ai_model.predict_proba(features)
    ai_samples = ai_model.trained_samples

    sig['ai_confidence'] = round(ai_conf, 3)
    sig['ai_samples']    = ai_samples
    sig['features']      = features

    # Blended score
    if ai_samples >= 30:
        ai_weight     = min((ai_samples - 30) / 200, 0.20)
        rule_weight   = 1.0 - ai_weight
        ai_adjustment = (ai_conf - 0.5) * 40
        blended_score = round(score * rule_weight + (score + ai_adjustment) * ai_weight)
        blended_score = max(0, min(100, blended_score))
        sig['blended_score']  = blended_score
        sig['ai_weight_pct']  = round(ai_weight * 100, 1)
    else:
        sig['blended_score'] = score
        sig['ai_weight_pct'] = 0

    # AI override labels
    if sig_type in ('STRONG_BUY', 'BUY') and ai_conf < 0.35 and ai_samples >= 20:
        sig['signal_label'] = sig_label + ' (AI caution)'; sig['color'] = 'warning'
    elif sig_type in ('STRONG_SELL', 'SELL') and ai_conf < 0.35 and ai_samples >= 20:
        sig['signal_label'] = sig_label + ' (AI caution)'; sig['color'] = 'warning'
    elif sig_type in ('BUY', 'SELL') and ai_conf > 0.80 and ai_samples >= 20:
        sig['signal_label'] = 'STRONG ' + sig_label
        sig['color']        = 'success' if 'BUY' in sig_label else 'danger'

    # Trade setup
    if sig_type in ("STRONG_BUY", "BUY"):
        stop_loss   = min(recent_low,  price - atr_val * 1.5)
        take_profit = max(recent_high, price + atr_val * 2)
        if stop_loss   >= price: stop_loss   = price - atr_val
        if take_profit <= price: take_profit = price + atr_val
    elif sig_type in ("STRONG_SELL", "SELL"):
        stop_loss   = max(recent_high, price + atr_val * 1.5)
        take_profit = min(recent_low,  price - atr_val * 2)
        if stop_loss   <= price: stop_loss   = price + atr_val
        if take_profit >= price: take_profit = price - atr_val
    else:
        stop_loss = take_profit = None

    if stop_loss and take_profit:
        risk   = abs(price - stop_loss)
        reward = abs(take_profit - price)
        rr     = round(reward / risk, 2) if risk > 0 else None
    else:
        rr = None

    sig["stop_loss"]   = round(stop_loss,   6) if stop_loss   else None
    sig["take_profit"] = round(take_profit, 6) if take_profit else None
    sig["risk_reward"] = rr
    return sig

# ── Indicator state helpers ──
def _rsi_state(v):
    if v < 30: return {"label": "Oversold",   "dir": "bull", "value": round(v,1)}
    if v > 70: return {"label": "Overbought", "dir": "bear", "value": round(v,1)}
    return             {"label": "Neutral",    "dir": "neutral","value": round(v,1)}

def _macd_state(h, hp):
    if h > 0 and hp <= 0: return {"label": "Bullish Cross", "dir": "bull", "value": round(h,6)}
    if h < 0 and hp >= 0: return {"label": "Bearish Cross", "dir": "bear", "value": round(h,6)}
    if h > 0:             return {"label": "Bullish",       "dir": "bull", "value": round(h,6)}
    return                       {"label": "Bearish",       "dir": "bear", "value": round(h,6)}

def _ema_state(e9, e21, e9p, e21p):
    if e9 > e21 and e9p <= e21p: return {"label": "Golden Cross", "dir": "bull", "value": round(e9-e21,4)}
    if e9 < e21 and e9p >= e21p: return {"label": "Death Cross",  "dir": "bear", "value": round(e9-e21,4)}
    if e9 > e21:                 return {"label": "Bullish",      "dir": "bull", "value": round(e9-e21,4)}
    return                               {"label": "Bearish",     "dir": "bear", "value": round(e9-e21,4)}

def _bb_state(pos):
    if pos < 0.1: return {"label": "At Lower Band", "dir": "bull", "value": round(pos*100,1)}
    if pos > 0.9: return {"label": "At Upper Band", "dir": "bear", "value": round(pos*100,1)}
    return               {"label": "Mid Range",     "dir": "neutral","value": round(pos*100,1)}

def _stoch_state(k, d):
    if k < 20 and d < 20: return {"label": "Oversold",   "dir": "bull", "value": round(k,1)}
    if k > 80 and d > 80: return {"label": "Overbought", "dir": "bear", "value": round(k,1)}
    return                        {"label": "Neutral",    "dir": "neutral","value": round(k,1)}

def _momentum_state(pct):
    if pct > 2:  return {"label": f"+{pct:.1f}% Strong","dir": "bull", "value": round(pct,2)}
    if pct > 0:  return {"label": f"+{pct:.1f}%",       "dir": "bull", "value": round(pct,2)}
    if pct < -2: return {"label": f"{pct:.1f}% Strong", "dir": "bear", "value": round(pct,2)}
    return               {"label": f"{pct:.1f}%",        "dir": "bear", "value": round(pct,2)}

def _volume_state(ratio):
    if ratio > 2:   return {"label": f"Surge {ratio:.1f}x",    "dir": "bull", "value": round(ratio,2)}
    if ratio > 1.3: return {"label": f"Above Avg {ratio:.1f}x","dir": "bull", "value": round(ratio,2)}
    if ratio < 0.5: return {"label": f"Low {ratio:.1f}x",      "dir": "bear", "value": round(ratio,2)}
    return                  {"label": "Normal",                  "dir": "neutral","value": round(ratio,2)}

def _vwap_state(price, vwap):
    diff_pct = (price / vwap - 1.0) * 100 if vwap > 0 else 0
    if diff_pct > 0.5:  return {"label": f"+{diff_pct:.2f}% vs VWAP","dir": "bull", "value": round(diff_pct,2)}
    if diff_pct < -0.5: return {"label": f"{diff_pct:.2f}% vs VWAP", "dir": "bear", "value": round(diff_pct,2)}
    return                      {"label": "At VWAP",                   "dir": "neutral","value": round(diff_pct,2)}

def _ichimoku_state(price, cloud_top, cloud_bot, cloud_bullish):
    if price > cloud_top: return {"label": "Above Cloud 🟢", "dir": "bull",    "value": round(price - cloud_top, 4)}
    if price < cloud_bot: return {"label": "Below Cloud 🔴", "dir": "bear",    "value": round(cloud_bot - price, 4)}
    return                        {"label": "Inside Cloud ⚪","dir": "neutral", "value": 0}

def _willr_state(v):
    if v < -80: return {"label": f"Oversold ({v:.1f})",   "dir": "bull",    "value": round(v,1)}
    if v > -20: return {"label": f"Overbought ({v:.1f})", "dir": "bear",    "value": round(v,1)}
    return             {"label": f"Neutral ({v:.1f})",    "dir": "neutral", "value": round(v,1)}

# ─────────────────────────────────────────────
# SUCCESS RATE TRACKER
# ─────────────────────────────────────────────
def evaluate_past_signals(pair: str, tf: str, current_price: float):
    history = signal_history[pair][tf]
    for entry in history:
        if entry.get("outcome") != "PENDING":
            continue
        sig          = entry["signal"]
        entry_price  = entry["price"]
        pct_change   = (current_price - entry_price) / entry_price * 100
        outcome      = None

        if sig in ("STRONG_BUY", "BUY"):
            if pct_change >= 1.0:
                outcome = 1; entry["outcome"] = "WIN";  entry["pct_change"] = round(pct_change,2)
            elif pct_change <= -1.0:
                outcome = 0; entry["outcome"] = "LOSS"; entry["pct_change"] = round(pct_change,2)
        elif sig in ("STRONG_SELL", "SELL"):
            if pct_change <= -1.0:
                outcome = 1; entry["outcome"] = "WIN";  entry["pct_change"] = round(pct_change,2)
            elif pct_change >= 1.0:
                outcome = 0; entry["outcome"] = "LOSS"; entry["pct_change"] = round(pct_change,2)

        if outcome is not None and "features" in entry:
            ai_model.update(entry["features"], outcome, pair=pair, tf=tf)

def get_success_stats(pair: str, tf: str) -> dict:
    history  = signal_history[pair][tf]
    resolved = [e for e in history if e.get("outcome") in ("WIN","LOSS")]
    if not resolved:
        return {"total":0,"wins":0,"losses":0,"rate":0,"pending":len(history)}

    wins    = sum(1 for e in resolved if e["outcome"] == "WIN")
    losses  = len(resolved) - wins
    rate    = round(wins / len(resolved) * 100, 1)
    pending = sum(1 for e in history if e.get("outcome") == "PENDING")

    by_type = {}
    for e in resolved:
        t = e["signal"]
        if t not in by_type:
            by_type[t] = {"wins":0,"total":0}
        by_type[t]["total"] += 1
        if e["outcome"] == "WIN":
            by_type[t]["wins"] += 1
    for t in by_type:
        by_type[t]["rate"] = round(by_type[t]["wins"]/by_type[t]["total"]*100,1)

    return {
        "total":   len(resolved),
        "wins":    wins,
        "losses":  losses,
        "rate":    rate,
        "pending": pending,
        "by_type": by_type,
        "recent":  list(reversed(list(history)))[:10],
    }

# ─────────────────────────────────────────────
# CHART DATA
# ─────────────────────────────────────────────
def build_chart_data(df: pd.DataFrame) -> dict:
    closes  = df["close"]
    volumes = df["volume"]

    ema9   = calc_ema(closes, 9)
    ema21  = calc_ema(closes, 21)
    ema50  = calc_ema(closes, 50)
    rsi    = calc_rsi(closes)
    ml, sl, hist_s = calc_macd(closes)
    bb_u, bb_m, bb_l = calc_bollinger(closes)
    sk, sd = calc_stoch_rsi(closes)
    vwap_s = calc_vwap(df)
    willr  = calc_williams_r(df)
    ich    = calc_ichimoku(df)

    def s(series):
        return [round(float(v),6) if not np.isnan(v) else None for v in series]

    return {
        "timestamps": [int(t.timestamp()*1000) for t in df.index],
        "ohlc": [
            {"t": int(t.timestamp()*1000), "o":round(float(o),6),
             "h":round(float(h),6), "l":round(float(l),6), "c":round(float(c),6)}
            for t,o,h,l,c in zip(df.index, df["open"], df["high"], df["low"], closes)
        ],
        "close": s(closes), "volume": s(volumes),
        "ema9": s(ema9), "ema21": s(ema21), "ema50": s(ema50),
        "rsi": s(rsi), "macd": s(ml), "signal": s(sl), "hist": s(hist_s),
        "bb_upper": s(bb_u), "bb_mid": s(bb_m), "bb_lower": s(bb_l),
        "stoch_k": s(sk), "stoch_d": s(sd),
        "vwap": s(vwap_s),
        "williams_r": s(willr),
        "ich_tenkan":   s(ich["tenkan"]),
        "ich_kijun":    s(ich["kijun"]),
        "ich_senkou_a": s(ich["senkou_a"]),
        "ich_senkou_b": s(ich["senkou_b"]),
    }

# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", pairs=list(PAIRS.keys()), timeframes=TIMEFRAMES)

# ── Session ping (called by frontend every 30s) ──
@app.route("/api/ping")
def api_ping():
    sid = request.args.get("sid", "")
    if sid:
        register_session(sid)
    return jsonify({"active_users": get_active_user_count(), "sid": sid})

# ── Active users ──
@app.route("/api/users")
def api_users():
    return jsonify({
        "active_users":    get_active_user_count(),
        "sse_connections": len(sse_clients),
    })

# ── SSE stream ──
@app.route("/api/stream")
def api_stream():
    """
    Server-Sent Events endpoint.
    Streams: ai_learn, signal_update, user_count events to the browser.
    """
    cid = str(uuid.uuid4())
    q   = sse_add_client(cid)

    def generate():
        # Send welcome event
        yield f"data: {json.dumps({'type': 'connected', 'cid': cid})}\n\n"
        try:
            while True:
                try:
                    payload = q.get(timeout=25)
                    yield f"data: {payload}\n\n"
                except queue.Empty:
                    # Heartbeat to keep connection alive
                    yield f": heartbeat\n\n"
        except GeneratorExit:
            pass
        finally:
            sse_remove_client(cid)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no",
                             "Access-Control-Allow-Origin": "*"})

# ── AI training log ──
@app.route("/api/ai_train_log")
def api_ai_train_log():
    return jsonify(list(ai_train_log))

@app.route("/api/signal")
def api_signal():
    pair = request.args.get("pair","BTC").upper()
    tf   = request.args.get("tf","15m")
    sid  = request.args.get("sid","")
    if pair not in PAIRS or tf not in TIMEFRAMES:
        return jsonify({"error":"Invalid pair or timeframe"}), 400
    if sid:
        register_session(sid)
    try:
        symbol = PAIRS[pair]
        df     = fetch_klines(symbol, tf)
        ticker = fetch_ticker(symbol)
        sig    = generate_signal(df)
        price  = sig["price"]
        evaluate_past_signals(pair, tf, price)
        signal_history[pair][tf].append({
            "signal":    sig["signal_type"],
            "price":     price,
            "timestamp": sig["timestamp"],
            "outcome":   "PENDING",
            "pct_change": 0,
            "features":  sig["features"],
        })
        log_entry = {
            "pair":   pair, "tf": tf,
            "signal": sig["signal_type"],
            "label":  sig["signal_label"],
            "price":  price,
            "time":   datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "score":  sig["blended_score"],
        }
        signal_log.appendleft(log_entry)
        sig["ticker"] = {
            "change_pct": round(float(ticker.get("priceChangePercent",0)),2),
            "high":       round(float(ticker.get("highPrice",0)),6),
            "low":        round(float(ticker.get("lowPrice",0)),6),
            "volume":     round(float(ticker.get("volume",0)),2),
            "quote_vol":  round(float(ticker.get("quoteVolume",0)),2),
        }
        sig["active_users"] = get_active_user_count()
        # Broadcast signal to SSE clients
        sse_broadcast("signal_update", {
            "pair": pair, "tf": tf,
            "signal": sig["signal_type"],
            "score":  sig["blended_score"],
            "price":  price,
            "ai_confidence": sig["ai_confidence"],
        })
        return jsonify(sig)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/chart")
def api_chart():
    pair = request.args.get("pair","BTC").upper()
    tf   = request.args.get("tf","15m")
    if pair not in PAIRS or tf not in TIMEFRAMES:
        return jsonify({"error":"Invalid"}), 400
    try:
        symbol = PAIRS[pair]
        df     = fetch_klines(symbol, tf)
        data   = build_chart_data(df)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/all_signals")
def api_all_signals():
    pair = request.args.get("pair","BTC").upper()
    sid  = request.args.get("sid","")
    if pair not in PAIRS:
        return jsonify({"error":"Invalid pair"}), 400
    if sid:
        register_session(sid)
    symbol  = PAIRS[pair]
    results = {}
    try:
        ticker = fetch_ticker(symbol)
    except Exception:
        ticker = {}
    for tf in TIMEFRAMES:
        try:
            df  = fetch_klines(symbol, tf)
            sig = generate_signal(df)
            evaluate_past_signals(pair, tf, sig["price"])
            signal_history[pair][tf].append({
                "signal":    sig["signal_type"],
                "price":     sig["price"],
                "timestamp": sig["timestamp"],
                "outcome":   "PENDING",
                "pct_change": 0,
                "features":  sig["features"],
            })
            results[tf] = sig
        except Exception as e:
            results[tf] = {"error": str(e)}
    return jsonify({
        "pair":     pair,
        "results":  results,
        "ticker": {
            "change_pct": round(float(ticker.get("priceChangePercent",0)),2),
            "high":       round(float(ticker.get("highPrice",0)),6),
            "low":        round(float(ticker.get("lowPrice",0)),6),
            "volume":     round(float(ticker.get("volume",0)),2),
        },
        "active_users": get_active_user_count(),
    })

@app.route("/api/success_rate")
def api_success_rate():
    pair = request.args.get("pair","BTC").upper()
    tf   = request.args.get("tf","15m")
    if pair not in PAIRS or tf not in TIMEFRAMES:
        return jsonify({"error":"Invalid"}), 400
    return jsonify(get_success_stats(pair, tf))

@app.route("/api/log")
def api_log():
    return jsonify(list(signal_log)[:50])

@app.route("/api/pairs")
def api_pairs():
    return jsonify(list(PAIRS.keys()))

@app.route("/api/ai_info")
def api_ai_info():
    global_acc = round(ai_model.accuracy.get() * 100, 1) if ai_model.trained_samples >= 10 else 0
    return jsonify({
        "active":          ai_model.initialized,
        "trained_samples": ai_model.trained_samples,
        "global_accuracy": global_acc,
        "pair_stats":      ai_model.pair_stats,
        "active_users":    get_active_user_count(),
    })

# ─────────────────────────────────────────────
# BACKGROUND: broadcast user count every 10s
# ─────────────────────────────────────────────
def user_count_broadcaster():
    while True:
        time.sleep(10)
        try:
            sse_broadcast("user_count", {"active_users": get_active_user_count()})
        except Exception:
            pass

threading.Thread(target=user_count_broadcaster, daemon=True).start()

if __name__ == "__main__":
    print("\n  ╔══════════════════════════════════════════════════════╗")
    print("  ║  CryptoSignal v3.0 — Online AI + ZEC + SSE Stream   ║")
    print("  ║  New: ZEC · Real-time SSE · Active User Counter      ║")
    print("  ║  Open: http://127.0.0.1:5000                         ║")
    print("  ╚══════════════════════════════════════════════════════╝\n")
    app.run(debug=True, host="0.0.0.0", port=5000, threaded=True)