# CryptoSignal — Flask Trading Indicator Dashboard

A full-featured real-time crypto trading indicator web app.
Uses Binance public API — **no API key required**.

---

## Features

- **6 Pairs**: BTC, ETH, BNB, SOL, PAXG, DOGE (/USDT)
- **4 Timeframes**: 15m, 1h, 4h, 1d — switch instantly
- **7 Indicators**: RSI, MACD, EMA Cross, Bollinger Bands, StochRSI, Momentum, Volume
- **Candlestick chart** with EMA9/21/50 + Bollinger Bands overlays
- **Sub-charts**: RSI, MACD histogram, StochRSI, Volume
- **Signal success rate tracker** — WIN/LOSS recorded as price moves ±1%
- **Multi-timeframe consensus** strip
- **Key levels**: Support, Resistance, BB bands, EMA50
- **Live signal log** — recent signals across all pairs/timeframes
- **Plain-English explanations** for every signal
- Auto-refresh every 30 seconds

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the app

```bash
python app.py
```

### 3. Open in browser

```
http://127.0.0.1:5000
```

---

## Project Structure

```
crypto_app/
├── app.py                  # Flask backend, signal engine, API routes
├── requirements.txt
├── README.md
├── templates/
│   └── index.html          # Main dashboard HTML
└── static/
    ├── css/
    │   └── style.css       # Dark terminal UI styles
    └── js/
        └── app.js          # Frontend JS — charts, state, API calls
```

---

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Dashboard UI |
| `GET /api/signal?pair=BTC&tf=15m` | Signal for one pair + timeframe |
| `GET /api/all_signals?pair=BTC` | Signals for all 4 timeframes at once |
| `GET /api/chart?pair=BTC&tf=15m` | Full chart data (OHLCV + all indicators) |
| `GET /api/success_rate?pair=BTC&tf=15m` | Signal accuracy stats |
| `GET /api/log` | Recent signal log (last 50) |
| `GET /api/pairs` | Available pairs list |

---

## Signal Types

| Signal | Meaning |
|---|---|
| **STRONG BUY** | 6+ score difference — very strong bullish confluence |
| **BUY** | 3-5 score difference — bullish lean |
| **NEUTRAL** | Mixed signals — wait for a clear setup |
| **SELL** | 3-5 score difference — bearish lean |
| **STRONG SELL** | 6+ score difference — strong bearish |
| **STAY AWAY** | Volatility >6% + mixed signals — too dangerous |

---

## Indicator Weights

| Indicator | Max Points | Notes |
|---|---|---|
| RSI | 3 | Extreme readings (25/75) = 3pts |
| MACD | 3 | Crossovers weighted higher |
| EMA Cross | 3 | Golden/death cross = 3pts |
| EMA50 Filter | 1 | Trend direction baseline |
| Bollinger Bands | 3 | Band extremes = 3pts |
| Momentum (10-period) | 2 | >3% move = 2pts |
| StochRSI | 2 | Extreme readings |
| Volume | 2 | >2.5x surge |

---

## Notes

- Data is cached for 15 seconds to avoid rate limiting
- Success rate requires signals to be tracked over time; starts empty
- WIN = price moved ≥1% in signal direction
- LOSS = price moved ≥1% against signal direction
- PENDING = not yet resolved

---

## Keyboard Shortcuts (in the UI)

- Click **pair buttons** to switch asset
- Click **TF tabs** (15m/1h/4h/1d) to switch signal timeframe
- Click **chart TF tabs** to change chart timeframe independently
- Click **RSI/MACD/StochRSI/Volume** tabs to change sub-chart
- Toggle **EMA/BB/Vol** checkboxes to show/hide chart overlays
