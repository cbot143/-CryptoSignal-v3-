/* ═══════════════════════════════════════════════════════════════
   CryptoSignal v3 — Frontend JS
   New: SSE real-time stream · Active user counter · ZEC support
        AI learning flow display · Session ping
   ═══════════════════════════════════════════════════════════════ */

'use strict';

// ─── SESSION ID (persistent per browser tab) ─────────────────
const SESSION_ID = (() => {
  let sid = sessionStorage.getItem('cs_sid');
  if (!sid) {
    sid = 'u_' + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
    sessionStorage.setItem('cs_sid', sid);
  }
  return sid;
})();

// ─── STATE ───────────────────────────────────────────────────
const state = {
  pair:        'BTC',
  tf:          '15m',
  chartTf:     '15m',
  subChart:    'rsi',
  chartData:   null,
  signal:      null,
  allSignals:  {},
  aiInfo:      {},
  loading:     false,
  activeUsers: 1,
  signalTimestamp: null,  // for signal age timer
};

// ─── DOM CACHE ───────────────────────────────────────────────
const dom = {
  tickerPrice:   document.getElementById('tickerPrice'),
  tickerChange:  document.getElementById('tickerChange'),
  tickerHigh:    document.getElementById('tickerHigh'),
  tickerLow:     document.getElementById('tickerLow'),
  tickerVol:     document.getElementById('tickerVol'),
  tickerAtr:     document.getElementById('tickerAtr'),
  tickerVwap:    document.getElementById('tickerVwap'),
  tickerWillR:   document.getElementById('tickerWillR'),
  tickerVola:    document.getElementById('tickerVola'),

  signalBanner:  document.getElementById('signalBanner'),
  signalLabel:   document.getElementById('signalLabel'),
  signalEnglish: document.getElementById('signalEnglish'),
  bullCount:     document.getElementById('bullCount'),
  bearCount:     document.getElementById('bearCount'),

  scoreRing:     document.getElementById('scoreRing'),
  scoreText:     document.getElementById('scoreText'),
  blendedRing:   document.getElementById('blendedRing'),
  blendedText:   document.getElementById('blendedText'),

  sdbBullFill:   document.getElementById('sdbBullFill'),
  sdbBearFill:   document.getElementById('sdbBearFill'),
  sdbBullVal:    document.getElementById('sdbBullVal'),
  sdbBearVal:    document.getElementById('sdbBearVal'),

  consensus:     document.getElementById('consensusLabel'),

  indicatorGrid: document.getElementById('indicatorGrid'),
  reasonsList:   document.getElementById('reasonsList'),
  warningsList:  document.getElementById('warningsList'),
  indTf:         document.getElementById('indTf'),

  lvlHigh:       document.getElementById('lvlHigh'),
  lvlLow:        document.getElementById('lvlLow'),
  lvlEma50:      document.getElementById('lvlEma50'),
  lvlBbUp:       document.getElementById('lvlBbUp'),
  lvlBbMid:      document.getElementById('lvlBbMid'),
  lvlBbLow:      document.getElementById('lvlBbLow'),
  lvlVwap:       document.getElementById('lvlVwap'),
  lvlCloudTop:   document.getElementById('lvlCloudTop'),
  lvlCloudBot:   document.getElementById('lvlCloudBot'),

  setupEntry:    document.getElementById('setupEntry'),
  setupStop:     document.getElementById('setupStop'),
  setupTarget:   document.getElementById('setupTarget'),
  setupRR:       document.getElementById('setupRR'),
  setupTf:       document.getElementById('setupTf'),

  aiProgressBar:    document.getElementById('aiProgressBar'),
  aiValue:          document.getElementById('aiValue'),
  aiLabel:          document.getElementById('aiLabel'),
  aiTf:             document.getElementById('aiTf'),
  aiStatSamples:    document.getElementById('aiStatSamples'),
  aiStatGlobalAcc:  document.getElementById('aiStatGlobalAcc'),
  aiStatWeight:     document.getElementById('aiStatWeight'),
  aiStatBlended:    document.getElementById('aiStatBlended'),
  aiPairAcc:        document.getElementById('aiPairAcc'),
  aiMiniBadge:      document.getElementById('aiSamplesBadge'),

  // NEW: AI learning log & user counter
  aiLearnLog:    document.getElementById('aiLearnLog'),
  userCountBadge:document.getElementById('userCountBadge'),

  srPct:         document.getElementById('srPct'),
  srSub:         document.getElementById('srSub'),
  srWinFill:     document.getElementById('srWinFill'),
  srLossFill:    document.getElementById('srLossFill'),
  srWins:        document.getElementById('srWins'),
  srLoss:        document.getElementById('srLoss'),
  srPend:        document.getElementById('srPend'),
  srByType:      document.getElementById('srByType'),
  srTf:          document.getElementById('srTf'),

  logList:       document.getElementById('logList'),

  ichiTenkan:      document.getElementById('ichiTenkan'),
  ichiKijun:       document.getElementById('ichiKijun'),
  ichiSenkouA:     document.getElementById('ichiSenkouA'),
  ichiSenkouB:     document.getElementById('ichiSenkouB'),
  ichiCloudStatus: document.getElementById('ichiCloudStatus'),
  ichiVwap:        document.getElementById('ichiVwap'),
  ichiWillR:       document.getElementById('ichiWillR'),

  signalAgeBadge:   document.getElementById('signalAgeBadge'),
  signalAgeText:    document.getElementById('signalAgeText'),
  copyAllBtn:       document.getElementById('copyAllBtn'),
  tooltipPortal:    document.getElementById('tooltipPortal'),

  refreshBtn:    document.getElementById('refreshBtn'),
  lastUpdate:    document.getElementById('lastUpdate'),
};

// ─── CHART INSTANCES ─────────────────────────────────────────
let priceChartInstance = null;
let candleSeries       = null;
let ema9Series = null, ema21Series = null, ema50Series = null;
let bbUpperSeries = null, bbMidSeries = null, bbLowerSeries = null;
let vwapSeries = null;
let ichTenkanSeries = null, ichKijunSeries = null;
let ichSenkouASeries = null, ichSenkouBSeries = null;

let subChartInstance = null;
const subSeries = {
  rsi:    { main: null, ob: null, os: null },
  macd:   { macd: null, signal: null, hist: null },
  stoch:  { k: null, d: null, ob: null, os: null },
  willr:  { main: null, ob: null, os: null },
  volume: { hist: null },
};

let pollTimer    = null;
let sseSource    = null;
let pingInterval = null;

const tfTabs   = document.querySelectorAll('.tf-tab');
const chtTabs  = document.querySelectorAll('.cht-tf');
const subTabs  = document.querySelectorAll('.sub-tab');
const mtCards  = document.querySelectorAll('.mt-card');
const pairBtns = document.querySelectorAll('.pair-btn');
const toggleEma  = document.getElementById('togEma');
const toggleBb   = document.getElementById('togBb');
const toggleVwap = document.getElementById('togVwap');
const toggleIch  = document.getElementById('togIch');
const toggleVol  = document.getElementById('togVol');

// ─── INIT ────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  bindPairButtons();
  bindTfTabs();
  bindChartTfTabs();
  bindSubChartTabs();
  bindToggles();
  bindMtCards();
  initPriceChart();
  initSubChart();
  fetchAll();
  startPolling();
  startSSE();
  startPing();
  startSignalAgeTimer();
  initTooltips();
  initCopyButtons();

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
    } else {
      startPolling();
      fetchAll();
    }
  });
});

// ─── SESSION PING (keep user count alive) ────────────────────
function startPing() {
  if (pingInterval) clearInterval(pingInterval);
  // Immediate first ping
  fetch(`/api/ping?sid=${SESSION_ID}`).then(r => r.json()).then(d => {
    updateUserCount(d.active_users);
  }).catch(() => {});
  // Then every 30s
  pingInterval = setInterval(() => {
    fetch(`/api/ping?sid=${SESSION_ID}`).then(r => r.json()).then(d => {
      updateUserCount(d.active_users);
    }).catch(() => {});
  }, 30000);
}

function updateUserCount(n) {
  state.activeUsers = n || 1;
  if (dom.userCountBadge) {
    dom.userCountBadge.textContent = n;
    dom.userCountBadge.title = `${n} active user${n !== 1 ? 's' : ''} right now`;
  }
}

// ─── SSE REAL-TIME STREAM ────────────────────────────────────
function startSSE() {
  if (sseSource) { sseSource.close(); sseSource = null; }
  try {
    sseSource = new EventSource('/api/stream');

    sseSource.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        handleSSEMessage(msg);
      } catch(e) {}
    };

    sseSource.onerror = () => {
      // Reconnect after 5s
      sseSource.close();
      setTimeout(startSSE, 5000);
    };
  } catch(e) {
    console.warn('SSE not available, using polling only');
  }
}

function handleSSEMessage(msg) {
  switch (msg.type) {

    case 'ai_learn':
      // Flash the AI panel and add to learn log
      appendAILearnLog(msg.data);
      if (dom.aiStatSamples) dom.aiStatSamples.textContent = msg.data.trained_samples;
      if (dom.aiMiniBadge)   dom.aiMiniBadge.textContent   = msg.data.trained_samples;
      if (dom.aiStatGlobalAcc && msg.data.global_acc != null)
        dom.aiStatGlobalAcc.textContent = msg.data.global_acc + '%';
      // Pulse the AI panel
      const aiPanel = document.querySelector('.ai-panel');
      if (aiPanel) {
        aiPanel.classList.add('ai-flash');
        setTimeout(() => aiPanel.classList.remove('ai-flash'), 700);
      }
      break;

    case 'user_count':
      updateUserCount(msg.data.active_users);
      break;

    case 'signal_update':
      // Optionally flash the live badge
      const liveDot = document.querySelector('.live-dot');
      if (liveDot) {
        liveDot.style.opacity = '0';
        setTimeout(() => { liveDot.style.opacity = '1'; }, 200);
      }
      break;

    case 'connected':
      console.log('[SSE] Connected, cid:', msg.cid);
      break;
  }
}

// ─── AI LEARN LOG ────────────────────────────────────────────
const _aiLearnBuf = [];  // max 30 entries shown

function appendAILearnLog(entry) {
  if (!dom.aiLearnLog) return;

  _aiLearnBuf.unshift(entry);
  if (_aiLearnBuf.length > 30) _aiLearnBuf.pop();

  dom.aiLearnLog.innerHTML = _aiLearnBuf.map(e => {
    const cls = e.outcome === 'WIN' ? 'al-win' : 'al-loss';
    return `<div class="al-row ${cls}">
      <span class="al-time">${e.time}</span>
      <span class="al-badge">#${e.sample}</span>
      <span class="al-pair">${e.pair}/${e.tf}</span>
      <span class="al-outcome ${cls}">${e.outcome}</span>
      <span class="al-acc">Acc: ${e.global_acc}%</span>
    </div>`;
  }).join('');
}

// ─── POLLING ─────────────────────────────────────────────────
function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(fetchAll, 30000);
}

function batchUpdate(fn) { requestAnimationFrame(fn); }

// ─── FETCH ALL ───────────────────────────────────────────────
async function fetchAll() {
  setRefreshSpinning(true);
  try {
    await Promise.all([fetchAllSignals(), fetchChartData(), fetchAiInfo()]);
    fetchSuccessRate();
    fetchLog();
    updateLastUpdate();
  } catch(e) {
    console.error('fetchAll error:', e);
  }
  setRefreshSpinning(false);
}

async function fetchAllSignals() {
  try {
    const res  = await fetch(`/api/all_signals?pair=${state.pair}&sid=${SESSION_ID}`);
    const data = await res.json();
    state.allSignals = data.results || {};
    if (data.active_users != null) updateUserCount(data.active_users);
    batchUpdate(() => {
      updateTickerBar(data);
      updateMtStrip(data.results);
      updateSignalBanner(state.allSignals[state.tf]);
      updateIndicatorGrid(state.allSignals[state.tf]);
      updateReasons(state.allSignals[state.tf]);
      updateLevels(state.allSignals[state.tf]);
      updateTradeSetup(state.allSignals[state.tf]);
      updateAIPanel(state.allSignals[state.tf]);
      updateIchiStrip(state.allSignals[state.tf]);
    });
  } catch(e) { console.error('fetchAllSignals error:', e); }
}

async function fetchChartData() {
  try {
    const res = await fetch(`/api/chart?pair=${state.pair}&tf=${state.chartTf}`);
    state.chartData = await res.json();
    renderPriceChart(state.chartData);
    updateSubChart(state.subChart, state.chartData);
  } catch(e) { console.error('fetchChartData error:', e); }
}

async function fetchSuccessRate() {
  try {
    const res  = await fetch(`/api/success_rate?pair=${state.pair}&tf=${state.tf}`);
    const data = await res.json();
    updateSuccessRate(data);
  } catch(e) {}
}

async function fetchLog() {
  try {
    const res  = await fetch('/api/log');
    const data = await res.json();
    updateLog(data);
  } catch(e) {}
}

async function fetchAiInfo() {
  try {
    const res  = await fetch('/api/ai_info');
    state.aiInfo = await res.json();
    if (dom.aiMiniBadge) dom.aiMiniBadge.textContent = state.aiInfo.trained_samples || '0';
    if (state.aiInfo.active_users != null) updateUserCount(state.aiInfo.active_users);
  } catch(e) {}
}

// ─── BINDINGS ────────────────────────────────────────────────
function bindPairButtons() {
  pairBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      state.pair = btn.dataset.pair;
      pairBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      fetchAll();
    });
  });
}

function bindTfTabs() {
  tfTabs.forEach(tab => {
    tab.addEventListener('click', () => {
      state.tf = tab.dataset.tf;
      tfTabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      if (state.allSignals[state.tf]) {
        batchUpdate(() => {
          updateSignalBanner(state.allSignals[state.tf]);
          updateIndicatorGrid(state.allSignals[state.tf]);
          updateReasons(state.allSignals[state.tf]);
          updateLevels(state.allSignals[state.tf]);
          updateTradeSetup(state.allSignals[state.tf]);
          updateAIPanel(state.allSignals[state.tf]);
          updateIchiStrip(state.allSignals[state.tf]);
        });
      }
      fetchSuccessRate();
      updateSrTfBadge();
    });
  });
}

function bindChartTfTabs() {
  chtTabs.forEach(tab => {
    tab.addEventListener('click', () => {
      state.chartTf = tab.dataset.tf;
      chtTabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      fetchChartData();
    });
  });
}

function bindSubChartTabs() {
  subTabs.forEach(tab => {
    tab.addEventListener('click', () => {
      state.subChart = tab.dataset.chart;
      subTabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      if (state.chartData) updateSubChart(state.subChart, state.chartData);
    });
  });
}

function bindMtCards() {
  mtCards.forEach(card => {
    card.addEventListener('click', () => {
      const tf = card.id.replace('mt-','');
      state.tf = tf;
      tfTabs.forEach(t => t.classList.toggle('active', t.dataset.tf === tf));
      if (state.allSignals[tf]) {
        batchUpdate(() => {
          updateSignalBanner(state.allSignals[tf]);
          updateIndicatorGrid(state.allSignals[tf]);
          updateReasons(state.allSignals[tf]);
          updateLevels(state.allSignals[tf]);
          updateTradeSetup(state.allSignals[tf]);
          updateAIPanel(state.allSignals[tf]);
          updateIchiStrip(state.allSignals[tf]);
        });
      }
      fetchSuccessRate();
      updateSrTfBadge();
    });
  });
}

function bindToggles() {
  [toggleEma, toggleBb, toggleVwap, toggleIch, toggleVol].forEach(tog => {
    tog?.addEventListener('change', () => {
      if (state.chartData) renderPriceChart(state.chartData);
    });
  });
}

// ─── UPDATE FUNCTIONS ────────────────────────────────────────

function updateTickerBar(data) {
  const ticker = data.ticker || {};
  const sig    = data.results?.['15m'] || {};
  const price  = sig.price || 0;
  const pair   = state.pair;

  dom.tickerPrice.textContent  = fmtPrice(price, pair);
  const ch = ticker.change_pct || 0;
  dom.tickerChange.textContent = (ch >= 0 ? '+' : '') + ch.toFixed(2) + '%';
  dom.tickerChange.className   = 'ticker-val ' + (ch >= 0 ? 'up' : 'down');
  dom.tickerHigh.textContent   = fmtPrice(ticker.high || 0, pair);
  dom.tickerLow.textContent    = fmtPrice(ticker.low  || 0, pair);
  dom.tickerVol.textContent    = fmtVol(ticker.volume || 0);
  if (sig.atr !== undefined)       dom.tickerAtr.textContent  = fmtPrice(sig.atr, pair);
  if (sig.vwap !== undefined)      dom.tickerVwap.textContent = fmtPrice(sig.vwap, pair);
  if (sig.williams_r !== undefined) {
    dom.tickerWillR.textContent = sig.williams_r.toFixed(1);
    dom.tickerWillR.className   = 'ticker-val ' + (sig.williams_r < -80 ? 'up' : sig.williams_r > -20 ? 'down' : '');
  }
  if (sig.vola_pct !== undefined) {
    dom.tickerVola.textContent = sig.vola_pct.toFixed(2) + '%';
    dom.tickerVola.className   = 'ticker-val ' + (sig.vola_pct > 4 ? 'down' : sig.vola_pct > 2 ? '' : 'up');
  }
}

function updateMtStrip(results) {
  if (!results) return;
  const tfs = ['15m','1h','4h','1d'];
  // Traffic-light class map
  const tlMt = {
    STRONG_BUY:  'sig-strong-buy',
    BUY:         'sig-buy',
    STRONG_SELL: 'sig-strong-sell',
    SELL:        'sig-sell',
    NEUTRAL:     'sig-warning',
  };
  let totalBull = 0, totalBear = 0;
  tfs.forEach(tf => {
    const sig  = results[tf];
    const card = document.getElementById(`mt-${tf}`);
    if (!card || !sig || sig.error) return;
    card.querySelector('.mt-sig').textContent   = sig.signal_label || '—';
    card.querySelector('.mt-score').textContent = `${sig.blended_score || sig.score || 0} / 100`;
    const tlClass = tlMt[sig.signal_type] || ('sig-' + (sig.color || 'neutral'));
    card.className = `mt-card ${tlClass}`;
    if (tf === state.tf) card.classList.add('active');
    totalBull += (sig.bull || 0);
    totalBear += (sig.bear || 0);
  });
  const diff = totalBull - totalBear;
  let con = '—';
  if      (diff >= 8)  con = 'BULLISH';
  else if (diff >= 4)  con = 'SLIGHT BULL';
  else if (diff <= -8) con = 'BEARISH';
  else if (diff <= -4) con = 'SLIGHT BEAR';
  else                  con = 'MIXED';
  dom.consensus.textContent = con;
  dom.consensus.className   = 'mt-con-val ' + (diff >= 4 ? 'up' : diff <= -4 ? 'down' : 'neutral');
}

function updateSignalBanner(sig) {
  if (!sig || sig.error) return;

  // ── Traffic-light class mapping ──
  const tlMap = {
    STRONG_BUY:  'tl-strong-buy',
    BUY:         'tl-buy',
    STRONG_SELL: 'tl-strong-sell',
    SELL:        'tl-sell',
    NEUTRAL:     'tl-neutral',
  };
  const tlClass = tlMap[sig.signal_type] || 'tl-neutral';
  const legacyClass = sig.color || 'neutral';
  dom.signalBanner.className = `signal-banner ${legacyClass} ${tlClass}`;

  dom.signalLabel.textContent   = sig.signal_label  || '—';
  dom.signalEnglish.textContent = sig.plain_english  || '—';
  dom.bullCount.textContent     = sig.bull  ?? '—';
  dom.bearCount.textContent     = sig.bear  ?? '—';

  const score   = sig.score || 0;
  const circum  = 213.6;
  dom.scoreRing.style.strokeDashoffset  = circum - (score / 100) * circum;
  dom.scoreText.textContent             = score;

  const blended = sig.blended_score || score;
  dom.blendedRing.style.strokeDashoffset = circum - (blended / 100) * circum;
  dom.blendedText.textContent            = blended;

  // ── Strength meter (percentage bar) ──
  const total = (sig.bull || 0) + (sig.bear || 0);
  if (total > 0) {
    const bullPct = Math.round((sig.bull / total) * 100);
    const bearPct = 100 - bullPct;
    dom.sdbBullFill.style.width = bullPct + '%';
    dom.sdbBearFill.style.width = bearPct + '%';
    dom.sdbBullVal.textContent  = sig.bull;
    dom.sdbBearVal.textContent  = sig.bear;
  }

  // ── Record timestamp for signal age timer ──
  state.signalTimestamp = Date.now();
  updateSignalAge();
}

// ── Tooltip definitions for technical terms ──
const IND_TIPS = {
  rsi:        'Relative Strength Index (0–100). Above 70 = overbought, below 30 = oversold.',
  macd:       'Moving Average Convergence Divergence. Positive histogram = bullish momentum building.',
  ema:        'EMA Cross: When fast EMA crosses above slow EMA it signals a trend change.',
  bb:         'Bollinger Bands % — how far price sits within the bands. 100% = upper band, 0% = lower.',
  stoch:      'Stochastic RSI momentum. Above 80 = overbought zone, below 20 = oversold zone.',
  momentum:   'Rate of price change over recent candles. Positive = price accelerating upward.',
  volume:     'Current volume vs. its 20-bar average. 2x means twice normal activity — confirms moves.',
  vwap:       'Volume Weighted Average Price — the "fair value" price for the session. Above it is bullish.',
  ichimoku:   'Ichimoku Cloud: Price above the cloud = bullish trend. Cloud color shows future support/resistance.',
  williams_r: 'Williams %R: Ranges -100 to 0. Near 0 = overbought (potential sell), near -100 = oversold (potential buy).',
  score:      'Blended score combining all 10 indicators + AI confidence. 0–100 scale.',
};

function updateIndicatorGrid(sig) {
  if (!sig || sig.error || !dom.indicatorGrid) return;
  if (dom.indTf) dom.indTf.textContent = state.tf;
  const ist = sig.indicator_states || {};
  const indicators = [
    { key:'rsi',        name:'RSI (14)',      val: sig.rsi?.toFixed(1),                          state: ist.rsi },
    { key:'macd',       name:'MACD Hist',     val: sig.macd_hist?.toExponential(2),               state: ist.macd },
    { key:'ema',        name:'EMA Cross',     val: ist.ema?.label,                                state: ist.ema },
    { key:'bb',         name:'Bollinger %B',  val: sig.bb_pos?.toFixed(1) + '%',                  state: ist.bb },
    { key:'stoch',      name:'StochRSI K',    val: sig.stoch_k?.toFixed(1),                      state: ist.stoch },
    { key:'momentum',   name:'Momentum',      val: sig.momentum_pct?.toFixed(2) + '%',            state: ist.momentum },
    { key:'volume',     name:'Volume Ratio',  val: sig.vol_ratio?.toFixed(2) + 'x',              state: ist.volume },
    { key:'vwap',       name:'VWAP Δ',        val: (sig.price_vs_vwap_pct?.toFixed(2) || '—') + '%', state: ist.vwap, isNew: true },
    { key:'ichimoku',   name:'Ichimoku',      val: ist.ichimoku?.label || '—',                    state: ist.ichimoku, isNew: true },
    { key:'williams_r', name:'Williams %R',   val: sig.williams_r?.toFixed(1),                    state: ist.williams_r, isNew: true },
    { key:'score',      name:'Overall Score', val: (sig.blended_score || sig.score) + '/100',
      state: { label: sig.signal_label, dir: sig.color === 'success' ? 'bull' : sig.color === 'danger' ? 'bear' : 'neutral',
               value: sig.blended_score || sig.score } },
  ];

  dom.indicatorGrid.innerHTML = indicators.map(ind => {
    const st    = ind.state || {};
    const dir   = st.dir || 'neutral';
    const pct   = getBarPct(ind.key, st.value, sig);
    const color = dir === 'bull' ? '#00e676' : dir === 'bear' ? '#ff1744' : '#484f58';
    const cls   = ind.isNew ? 'ind-item new-ind' : 'ind-item';
    const tip   = IND_TIPS[ind.key] || '';
    return `
      <div class="${cls}" data-tip="${tip}">
        <div class="ind-name">${ind.name}</div>
        <div class="ind-val">${ind.val || '—'}</div>
        <div class="ind-bar-bg">
          <div class="ind-bar-fill" style="width:${pct}%;background:${color};"></div>
        </div>
        <div class="ind-state ${dir}">${st.label || '—'}</div>
      </div>`;
  }).join('');
}

function getBarPct(key, value, sig) {
  switch(key) {
    case 'rsi':        return Math.max(0, Math.min(100, value || sig?.rsi || 50));
    case 'macd':       return Math.max(0, Math.min(100, 50 + (value || 0) * 3000));
    case 'bb':         return Math.max(0, Math.min(100, sig?.bb_pos || 50));
    case 'stoch':      return Math.max(0, Math.min(100, value || sig?.stoch_k || 50));
    case 'momentum':   return Math.max(0, Math.min(100, 50 + (sig?.momentum_pct || 0) * 5));
    case 'volume':     return Math.max(0, Math.min(100, Math.min((sig?.vol_ratio || 1) * 33, 100)));
    case 'score':      return sig?.blended_score || sig?.score || 50;
    case 'vwap':       return Math.max(0, Math.min(100, 50 + (sig?.price_vs_vwap_pct || 0) * 10));
    case 'williams_r': return Math.max(0, Math.min(100, 100 + (value || -50)));
    case 'ichimoku':   return value > 0 ? Math.min(100, 60 + value * 100) : Math.max(0, 40 + value * 100);
    default:           return 50;
  }
}

function updateReasons(sig) {
  if (!sig || sig.error) return;
  const reasons = sig.reasons || [];
  const isUp    = ['STRONG_BUY','BUY'].includes(sig.signal_type);
  const isDn    = ['STRONG_SELL','SELL'].includes(sig.signal_type);
  dom.reasonsList.innerHTML = reasons.length
    ? reasons.map(r => `<div class="reason-item ${isUp?'bull':isDn?'bear':'neutral'}">${r}</div>`).join('')
    : `<div class="reason-item neutral">No dominant signals — mixed market</div>`;
  dom.warningsList.innerHTML = (sig.warnings || [])
    .map(w => `<div class="warning-item">${w}</div>`).join('');
}

function updateLevels(sig) {
  if (!sig || sig.error) return;
  const pair = state.pair;
  dom.lvlHigh.textContent  = fmtPrice(sig.recent_high, pair);
  dom.lvlLow.textContent   = fmtPrice(sig.recent_low,  pair);
  dom.lvlEma50.textContent = fmtPrice(sig.ema50,        pair);
  dom.lvlBbUp.textContent  = fmtPrice(sig.bb_upper,     pair);
  dom.lvlBbMid.textContent = fmtPrice(sig.bb_mid,       pair);
  dom.lvlBbLow.textContent = fmtPrice(sig.bb_lower,     pair);
  dom.lvlVwap.textContent  = fmtPrice(sig.vwap,         pair);
  if (sig.ichimoku) {
    dom.lvlCloudTop.textContent = fmtPrice(sig.ichimoku.cloud_top, pair);
    dom.lvlCloudBot.textContent = fmtPrice(sig.ichimoku.cloud_bot, pair);
    dom.lvlCloudTop.className   = 'level-val ' + (sig.ichimoku.cloud_bullish ? 'up' : '');
    dom.lvlCloudBot.className   = 'level-val ' + (sig.ichimoku.cloud_bullish ? '' : 'down');
  }
}

function updateIchiStrip(sig) {
  if (!sig || sig.error) return;
  const pair = state.pair;
  if (sig.ichimoku) {
    dom.ichiTenkan.textContent      = fmtPrice(sig.ichimoku.tenkan,   pair);
    dom.ichiKijun.textContent       = fmtPrice(sig.ichimoku.kijun,    pair);
    dom.ichiSenkouA.textContent     = fmtPrice(sig.ichimoku.senkou_a, pair);
    dom.ichiSenkouB.textContent     = fmtPrice(sig.ichimoku.senkou_b, pair);
    const bullishCloud = sig.ichimoku.cloud_bullish;
    dom.ichiCloudStatus.textContent = bullishCloud ? '🟢 BULL' : '🔴 BEAR';
    dom.ichiCloudStatus.className   = 'ichi-val ' + (bullishCloud ? 'up' : 'down');
  }
  if (sig.vwap) dom.ichiVwap.textContent = fmtPrice(sig.vwap, pair);
  if (sig.williams_r !== undefined) {
    dom.ichiWillR.textContent = sig.williams_r.toFixed(1);
    dom.ichiWillR.className   = 'ichi-val ' + (sig.williams_r < -80 ? 'up' : sig.williams_r > -20 ? 'down' : '');
  }
}

function updateTradeSetup(sig) {
  if (!sig || sig.error) {
    dom.setupEntry.textContent = dom.setupStop.textContent = dom.setupTarget.textContent = dom.setupRR.textContent = '—';
    _resetQeRows();
    return;
  }
  const pair   = state.pair;
  const isSell = ['STRONG_SELL', 'SELL'].includes(sig.signal_type);
  const isBuy  = ['STRONG_BUY',  'BUY' ].includes(sig.signal_type);

  const entryPrice = sig.price;
  const slPrice    = sig.stop_loss;
  const tpPrice    = sig.take_profit;

  // ── Sanity-check: warn if levels are inverted for direction ──
  // For SELL: SL must be above entry, TP must be below entry
  // For BUY:  SL must be below entry, TP must be above entry
  let slValid = true, tpValid = true;
  if (isSell && slPrice && entryPrice) slValid = slPrice > entryPrice;
  if (isSell && tpPrice && entryPrice) tpValid = tpPrice < entryPrice;
  if (isBuy  && slPrice && entryPrice) slValid = slPrice < entryPrice;
  if (isBuy  && tpPrice && entryPrice) tpValid = tpPrice > entryPrice;

  dom.setupEntry.textContent  = fmtPrice(entryPrice, pair);
  dom.setupStop.textContent   = slPrice   ? fmtPrice(slPrice,  pair) : '—';
  dom.setupTarget.textContent = tpPrice   ? fmtPrice(tpPrice,  pair) : '—';

  // ── Colour the QE rows correctly by direction ──
  const qeEntry = document.querySelector('.qe-entry');
  const qeSl    = document.querySelector('.qe-sl');
  const qeTp    = document.querySelector('.qe-tp');
  const qeSlLbl = document.querySelector('.qe-sl .qe-label');
  const qeTpLbl = document.querySelector('.qe-tp .qe-label');

  if (qeEntry) {
    qeEntry.className = 'qe-row qe-entry';  // always neutral entry color
  }
  if (qeSl) {
    // SL is always the "danger" side — red for buy (loss if hit below), red for sell too
    qeSl.className = `qe-row qe-sl${slValid ? '' : ' qe-invalid'}`;
    if (qeSlLbl) qeSlLbl.textContent = 'STOP LOSS';
  }
  if (qeTp) {
    qeTp.className = `qe-row qe-tp${tpValid ? '' : ' qe-invalid'}`;
    if (qeTpLbl) qeTpLbl.textContent = 'TAKE PROFIT';
  }

  // ── Direction badge next to panel title ──
  const dirBadge = document.getElementById('qeDirectionBadge');
  if (dirBadge) {
    if (isSell) {
      dirBadge.textContent = '▼ SHORT / SELL';
      dirBadge.className   = 'qe-dir-badge qe-dir-sell';
    } else if (isBuy) {
      dirBadge.textContent = '▲ LONG / BUY';
      dirBadge.className   = 'qe-dir-badge qe-dir-buy';
    } else {
      dirBadge.textContent = '— NEUTRAL';
      dirBadge.className   = 'qe-dir-badge qe-dir-neutral';
    }
  }

  // ── Risk/Reward — always recalculate from raw prices ──
  let rr = sig.risk_reward;
  if ((rr === null || rr === undefined) && entryPrice && slPrice && tpPrice) {
    const risk   = Math.abs(entryPrice - slPrice);
    const reward = Math.abs(tpPrice    - entryPrice);
    rr = risk > 0 ? reward / risk : null;
  }
  if (rr !== null && rr !== undefined) {
    dom.setupRR.textContent = rr.toFixed(2) + 'R';
    dom.setupRR.className   = 'setup-val ' + (rr >= 2 ? 'good' : rr <= 0.8 ? 'bad' : '');
  } else {
    dom.setupRR.textContent = '—';
    dom.setupRR.className   = 'setup-val';
  }

  if (dom.setupTf) dom.setupTf.textContent = state.tf;

  // ── Store values for copy buttons ──
  const qeBtns = document.querySelectorAll('.qe-copy-btn');
  qeBtns.forEach(btn => {
    const f = btn.dataset.field;
    if (f === 'entry')  btn.dataset.value = fmtPrice(entryPrice, pair);
    if (f === 'stop')   btn.dataset.value = slPrice  ? fmtPrice(slPrice,  pair) : '—';
    if (f === 'target') btn.dataset.value = tpPrice  ? fmtPrice(tpPrice,  pair) : '—';
  });
}

function _resetQeRows() {
  const dirBadge = document.getElementById('qeDirectionBadge');
  if (dirBadge) { dirBadge.textContent = '—'; dirBadge.className = 'qe-dir-badge qe-dir-neutral'; }
}

// ─── SIGNAL AGE TIMER ────────────────────────────────────────
let _ageTimer = null;
function startSignalAgeTimer() {
  if (_ageTimer) clearInterval(_ageTimer);
  _ageTimer = setInterval(updateSignalAge, 10000);
}
function updateSignalAge() {
  if (!dom.signalAgeBadge || !dom.signalAgeText) return;
  if (!state.signalTimestamp) {
    dom.signalAgeText.textContent = 'just now';
    dom.signalAgeBadge.className = 'signal-age-badge age-fresh';
    return;
  }
  const elapsed = Math.floor((Date.now() - state.signalTimestamp) / 1000);
  let text, ageClass;
  if (elapsed < 60) {
    text = elapsed <= 5 ? 'just now' : `${elapsed}s ago`;
    ageClass = 'age-fresh';
  } else if (elapsed < 300) {
    text = `${Math.floor(elapsed / 60)}m ago`;
    ageClass = '';
  } else {
    text = `${Math.floor(elapsed / 60)}m ago — may be stale`;
    ageClass = 'age-stale';
  }
  dom.signalAgeText.textContent = text;
  dom.signalAgeBadge.className = `signal-age-badge ${ageClass}`;
}

// ─── TOOLTIP SYSTEM ──────────────────────────────────────────
function initTooltips() {
  const portal = dom.tooltipPortal;
  if (!portal) return;
  let _tip = null;

  document.addEventListener('mouseover', (e) => {
    const el = e.target.closest('[data-tip]');
    if (!el) return;
    const text = el.getAttribute('data-tip');
    if (!text) return;
    portal.textContent = text;
    portal.classList.add('visible');
    positionTip(e);
    _tip = el;
  });
  document.addEventListener('mousemove', (e) => {
    if (_tip) positionTip(e);
  });
  document.addEventListener('mouseout', (e) => {
    if (_tip && !_tip.contains(e.relatedTarget)) {
      portal.classList.remove('visible');
      _tip = null;
    }
  });

  function positionTip(e) {
    const x = e.clientX + 12;
    const y = e.clientY + 12;
    const w = portal.offsetWidth || 200;
    portal.style.left = (x + w > window.innerWidth ? e.clientX - w - 8 : x) + 'px';
    portal.style.top  = (y + 60 > window.innerHeight ? e.clientY - 60 : y) + 'px';
  }
}

// ─── COPY BUTTONS ────────────────────────────────────────────
function initCopyButtons() {
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('.qe-copy-btn');
    if (btn) {
      const val = btn.dataset.value || '—';
      if (val && val !== '—') {
        navigator.clipboard?.writeText(val).catch(() => {});
        btn.textContent = '✓';
        btn.classList.add('copied');
        setTimeout(() => { btn.textContent = '⎘'; btn.classList.remove('copied'); }, 1500);
      }
      return;
    }
    if (e.target.id === 'copyAllBtn' || e.target.closest('#copyAllBtn')) {
      const entry  = dom.setupEntry?.textContent  || '—';
      const sl     = dom.setupStop?.textContent   || '—';
      const tp     = dom.setupTarget?.textContent || '—';
      const rr     = dom.setupRR?.textContent     || '—';
      const pair   = state.pair;
      const tf     = state.tf;
      const text   = `${pair} ${tf}\nEntry: ${entry}\nSL: ${sl}\nTP: ${tp}\nRR: ${rr}`;
      navigator.clipboard?.writeText(text).catch(() => {});
      const btn = dom.copyAllBtn;
      if (btn) {
        btn.textContent = '✓ Copied!';
        btn.classList.add('copied');
        setTimeout(() => { btn.textContent = 'Copy All Levels'; btn.classList.remove('copied'); }, 1800);
      }
    }
  });
}

function updateAIPanel(sig) {
  if (!sig || sig.error) return;
  const conf     = sig.ai_confidence ?? 0.5;
  const pct      = Math.round(conf * 100);
  const samples  = sig.ai_samples || 0;
  const blended  = sig.blended_score || sig.score || 0;
  const aiWeight = sig.ai_weight_pct || 0;

  dom.aiProgressBar.style.width = pct + '%';
  dom.aiProgressBar.className   = 'ai-progress-bar ' + (pct >= 70 ? 'high' : pct >= 40 ? 'medium' : 'low');
  dom.aiValue.textContent       = pct + '%';
  dom.aiTf.textContent          = state.tf;

  if (samples < 10) {
    dom.aiLabel.textContent = `Model warming up — ${samples} samples, need 10+`;
  } else if (samples < 30) {
    dom.aiLabel.textContent = `Learning phase — ${samples} samples. AI activates at 30`;
  } else {
    const quality = pct >= 70 ? 'High confidence' : pct >= 50 ? 'Moderate confidence' : 'Low confidence — opposing signal';
    dom.aiLabel.textContent = `${quality} · AI weight: ${aiWeight}%`;
  }

  dom.aiStatSamples.textContent   = samples;
  dom.aiStatWeight.textContent    = aiWeight + '%';
  dom.aiStatBlended.textContent   = blended;
  dom.aiStatGlobalAcc.textContent = state.aiInfo?.global_accuracy != null
    ? state.aiInfo.global_accuracy + '%'
    : samples >= 10 ? '—%' : 'N/A';

  // Per-pair accuracy table
  const pairStats = state.aiInfo?.pair_stats || {};
  const rows = Object.entries(pairStats)
    .filter(([, v]) => v.total >= 3)
    .sort((a, b) => b[1].total - a[1].total)
    .slice(0, 6);

  dom.aiPairAcc.innerHTML = rows.length
    ? rows.map(([key, info]) => {
        const acc = info.accuracy || 0;
        return `<div class="ai-pa-row">
          <span class="ai-pa-key">${key.replace('_',' ')}</span>
          <span>${info.wins}W/${info.losses}L</span>
          <span class="ai-pa-acc ${acc >= 55 ? 'good' : 'bad'}">${acc}%</span>
        </div>`;
      }).join('')
    : `<div class="ai-pa-row"><span class="ai-pa-key" style="color:var(--text-3)">Per-pair accuracy after 3+ resolved signals</span></div>`;
}

function updateSuccessRate(data) {
  dom.srPct.textContent  = data.total > 0 ? data.rate + '%' : '—%';
  dom.srSub.textContent  = data.total > 0
    ? `${data.total} resolved signals`
    : 'Tracking — results after ±1% price move';

  const total = data.wins + data.losses;
  dom.srWinFill.style.width  = total > 0 ? (data.wins   / total * 100) + '%' : '0%';
  dom.srLossFill.style.width = total > 0 ? (data.losses / total * 100) + '%' : '0%';
  dom.srWins.textContent = `${data.wins}W`;
  dom.srLoss.textContent = `${data.losses}L`;
  dom.srPend.textContent = `${data.pending || 0}P`;

  const byType = data.by_type || {};
  dom.srByType.innerHTML = Object.entries(byType).map(([type, info]) =>
    `<div class="sr-type-row ${info.rate >= 50 ? 'good' : 'bad'}">
      <span class="sr-type-name">${type.replace('_',' ')}</span>
      <span>${info.wins}W / ${info.total}T</span>
      <span class="sr-type-rate">${info.rate}%</span>
    </div>`).join('');
}

function updateLog(entries) {
  if (!entries || entries.length === 0) {
    dom.logList.innerHTML = '<div class="log-empty">No signals yet…</div>';
    return;
  }
  dom.logList.innerHTML = entries.slice(0, 30).map(e => {
    const color = sigColor(e.signal);
    return `<div class="log-entry">
      <span class="log-time">${e.time}</span>
      <span class="log-pair">${e.pair}</span>
      <span class="log-tf">${e.tf}</span>
      <span class="log-sig ${color}">${(e.label || e.signal).replace('STRONG_','S.')}</span>
      <span class="log-score">${e.score}/100</span>
    </div>`;
  }).join('');
}

// ─── PRICE CHART ─────────────────────────────────────────────
function initPriceChart() {
  const container = document.getElementById('priceChart');
  if (!container || !window.LightweightCharts) return;

  priceChartInstance = LightweightCharts.createChart(container, {
    width:  container.clientWidth,
    height: 340,
    layout: { background: { color: '#0d1117' }, textColor: '#8b949e' },
    grid: { vertLines: { color: '#21262d', style: 1 }, horzLines: { color: '#21262d', style: 1 } },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: '#21262d' },
    timeScale: { borderColor: '#21262d', timeVisible: true, secondsVisible: false },
  });

  candleSeries = priceChartInstance.addCandlestickSeries({
    upColor: '#3fb950', downColor: '#f85149',
    borderUpColor: '#3fb950', borderDownColor: '#f85149',
    wickUpColor: '#3fb950', wickDownColor: '#f85149',
  });

  ema9Series  = priceChartInstance.addLineSeries({ color: '#3fb950', lineWidth: 1, lineStyle: 1, priceLineVisible: false, lastValueVisible: false });
  ema21Series = priceChartInstance.addLineSeries({ color: '#f85149', lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false });
  ema50Series = priceChartInstance.addLineSeries({ color: '#db6d28', lineWidth: 1, lineStyle: 3, priceLineVisible: false, lastValueVisible: false });

  bbUpperSeries = priceChartInstance.addLineSeries({ color: 'rgba(188,140,255,0.4)', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
  bbMidSeries   = priceChartInstance.addLineSeries({ color: 'rgba(188,140,255,0.25)', lineWidth: 1, lineStyle: 1, priceLineVisible: false, lastValueVisible: false });
  bbLowerSeries = priceChartInstance.addLineSeries({ color: 'rgba(188,140,255,0.4)', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });

  vwapSeries      = priceChartInstance.addLineSeries({ color: '#f0c33c', lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false });
  ichTenkanSeries = priceChartInstance.addLineSeries({ color: '#58a6ff', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
  ichKijunSeries  = priceChartInstance.addLineSeries({ color: '#db6d28', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
  ichSenkouASeries= priceChartInstance.addLineSeries({ color: 'rgba(63,185,80,0.3)', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
  ichSenkouBSeries= priceChartInstance.addLineSeries({ color: 'rgba(248,81,73,0.3)', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });

  const ro = new ResizeObserver(entries => {
    requestAnimationFrame(() => {
      for (const e of entries) priceChartInstance?.resize(e.contentRect.width, 340);
    });
  });
  ro.observe(container);
}

function renderPriceChart(data) {
  if (!priceChartInstance || !data) return;
  const showEma  = toggleEma?.checked  ?? true;
  const showBb   = toggleBb?.checked   ?? true;
  const showVwap = toggleVwap?.checked ?? true;
  const showIch  = toggleIch?.checked  ?? true;

  const ohlc = data.ohlc?.map(d => ({
    time: d.t / 1000, open: d.o, high: d.h, low: d.l, close: d.c,
  })).filter(d => d.open && d.close) || [];
  if (ohlc.length) candleSeries.setData(ohlc);

  const ts = data.timestamps?.map(t => t / 1000) || [];

  if (showEma) {
    setLineData(ema9Series, ts, data.ema9);
    setLineData(ema21Series, ts, data.ema21);
    setLineData(ema50Series, ts, data.ema50);
  } else {
    ema9Series.setData([]); ema21Series.setData([]); ema50Series.setData([]);
  }

  if (showBb) {
    setLineData(bbUpperSeries, ts, data.bb_upper);
    setLineData(bbMidSeries,   ts, data.bb_mid);
    setLineData(bbLowerSeries, ts, data.bb_lower);
  } else {
    bbUpperSeries.setData([]); bbMidSeries.setData([]); bbLowerSeries.setData([]);
  }

  if (showVwap) {
    setLineData(vwapSeries, ts, data.vwap);
  } else {
    vwapSeries.setData([]);
  }

  if (showIch) {
    setLineData(ichTenkanSeries,  ts, data.ich_tenkan);
    setLineData(ichKijunSeries,   ts, data.ich_kijun);
    setLineData(ichSenkouASeries, ts, data.ich_senkou_a);
    setLineData(ichSenkouBSeries, ts, data.ich_senkou_b);
  } else {
    ichTenkanSeries.setData([]); ichKijunSeries.setData([]);
    ichSenkouASeries.setData([]); ichSenkouBSeries.setData([]);
  }
}

function setLineData(series, times, values) {
  if (!series || !values) return;
  series.setData(
    times.map((t, i) => ({ time: t, value: values[i] }))
         .filter(d => d.value !== null && d.value !== undefined && !isNaN(d.value))
  );
}

// ─── SUB CHART ───────────────────────────────────────────────
function initSubChart() {
  const container = document.getElementById('subChart');
  if (!container || !window.LightweightCharts) return;

  subChartInstance = LightweightCharts.createChart(container, {
    width: container.clientWidth, height: 160,
    layout: { background: { color: '#0d1117' }, textColor: '#8b949e' },
    grid: { vertLines: { color: '#21262d', style: 1 }, horzLines: { color: '#21262d', style: 1 } },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: '#21262d' },
    timeScale: { borderColor: '#21262d', timeVisible: true, secondsVisible: false },
  });

  subSeries.rsi.main = subChartInstance.addLineSeries({ color: '#bc8cff', lineWidth: 1, priceLineVisible: false });
  subSeries.rsi.ob   = subChartInstance.addLineSeries({ color: 'rgba(248,81,73,0.4)',  lineWidth: 1, lineStyle: 2, priceLineVisible: false });
  subSeries.rsi.os   = subChartInstance.addLineSeries({ color: 'rgba(63,185,80,0.4)',  lineWidth: 1, lineStyle: 2, priceLineVisible: false });

  subSeries.macd.macd   = subChartInstance.addLineSeries({ color: '#58a6ff', lineWidth: 1, priceLineVisible: false });
  subSeries.macd.signal = subChartInstance.addLineSeries({ color: '#db6d28', lineWidth: 1, lineStyle: 2, priceLineVisible: false });
  subSeries.macd.hist   = subChartInstance.addHistogramSeries({ priceLineVisible: false });

  subSeries.stoch.k  = subChartInstance.addLineSeries({ color: '#58a6ff', lineWidth: 1, priceLineVisible: false });
  subSeries.stoch.d  = subChartInstance.addLineSeries({ color: '#db6d28', lineWidth: 1, lineStyle: 2, priceLineVisible: false });
  subSeries.stoch.ob = subChartInstance.addLineSeries({ color: 'rgba(248,81,73,0.4)', lineWidth: 1, lineStyle: 2, priceLineVisible: false });
  subSeries.stoch.os = subChartInstance.addLineSeries({ color: 'rgba(63,185,80,0.4)', lineWidth: 1, lineStyle: 2, priceLineVisible: false });

  subSeries.willr.main = subChartInstance.addLineSeries({ color: '#39d0d8', lineWidth: 1, priceLineVisible: false });
  subSeries.willr.ob   = subChartInstance.addLineSeries({ color: 'rgba(248,81,73,0.4)', lineWidth: 1, lineStyle: 2, priceLineVisible: false });
  subSeries.willr.os   = subChartInstance.addLineSeries({ color: 'rgba(63,185,80,0.4)', lineWidth: 1, lineStyle: 2, priceLineVisible: false });

  subSeries.volume.hist = subChartInstance.addHistogramSeries({ priceLineVisible: false });

  const ro = new ResizeObserver(entries => {
    requestAnimationFrame(() => {
      for (const e of entries) subChartInstance?.resize(e.contentRect.width, 160);
    });
  });
  ro.observe(container);
}

function updateSubChart(type, data) {
  if (!subChartInstance || !data) return;
  const ts = data.timestamps?.map(t => t / 1000) || [];
  clearAllSubSeries();

  if (type === 'rsi') {
    setLineData(subSeries.rsi.main, ts, data.rsi);
    subSeries.rsi.ob.setData(ts.map(t => ({ time: t, value: 70 })));
    subSeries.rsi.os.setData(ts.map(t => ({ time: t, value: 30 })));
  } else if (type === 'macd') {
    setLineData(subSeries.macd.macd,   ts, data.macd);
    setLineData(subSeries.macd.signal, ts, data.signal);
    const hist = data.hist || [];
    subSeries.macd.hist.setData(
      ts.map((t, i) => ({
        time: t, value: hist[i] || 0,
        color: (hist[i] || 0) >= 0 ? 'rgba(63,185,80,0.6)' : 'rgba(248,81,73,0.6)',
      })).filter(d => d.value !== null && !isNaN(d.value))
    );
  } else if (type === 'stoch') {
    setLineData(subSeries.stoch.k, ts, data.stoch_k);
    setLineData(subSeries.stoch.d, ts, data.stoch_d);
    subSeries.stoch.ob.setData(ts.map(t => ({ time: t, value: 80 })));
    subSeries.stoch.os.setData(ts.map(t => ({ time: t, value: 20 })));
  } else if (type === 'willr') {
    setLineData(subSeries.willr.main, ts, data.williams_r);
    subSeries.willr.ob.setData(ts.map(t => ({ time: t, value: -20 })));
    subSeries.willr.os.setData(ts.map(t => ({ time: t, value: -80 })));
  } else if (type === 'volume') {
    const ohlc = data.ohlc || [];
    subSeries.volume.hist.setData(
      ohlc.map((d, i) => ({
        time:  d.t / 1000,
        value: data.volume[i] || 0,
        color: d.c >= d.o ? 'rgba(63,185,80,0.5)' : 'rgba(248,81,73,0.5)',
      })).filter(d => d.value)
    );
  }
}

function clearAllSubSeries() {
  Object.values(subSeries).forEach(group => {
    Object.values(group).forEach(s => {
      if (s && typeof s.setData === 'function') s.setData([]);
    });
  });
}

// ─── HELPERS ─────────────────────────────────────────────────
function fmtPrice(p, pair) {
  if (!p) return '—';
  if (pair === 'DOGE') return '$' + p.toFixed(5);
  if (pair === 'ZEC')  return '$' + p.toFixed(2);
  if (p >= 1000) return '$' + p.toLocaleString('en-US', { maximumFractionDigits: 2 });
  return '$' + p.toFixed(4);
}

function fmtVol(v) {
  if (!v) return '—';
  if (v >= 1e9) return (v/1e9).toFixed(2) + 'B';
  if (v >= 1e6) return (v/1e6).toFixed(2) + 'M';
  if (v >= 1e3) return (v/1e3).toFixed(0) + 'K';
  return v.toFixed(0);
}

function sigColor(sig) {
  if (['STRONG_BUY','BUY'].includes(sig))  return 'success';
  if (['STRONG_SELL','SELL'].includes(sig)) return 'danger';
  return 'neutral';
}

function setRefreshSpinning(on) {
  if (dom.refreshBtn) dom.refreshBtn.classList.toggle('spinning', on);
}

function updateLastUpdate() {
  if (dom.lastUpdate) dom.lastUpdate.textContent = 'Updated ' + new Date().toLocaleTimeString();
}

function updateSrTfBadge() {
  if (dom.srTf) dom.srTf.textContent = state.tf;
}