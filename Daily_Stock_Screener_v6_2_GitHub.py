# -*- coding: utf-8 -*-
"""
Daily_Stock_Screener_v6_2.py
======================================
Version 6.2 - Runtime Fixes

Built by Vishvesh Trivedi
OSS Architect | AI/ML Automation | 12 Patents
LinkedIn: https://www.linkedin.com/in/vishvesh-trivedi

─────────────────────────────────────────────────────────────
⚠️  IMPORTANT: HOW TO SET YOUR API KEY (READ THIS FIRST)
─────────────────────────────────────────────────────────────

Option A - Google Colab (Recommended):
  1. Click the 🔑 Secrets icon in the left sidebar
  2. Add a new secret:
       Name:  ANTHROPIC_API_KEY
       Value: your-key-here (get it from console.anthropic.com)
  3. Enable the secret for this notebook
  4. The code below will read it automatically

Option B - Local Python:
  1. Create a file called .env in the same folder as this script
  2. Add this line:  ANTHROPIC_API_KEY=your-key-here
  3. Install python-dotenv:  pip install python-dotenv
  4. The code below will read it automatically

Option C - Paste directly (Colab only, NOT for GitHub):
  Find the line:  ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
  Replace with:   ANTHROPIC_API_KEY = "your-key-here"
  ⚠️  Never upload this version to GitHub!

─────────────────────────────────────────────────────────────
WHAT THIS SCREENER DOES
─────────────────────────────────────────────────────────────

Every evening after market close:
  → Downloads OHLCV for 375+ stocks in a single API call
  → Runs bidirectional screening — technical filters AND news rescue in parallel
  → Computes deterministic pre-scores:
       RSI, MACD, ADX, CMF, StochRSI, VWAP, OBV,
       Options P/C ratio, Insider flow, VADER NLP sentiment
  → Feeds top 30 candidates to Claude with full context:
       Macro headlines, sector rotation, earnings risk,
       self-calibration from past picks
  → Gets back BUY / WATCH / NO PICK with:
       Stop zones, price targets, R:R ratio,
       devil's advocate, full score breakdown
  → Saves everything to Google Drive
  → Auto-updates 10-day and 30-day returns over time

Tech Stack (all free except Anthropic API):
  • yfinance       — market data
  • VADER          — free NLP sentiment (no API key needed)
  • 16 RSS feeds   — macro news (Reuters, CNBC, MarketWatch, BBC...)
  • Anthropic API  — AI reasoning layer (~$0.05-0.10 per run)
  • Google Colab + Drive — zero-infrastructure deployment

Cost per run:  ~$0.05 - $0.10
Runtime:       ~7 - 9 minutes

─────────────────────────────────────────────────────────────
DAILY ROUTINE
─────────────────────────────────────────────────────────────
  Cell 1  Mount Google Drive        → run every session
  Cell 2  Install dependencies      → first time only
  Cell 3  API key + config          → first time only
  Cell 4  Load functions            → run every session
  Cell 5  Run screener              → run every day after market close

─────────────────────────────────────────────────────────────
DISCLAIMER
─────────────────────────────────────────────────────────────
This is a personal learning project built out of curiosity.
It is NOT financial advice. Past screener performance does
not guarantee future results. Always do your own research.

─────────────────────────────────────────────────────────────
Version History:
  FIX 1  enrich_with_scores() is now actually called in run_screener
  FIX 2  load_performance_history() called and passed to analyze_with_claude
  FIX 3  Stream B now runs BEFORE options/insider fetch
  FIX 4  Hard caps clamped post-hoc instead of trusted to the LLM
  FIX 5  FI ticker removed (Yahoo Finance delisted - was FISV)
  FIX 6  Sector bonus reduced from 3 to 2 (sum cleanly to 60 max)
  FIX 7  Model names unified to claude-sonnet-4-5 in all 3 Claude calls
  FIX 8  compute_indicators logs exception when SCREENER_DEBUG env set
  FIX 9  ETFs included in batch_download so sector ranks + ETF news work
  FIX 10 Candidates trimmed to top 30 by pre_score before Claude call
  FIX 11 Claude output tokens raised from 1200 to 2000
"""

# ============================================================
# CELL 1 - MOUNT GOOGLE DRIVE (run every session)
# ============================================================
from google.colab import drive
import os

drive.mount('/content/drive')
DRIVE_FOLDER = '/content/drive/MyDrive/StockScreener'
os.makedirs(DRIVE_FOLDER, exist_ok=True)

print('✅ Google Drive mounted')
print(f'📁 Folder: {DRIVE_FOLDER}')
print(f'📄 Files:  {os.listdir(DRIVE_FOLDER)}')

# ============================================================
# CELL 2 - INSTALL DEPENDENCIES (first time only)
# ============================================================
!pip install yfinance pandas anthropic requests vaderSentiment --quiet
print('✅ Dependencies installed')

# ============================================================
# CELL 3 - CONFIGURATION
# ============================================================

# ── API KEY (reads from Colab Secrets or .env file) ────────
# Follow the instructions at the top of this file to set your key safely.
# Never paste your real key here if you plan to share or upload this file.

import os

# Try Colab Secrets first, then environment variable, then .env file
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# If running locally, try loading from .env file
if not ANTHROPIC_API_KEY:
    try:
        from dotenv import load_dotenv
        load_dotenv()
        ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
    except ImportError:
        pass  # dotenv not installed, that's fine

if not ANTHROPIC_API_KEY:
    print("⚠️  WARNING: No API key found!")
    print("   Please follow the setup instructions at the top of this file.")
    print("   In Colab: use the 🔑 Secrets panel on the left sidebar.")
else:
    print("✅ API key loaded successfully")

# ── SCREENER SETTINGS ──────────────────────────────────────
BUY_THRESHOLD    = 80    # minimum confidence score to generate a BUY signal
WATCH_THRESHOLD  = 70    # minimum confidence score for WATCH list
VOLUME_MIN_RATIO = 1.2   # stock must trade at 1.2x its average volume
RSI_MIN          = 35    # minimum RSI (avoid oversold)
RSI_MAX          = 75    # maximum RSI (avoid overbought)
MIN_PRICE        = 5.0   # minimum stock price in USD
SAMPLE_SIZE      = 600   # increase to 500 for weekend full runs

PICKS_CSV = f'{DRIVE_FOLDER}/stock_picks.csv'
WATCH_CSV = f'{DRIVE_FOLDER}/watch_list.csv'

# Optional: set to '1' to see why tickers fail compute_indicators
# os.environ['SCREENER_DEBUG'] = '1'

print('\n✅ Configuration ready')
print(f'   BUY threshold:   {BUY_THRESHOLD}')
print(f'   WATCH threshold: {WATCH_THRESHOLD}')
print(f'   Sample size:     {SAMPLE_SIZE}')

# ============================================================
# CELL 4 - ALL FUNCTIONS (run once per session)
# ============================================================

import yfinance as yf
import pandas as pd
import numpy as np
import anthropic
import json
import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer as _VaderAnalyzer
    _VADER = _VaderAnalyzer()
except ImportError:
    _VADER = None


# ── TUNING CONSTANTS ───────────────────────────────────────
MIN_DOLLAR_VOLUME_M  = 20      # minimum $20M/day - ensures liquidity
ATR_STOP_MULT        = 1.5     # stop = entry - 1.5xATR
ATR_TARGET_MULT      = 3.0     # target = entry + 3xATR → R:R 1:2
ADX_MIN              = 20      # minimum trend strength
VIX_LOW_PCTILE       = 25      # below this = risk-on regime
VIX_HIGH_PCTILE      = 75      # above this = risk-off regime
SECTOR_CONC_LOOKBACK = 5       # look back N picks for sector concentration
SECTOR_CONC_MAX      = 3       # max same-sector picks in lookback window
SECTOR_CONC_PENALTY  = 10      # confidence penalty if concentrated
NEWS_WORKERS         = 20      # parallel workers for news/fundamentals fetch

CLAUDE_MODEL = 'claude-sonnet-4-5'

# ── TWO-TIER RESCUE KEYWORDS ───────────────────────────────
RESCUE_TIER1 = [
    'acquisition', 'merger', 'buyout', 'takeover',
    'fda', 'clearance',
    'bankruptcy', 'restructur',
    'lawsuit', 'doj', 'ftc', 'fraud', 'indicted',
    'tariff', 'sanction', 'delisting', 'halted',
    'resign', 'fired', 'ousted',
]

RESCUE_TIER2_REACTION = [
    'beats', 'misses', 'exceeds', 'warning',
    'raised', 'lowered', 'rejected',
    'approved', 'upgraded', 'downgraded',
    'investigation', 'subpoena',
]

RESCUE_TIER2_CONTEXT = [
    'earnings', 'revenue', 'guidance',
    'profit', 'outlook', 'quarterly',
]


def has_significant_news(news_titles):
    """
    Two-tier rescue logic.
    Returns (should_rescue, tier1_hits, tier2_reaction_hits, tier2_context_hits)
    """
    text  = ' '.join(news_titles).lower()
    t1    = [k for k in RESCUE_TIER1 if k in text]
    t2r   = [k for k in RESCUE_TIER2_REACTION if k in text]
    t2c   = [k for k in RESCUE_TIER2_CONTEXT if k in text]
    rescue = len(t1) >= 1 or (len(t2r) >= 1 and len(t2c) >= 1)
    return rescue, t1, t2r, t2c


def compute_vader_sentiment(news_titles):
    """
    NLP sentiment on news headlines. Free - no API key needed.
    Returns (compound, label) where compound is -1 to +1.
    BULLISH >= +0.05  |  BEARISH <= -0.05  |  NEUTRAL otherwise
    """
    if not news_titles or _VADER is None:
        return 0.0, 'NEUTRAL'
    scores = [_VADER.polarity_scores(t)['compound'] for t in news_titles]
    avg = sum(scores) / len(scores)
    label = 'BULLISH' if avg >= 0.05 else 'BEARISH' if avg <= -0.05 else 'NEUTRAL'
    return round(avg, 3), label


def compute_tech_score(ind, ctx, sector_rank=6):
    """
    Deterministic technical score 0-60. Runs BEFORE Claude.

    Components:
      RSI regime fit      0-12
      Volume conviction   0-12
      Momentum alignment  0-12
      Trend quality       0-12
      MA + 52w position   0-12
    Sector bonus          0-2
    """
    if not ind:
        return 0, {}
    bd = {}
    is_bull = ctx.get('qqq_trend', 'BULLISH') == 'BULLISH'
    rsi = ind.get('rsi', 50)

    # 1. RSI regime fit (0-12)
    if is_bull:
        if   50 <= rsi <= 65: rsi_pts = 12
        elif 45 <= rsi <  50: rsi_pts = 8
        elif 65 <  rsi <= 72: rsi_pts = 7
        elif 40 <= rsi <  45: rsi_pts = 4
        elif 72 <  rsi <= 78: rsi_pts = 3
        else:                 rsi_pts = 0
    else:
        if   40 <= rsi <= 55: rsi_pts = 12
        elif 35 <= rsi <  40: rsi_pts = 8
        elif 55 <  rsi <= 62: rsi_pts = 7
        elif 30 <= rsi <  35: rsi_pts = 4
        elif 62 <  rsi <= 68: rsi_pts = 2
        else:                 rsi_pts = 0
    bd['rsi_fit'] = rsi_pts

    # 2. Volume conviction (0-12)
    vr  = ind.get('vol_ratio', 0.0)
    va  = ind.get('vol_accel', False)
    obv = ind.get('obv_rising', False)
    cmf = ind.get('cmf', 0.0)
    if   vr >= 3.0: vol_pts = 8
    elif vr >= 2.5: vol_pts = 7
    elif vr >= 2.0: vol_pts = 6
    elif vr >= 1.5: vol_pts = 4
    elif vr >= 1.2: vol_pts = 2
    else:           vol_pts = 0
    if va:        vol_pts = min(12, vol_pts + 2)
    if obv:       vol_pts = min(12, vol_pts + 1)
    if cmf > 0.1: vol_pts = min(12, vol_pts + 1)
    bd['volume'] = vol_pts

    # 3. Momentum alignment (0-12)
    m5   = ind.get('momentum_5d',  0.0)
    m20  = ind.get('momentum_20d', 0.0)
    rs   = ind.get('rs_vs_spy',    0.0)
    hh   = ind.get('hh_hl', False)
    if is_bull:
        m5_pts  = 5 if m5 > 2 else 4 if m5 > 0 else 1 if m5 > -2 else 0
        m20_pts = 4 if m20 > 4 else 3 if m20 > 1 else 1 if m20 > 0 else 0
    else:
        m5_pts  = 5 if 0 < m5 <= 3 else 2 if m5 > 3 else 2 if m5 > -2 else 0
        m20_pts = 4 if 0 < m20 <= 5 else 1 if m20 > 5 else 2 if m20 > -3 else 0
    rs_pts  = 3 if rs > 1.5 else 2 if rs > 0.5 else 1 if rs > 0 else 0
    hh_pts  = 1 if hh else 0
    mom_pts = min(12, m5_pts + m20_pts + rs_pts + hh_pts)
    bd['momentum'] = mom_pts

    # 4. Trend quality - MACD + ADX + BB%B + StochRSI (0-12)
    macd      = ind.get('macd_bullish', False)
    adx       = ind.get('adx', 0.0)
    bb        = ind.get('bb_pct_b', 0.5)
    stoch_rsi = ind.get('stoch_rsi', 0.5)
    tr_pts = 0
    if macd:        tr_pts += 4
    if   adx >= 35: tr_pts += 4
    elif adx >= 28: tr_pts += 3
    elif adx >= 22: tr_pts += 1
    elif adx <  18: tr_pts -= 1
    if bb >= 0.8:         tr_pts += 2
    elif bb >= 0.6:       tr_pts += 1
    elif bb <= 0.2:       tr_pts -= 1
    if stoch_rsi > 0.8:   tr_pts += 1
    elif stoch_rsi < 0.2: tr_pts -= 1
    bd['trend_quality'] = max(0, min(12, tr_pts))

    # 5. MA alignment + 52w + VWAP (0-12)
    vs20  = ind.get('vs_ma20_pct', 0.0)
    vs50  = ind.get('vs_ma50_pct', 0.0)
    vs200 = ind.get('vs_ma200_pct', 0.0)
    h52   = ind.get('pct_from_52h', -20.0)
    vwap  = ind.get('vs_vwap_pct', 0.0)
    ma_pts = 0
    if vs20 > 0 and vs50 > 0 and vs200 > 0: ma_pts += 5
    elif vs20 > 0 and vs50 > 0:              ma_pts += 3
    elif vs20 > 0:                            ma_pts += 1
    if   h52 >= -3:  ma_pts += 4
    elif h52 >= -8:  ma_pts += 3
    elif h52 >= -15: ma_pts += 2
    elif h52 >= -25: ma_pts += 1
    elif h52 < -40:  ma_pts -= 1
    if vwap > 0.5:   ma_pts = min(12, ma_pts + 1)
    bd['ma_52w'] = max(0, min(12, ma_pts))

    # Sector bonus (0-2)
    sec_bonus = 2 if sector_rank <= 3 else 1 if sector_rank <= 6 else 0

    total = rsi_pts + vol_pts + mom_pts + bd['trend_quality'] + bd['ma_52w'] + sec_bonus
    total = max(0, min(60, total))
    bd['sector_bonus'] = sec_bonus
    bd['total'] = total
    return total, bd


def compute_news_score(news_titles, rescue_keywords, analyst_rating,
                       upside_pct, market_sentiment, pc_ratio=None,
                       insider_label='NEUTRAL'):
    """
    News-driven score 0-40. Runs BEFORE Claude.
    """
    vader_compound, vader_label = compute_vader_sentiment(news_titles)

    # 1. VADER sentiment (0-15)
    if   vader_compound >= 0.35: v_pts = 15
    elif vader_compound >= 0.20: v_pts = 13
    elif vader_compound >= 0.10: v_pts = 11
    elif vader_compound >= 0.05: v_pts =  9
    elif vader_compound >= -0.05:v_pts =  7
    elif vader_compound >= -0.15:v_pts =  4
    elif vader_compound >= -0.25:v_pts =  2
    else:                        v_pts =  0

    # 2. News significance (0-10)
    n_hits   = len(rescue_keywords) if rescue_keywords else 0
    n_titles = len(news_titles)     if news_titles     else 0
    if   n_hits >= 4: kw_pts = 10
    elif n_hits >= 3: kw_pts = 8
    elif n_hits >= 2: kw_pts = 6
    elif n_hits >= 1: kw_pts = 4
    elif n_titles >= 5: kw_pts = 3
    elif n_titles >= 2: kw_pts = 2
    else:               kw_pts = 0

    # 3. Macro alignment (0-10)
    ms = (market_sentiment or 'NEUTRAL').upper()
    if   ms == 'BULLISH':  mac_pts = 10
    elif ms == 'NEUTRAL':  mac_pts =  6
    elif ms == 'CAUTIOUS': mac_pts =  3
    else:                  mac_pts =  1

    # 4. Analyst consensus (0-3)
    a_pts = 0
    if analyst_rating is not None:
        if   analyst_rating <= 1.5: a_pts = 3
        elif analyst_rating <= 2.0: a_pts = 2
        elif analyst_rating <= 2.5: a_pts = 1
    if upside_pct is not None:
        if   upside_pct >= 30: a_pts = min(3, a_pts + 1)
        elif upside_pct <= -10: a_pts = max(0, a_pts - 1)

    # 5. Options signal (0-2)
    opt_pts = 0
    if pc_ratio is not None:
        if   pc_ratio < 0.6: opt_pts = 2
        elif pc_ratio < 0.9: opt_pts = 1
        elif pc_ratio > 1.5: opt_pts = -1

    # 6. Insider signal (0-2)
    ins_pts = 2 if insider_label == 'BUYING' else -1 if insider_label == 'SELLING' else 0

    total = v_pts + kw_pts + mac_pts + a_pts + opt_pts + ins_pts
    bd = {
        'vader_sentiment':    v_pts,
        'news_significance':  kw_pts,
        'macro_alignment':    mac_pts,
        'analyst_consensus':  a_pts,
        'options_signal':     opt_pts,
        'insider_signal':     ins_pts,
        'total':              total,
        'vader_label':        vader_label,
    }
    return min(40, max(0, total)), bd, vader_label


def _fetch_options_single(ticker):
    """Fetch put/call ratio from nearest expiry options chain."""
    try:
        tk   = yf.Ticker(ticker)
        exps = tk.options
        if not exps:
            return ticker, None, 'NEUTRAL'
        opt        = tk.option_chain(exps[0])
        calls_vol  = float(opt.calls['volume'].fillna(0).sum())
        puts_vol   = float(opt.puts['volume'].fillna(0).sum())
        if calls_vol + puts_vol < 200:
            return ticker, None, 'NEUTRAL'
        pc = round(puts_vol / calls_vol, 2) if calls_vol > 0 else None
        if   pc is None:  label = 'NEUTRAL'
        elif pc < 0.7:    label = 'BULLISH'
        elif pc > 1.3:    label = 'BEARISH'
        else:             label = 'NEUTRAL'
        return ticker, pc, label
    except:
        return ticker, None, 'NEUTRAL'


def _fetch_insider_single(ticker):
    """Fetch insider buy/sell from yfinance over last 90 days."""
    try:
        tk      = yf.Ticker(ticker)
        ins     = tk.insider_transactions
        if ins is None or ins.empty:
            return ticker, 0, 'NEUTRAL'
        ins      = ins.copy()
        date_col = 'Start Date' if 'Start Date' in ins.columns else 'Date'
        ins['_dt'] = pd.to_datetime(ins[date_col], errors='coerce')
        cutoff   = pd.Timestamp.now() - pd.Timedelta(days=90)
        recent   = ins[ins['_dt'] >= cutoff]
        if recent.empty:
            return ticker, 0, 'NEUTRAL'
        if 'Transaction' in recent.columns:
            tx_col = 'Transaction'
        elif 'Type' in recent.columns:
            tx_col = 'Type'
        else:
            return ticker, 0, 'NEUTRAL'
        if 'Shares' in recent.columns:
            share_col = 'Shares'
        elif 'Value' in recent.columns:
            share_col = 'Value'
        else:
            share_col = None
        def safe_shares(row):
            if share_col is None: return 0
            v = row[share_col]
            try: return abs(float(str(v).replace(',', '')))
            except: return 0
        buys  = recent[recent[tx_col].astype(str).str.contains('Buy|Purchase', case=False, na=False)]
        sells = recent[recent[tx_col].astype(str).str.contains('Sale|Sell',    case=False, na=False)]
        buy_sh  = sum(safe_shares(r) for _, r in buys.iterrows())
        sell_sh = sum(safe_shares(r) for _, r in sells.iterrows())
        net     = int(buy_sh - sell_sh)
        if   net >  50_000:  label = 'BUYING'
        elif net < -100_000: label = 'SELLING'
        else:                label = 'NEUTRAL'
        return ticker, net, label
    except:
        return ticker, 0, 'NEUTRAL'


def fetch_options_and_insider_parallel(candidates):
    """Fetch options P/C + insider sentiment for ALL candidates in parallel."""
    tickers = [c['ticker'] for c in candidates]
    print(f'  Fetching options + insider data for {len(tickers)} candidates...')
    options_data = {}
    insider_data = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        opt_futures = {ex.submit(_fetch_options_single, t): t for t in tickers}
        ins_futures = {ex.submit(_fetch_insider_single, t): t for t in tickers}
        for f in as_completed(opt_futures):
            t, pc, lbl = f.result()
            options_data[t] = {'pc_ratio': pc, 'label': lbl}
        for f in as_completed(ins_futures):
            t, net, lbl = f.result()
            insider_data[t] = {'net_shares': net, 'label': lbl}
    opt_signals = sum(1 for v in options_data.values() if v['pc_ratio'] is not None)
    ins_signals = sum(1 for v in insider_data.values()  if v['label'] != 'NEUTRAL')
    print(f'  Options data: {opt_signals}/{len(tickers)} | Insider signals: {ins_signals}/{len(tickers)}')
    return options_data, insider_data


def compute_sector_ranks(batch_data):
    """Rank all 11 sectors by 5-day ETF performance."""
    perf = {}
    for sector, etf in SECTOR_ETF_MAP.items():
        df = batch_data.get(etf)
        if df is None or len(df) < 5:
            perf[sector] = 0.0
            continue
        try:
            p_now = float(df['Close'].iloc[-1])
            p_5d  = float(df['Close'].iloc[-5])
            perf[sector] = round(((p_now - p_5d) / p_5d) * 100, 2) if p_5d > 0 else 0.0
        except:
            perf[sector] = 0.0
    sorted_s = sorted(perf.items(), key=lambda x: x[1], reverse=True)
    ranks    = {s: i + 1 for i, (s, _) in enumerate(sorted_s)}
    top3     = [s for s, _ in sorted_s[:3]]
    print(f'  Sector ranks - Top 3: {top3} | Perf: '
          + ' | '.join(f'{s}:{v:+.1f}%' for s, v in sorted_s[:5]))
    return ranks, perf


def enrich_with_scores(candidates, ctx, market_sentiment,
                       sector_ranks, options_data, insider_data):
    """Attach tech_score and news_score to every candidate."""
    print(f'\nPhase 5.5 - Computing score breakdown ({len(candidates)} candidates)...')
    for c in candidates:
        t = c['ticker']
        sec_rank = sector_ranks.get(c.get('sector', 'Unknown'), 6)
        opt  = options_data.get(t, {})
        ins  = insider_data.get(t, {})

        ts, ts_bd = compute_tech_score(c, ctx, sector_rank=sec_rank)
        c['tech_score']           = ts
        c['tech_score_breakdown'] = ts_bd

        ns, ns_bd, vader_lbl = compute_news_score(
            news_titles      = c.get('stock_news', []),
            rescue_keywords  = c.get('rescue_keywords', []),
            analyst_rating   = c.get('analyst_rating'),
            upside_pct       = c.get('upside_pct'),
            market_sentiment = market_sentiment,
            pc_ratio         = opt.get('pc_ratio'),
            insider_label    = ins.get('label', 'NEUTRAL'),
        )
        c['news_score']           = ns
        c['news_score_breakdown'] = ns_bd
        c['vader_label']          = vader_lbl
        c['options_pc']           = opt.get('pc_ratio')
        c['options_label']        = opt.get('label', 'NEUTRAL')
        c['insider_label']        = ins.get('label', 'NEUTRAL')
        c['pre_score']            = ts + ns

    top5 = sorted(candidates, key=lambda x: x['pre_score'], reverse=True)[:5]
    print('  Pre-score top 5: '
          + '  '.join(f'{c["ticker"]}({c["pre_score"]}='
                      f'{c["tech_score"]}T+{c["news_score"]}N [vader:{c["vader_label"]}])'
                      for c in top5))
    return candidates


def load_performance_history(fp):
    """Load last 20 evaluated picks for Claude self-calibration."""
    if not os.path.exists(fp):
        return []
    try:
        df   = pd.read_csv(fp)
        done = df[df['Result'].isin(['Win', 'Loss', 'Neutral'])].tail(20)
        if done.empty:
            return []
        hist = []
        for _, row in done.iterrows():
            hist.append({
                'ticker':     str(row.get('Ticker', '')),
                'confidence': str(row.get('Confidence', '')),
                'source':     str(row.get('Source', '')),
                'result':     str(row.get('Result', '')),
                'return_30d': str(row.get('Return_30d_pct', '')),
            })
        return hist
    except:
        return []


# ── TICKER UNIVERSE ────────────────────────────────────────
NASDAQ_100 = [
    'AAPL','MSFT','NVDA','AMZN','META','GOOGL','GOOG','TSLA','AVGO','COST',
    'NFLX','ASML','TMUS','AMD','ADBE','CSCO','PEP','INTU','CMCSA','HON',
    'AMGN','SBUX','QCOM','AMAT','ISRG','ARM','BKNG','TXN','PANW','VRTX',
    'ADI','REGN','LRCX','MDLZ','MU','KLAC','CDNS','SNPS','CEG','CTAS',
    'FTNT','MELI','ABNB','KDP','PYPL','MAR','ORLY','MNST','PCAR','MRNA',
    'CRWD','NXPI','TEAM','DXCM','BIIB','IDXX','PAYX','ROST','ODFL','FAST',
    'EXC','FANG','CTSH','AEP','GEHC','VRSK','MCHP','XEL','ON','TTWO',
    'ZS','DLTR','EA','CCEP','CDW','GFS','ILMN','BKR','DDOG',
    'EBAY','ENPH','ALGN','AZN','INTC','SMCI','LULU','WDAY','DASH','ROP',
    'CPRT','CSGP','WBD','SIRI','GEN','APP','AXON','TTD','MSTR','COIN',
]

SP500_STOCKS = [
    # Financials
    'JPM','BAC','WFC','GS','MS','C','BLK','SCHW','AXP','CB','AON',
    'ICE','CME','SPGI','MCO','TRV','PGR','AFL','MET','PRU','AIG','ALL',
    'USB','PNC','TFC','COF','SYF','AMP','BK','STT','NTRS','RF',
    'CFG','HBAN','KEY','MTB','FITB','ZION','FOUR','HOOD',
    # Healthcare
    'UNH','JNJ','LLY','MRK','ABT','TMO','DHR','BSX','EW','SYK','BDX',
    'ZTS','GILD','HCA','CI','ELV','HUM','MOH','CNC','CVS','MCK',
    'CAH','COR','HOLX','RMD','BAX','HSIC','VTRS','OGN','SOLV',
    # Energy
    'XOM','CVX','COP','EOG','SLB','MPC','PSX','VLO','DVN','APA',
    'OXY','HAL','OVV','CTRA','RRC','EQT','KMI','WMB','OKE','ET','TRGP','LNG',
    # Nuclear and Clean Energy
    'OKLO','SMR','IMSR','NNE','CCJ','LEU',
    # Industrials
    'GE','GEV','MMM','CAT','DE','RTX','LMT','NOC','GD','LHX','TDG',
    'BA','UPS','FDX','NSC','CSX','UNP','EMR','ETN','ITW','PH','ROK',
    'AME','CARR','OTIS','GWW','SWK','IR','XYL','GNRC',
    # Consumer Discretionary
    'WMT','HD','MCD','NKE','LOW','TGT','TJX','ROST',
    'GM','F','APTV','BWA','LEA','MGA','GNTX',
    'CMG','YUM','QSR','DPZ','TXRH','EAT','DRI',
    # Consumer Staples
    'PG','KO','MO','PM','KHC','GIS','CPB','HRL',
    'SJM','MKC','CLX','CHD','CL','EL','PPC',
    # Technology
    'IBM','ORCL','CRM','NOW','SNOW','PLTR','DELL','HPQ','HPE','NTAP',
    'PSTG','NTNX','MANH','VEEV','HUBS','NET','MDB','OKTA',
    'SHOP','SOFI','AFRM','ADSK','ROKU','KVUE',
    'MRVL','QRVO','SWKS','CRUS','TER','ENTG','ACLS','VLTO',
    # Crypto and Digital Assets
    'MARA','RIOT','IREN','CIFR','WULF','SBET','ZETA','CRCL',
    # AI and Emerging Tech
    'NBIS','CRWV',
    # Real Estate
    'AMT','PLD','CCI','EQIX','SPG','O','WELL','VTR','EQR','AVB',
    'ESS','MAA','UDR','CPT',
    # Utilities
    'NEE','DUK','SO','D','SRE','PCG','EIX','ES',
    'WEC','DTE','CMS','AES','NRG','VST','ETR','PPL',
    # Materials
    'LIN','APD','ECL','SHW','NEM','FCX','NUE','STLD','CLF','AA',
    'CF','MOS','FMC','ALB','SQM','MP','SCCO',
    # Communications and Media
    'VZ','T','LUMN','NWSA','NWS','LYV',
    'MGM','WYNN','LVS','CZR','DKNG',
    'CCL','RCL','NCLH','AAL','DAL','UAL','LUV','ALK',
    'DIS','CHTR','OMC',
]

KEY_ETFS = [
    'SPY','QQQ','IWM','DIA','VTI','VOO',
    'XLK','XLF','XLE','XLV','XLI','XLY','XLP','XLU','XLB','XLRE','XLC',
    'ARKK','SOXX','SMH','IBB','GLD','SLV','USO','VXX','UVXY',
]

SECTOR_ETF_MAP = {
    'Technology':             'XLK',
    'Financial Services':     'XLF',
    'Energy':                 'XLE',
    'Healthcare':             'XLV',
    'Industrials':            'XLI',
    'Consumer Cyclical':      'XLY',
    'Consumer Defensive':     'XLP',
    'Utilities':              'XLU',
    'Basic Materials':        'XLB',
    'Real Estate':            'XLRE',
    'Communication Services': 'XLC',
}

TICKER_UNIVERSE = list(dict.fromkeys(NASDAQ_100 + SP500_STOCKS + KEY_ETFS))
UNIVERSE_SET    = set(TICKER_UNIVERSE)
ETF_SET         = set(KEY_ETFS)
STOCK_UNIVERSE  = [t for t in TICKER_UNIVERSE if t not in ETF_SET]

print('✅ Universe loaded:')
print(f'   NASDAQ 100:           {len(set(NASDAQ_100))}')
print(f'   S&P 500 + custom:     {len(set(SP500_STOCKS))}')
print(f'   Key ETFs:             {len(set(KEY_ETFS))}')
print(f'   TOTAL UNIQUE:         {len(TICKER_UNIVERSE)} ({len(STOCK_UNIVERSE)} stocks)')


def get_market_context():
    print('\nMarket context...')
    try:
        vix_hist  = yf.Ticker('^VIX').history(period='1y')['Close']
        vix_l     = round(float(vix_hist.iloc[-1]), 2)
        vix_pct   = round(float(vix_hist.rank(pct=True).iloc[-1]) * 100, 1)
    except:
        vix_l, vix_pct = 20.0, 50.0

    if   vix_pct < VIX_LOW_PCTILE:  vr, vm = f'LOW (p{vix_pct:.0f} risk-on)',       1.10
    elif vix_pct < VIX_HIGH_PCTILE: vr, vm = f'MODERATE (p{vix_pct:.0f} neutral)',  1.00
    elif vix_pct < 90:               vr, vm = f'ELEVATED (p{vix_pct:.0f} cautious)', 0.85
    else:                            vr, vm = f'HIGH (p{vix_pct:.0f} risk-off)',      0.70

    try:
        qh  = yf.Ticker('QQQ').history(period='60d')
        qc  = float(qh['Close'].iloc[-1])
        q50 = float(qh['Close'].rolling(50).mean().iloc[-1])
        qt  = 'BULLISH' if qc > q50 else 'BEARISH'
        qv  = round(((qc - q50) / q50) * 100, 2)
        qp  = round(qc, 2)
    except:
        qt, qv, qp = 'UNKNOWN', 0.0, 0.0

    try:
        spy_hist = yf.Ticker('SPY').history(period='5d')
        spy_ret  = round(((float(spy_hist['Close'].iloc[-1]) -
                           float(spy_hist['Close'].iloc[-2])) /
                           float(spy_hist['Close'].iloc[-2])) * 100, 2)
    except:
        spy_ret = 0.0

    ctx = {
        'vix_level':        vix_l,
        'vix_percentile':   vix_pct,
        'vix_regime':       vr,
        'vix_multiplier':   vm,
        'qqq_trend':        qt,
        'qqq_vs_ma50':      qv,
        'qqq_price':        qp,
        'spy_return_today': spy_ret,
        'defensive_mode':   vix_pct > 90 and qt == 'BEARISH',
    }
    print(f'  VIX:  {vix_l} (p{vix_pct}) -> {vr} ({vm}x)')
    print(f'  QQQ:  ${qp} | {qt} ({qv:+.2f}% vs 50MA)')
    print(f'  SPY today: {spy_ret:+.2f}%')
    if ctx['defensive_mode']:
        print('  *** DEFENSIVE MODE ACTIVE ***')
    return ctx


def batch_download(tickers):
    """Download OHLCV for all tickers in ONE yf.download() call."""
    print(f'\nBatch downloading {len(tickers)} tickers (1 API call)...')
    try:
        raw = yf.download(
            tickers, period='65d',
            auto_adjust=True, progress=False, threads=True
        )
        if raw.empty:
            print('  Batch returned empty'); return {}

        result = {}
        if isinstance(raw.columns, pd.MultiIndex):
            for t in tickers:
                try:
                    df = raw.xs(t, axis=1, level=1).dropna(how='all')
                    if not df.empty and len(df) >= 20:
                        result[t] = df
                except:
                    continue
        else:
            if len(tickers) == 1 and not raw.empty:
                result[tickers[0]] = raw

        print(f'  Downloaded: {len(result)}/{len(tickers)} tickers')
        return result
    except Exception as e:
        print(f'  Batch error: {e}'); return {}


def _fetch_stock_news_single(ticker):
    """Fetch news for one ticker."""
    try:
        news   = yf.Ticker(ticker).news or []
        titles = []
        for n in news[:8]:
            t = n.get('content', {}).get('title', '') or n.get('title', '')
            if t:
                titles.append(t.lower())
        return ticker, titles
    except:
        return ticker, []


def fetch_all_stock_news_parallel(tickers):
    """Fetch yfinance .news for ALL tickers in parallel."""
    print(f'  Fetching news for all {len(tickers)} stocks (parallel)...')
    results = {}
    with ThreadPoolExecutor(max_workers=NEWS_WORKERS) as executor:
        futures = {executor.submit(_fetch_stock_news_single, t): t for t in tickers}
        for future in as_completed(futures):
            ticker, news = future.result()
            results[ticker] = news
    has_news = sum(1 for v in results.values() if v)
    print(f'  News found: {has_news}/{len(tickers)} stocks have news today')
    return results


def _fetch_fundamentals_single(ticker):
    """Fetch fundamentals for one ticker."""
    result = {
        'earnings_date':      'Unknown',
        'earnings_days_away': -1,
        'earnings_risk':      False,
        'analyst_rating':     None,
        'analyst_target':     None,
        'short_ratio':        None,
        'upside_pct':         None,
        'sector':             'Unknown',
        'mkt_cap_b':          0.0,
    }
    try:
        info  = yf.Ticker(ticker).info
        rec   = info.get('recommendationMean')
        tgt   = info.get('targetMeanPrice')
        price = info.get('currentPrice') or info.get('regularMarketPrice')
        sr    = info.get('shortRatio')
        sec   = info.get('sector', 'Unknown')
        mc    = info.get('marketCap', 0)

        if rec:   result['analyst_rating'] = round(float(rec), 1)
        if tgt:   result['analyst_target'] = round(float(tgt), 2)
        if tgt and price and float(price) > 0:
            result['upside_pct'] = round(((float(tgt) - float(price)) / float(price)) * 100, 1)
        if sr:    result['short_ratio'] = round(float(sr), 1)
        result['sector']    = sec or 'Unknown'
        result['mkt_cap_b'] = round((mc or 0) / 1e9, 1)
    except:
        pass

    try:
        cal   = yf.Ticker(ticker).calendar
        today = datetime.now().date()
        ed_raw = None

        if isinstance(cal, dict):
            ed_raw = cal.get('Earnings Date')
            if isinstance(ed_raw, list) and ed_raw:
                ed_raw = ed_raw[0]
        elif cal is not None and hasattr(cal, 'empty') and not cal.empty:
            if 'Earnings Date' in cal.index:
                vals = cal.loc['Earnings Date'].values
                if len(vals) > 0:
                    ed_raw = vals[0]

        if ed_raw is not None:
            ed        = pd.to_datetime(ed_raw).date()
            days_away = (ed - today).days
            result['earnings_date']      = str(ed)
            result['earnings_days_away'] = days_away
            result['earnings_risk']      = 0 <= days_away <= 5
    except:
        pass

    return ticker, result


def fetch_all_fundamentals_parallel(tickers):
    """Fetch fundamentals for ALL tickers in parallel."""
    print(f'  Fetching fundamentals for all {len(tickers)} stocks (parallel)...')
    results = {}
    with ThreadPoolExecutor(max_workers=NEWS_WORKERS) as executor:
        futures = {executor.submit(_fetch_fundamentals_single, t): t for t in tickers}
        for future in as_completed(futures):
            ticker, data = future.result()
            results[ticker] = data
    full = sum(1 for v in results.values()
               if v['analyst_rating'] is not None and v['earnings_date'] != 'Unknown')
    print(f'  Full fundamentals: {full}/{len(tickers)} stocks')
    return results


def fetch_macro_news():
    """16 RSS feed categories, ~60 headlines total, all free."""
    feeds = [
        ('US_MARKET',    'https://finance.yahoo.com/rss/topstories'),
        ('US_MARKET',    'https://finance.yahoo.com/rss/2.0/headline?s=^GSPC&region=US&lang=en-US'),
        ('GEOPOLITICAL', 'https://feeds.bbci.co.uk/news/world/rss.xml'),
        ('GEOPOLITICAL', 'https://feeds.reuters.com/reuters/worldNews'),
        ('GEOPOLITICAL', 'https://rss.nytimes.com/services/xml/rss/nyt/World.xml'),
        ('US_POLICY',    'https://feeds.reuters.com/reuters/politicsNews'),
        ('US_POLICY',    'https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml'),
        ('ENERGY',       'https://feeds.reuters.com/reuters/USenergyNews'),
        ('ENERGY',       'https://oilprice.com/rss/main'),
        ('US_MARKET',    'https://www.cnbc.com/id/100003114/device/rss/rss.html'),
        ('US_MARKET',    'https://feeds.marketwatch.com/marketwatch/topstories/'),
        ('US_MARKET',    'https://feeds.marketwatch.com/marketwatch/bulletins/'),
        ('FED',          'https://feeds.reuters.com/reuters/USfinancialNews'),
        ('SECTOR',       'https://feeds.reuters.com/reuters/businessNews'),
        ('EARNINGS',     'https://finance.yahoo.com/rss/2.0/headline?s=earnings&region=US&lang=en-US'),
        ('TECH',         'https://www.cnbc.com/id/19854910/device/rss/rss.html'),
    ]
    headlines = []
    feed_counts = {}
    for category, url in feeds:
        try:
            r    = requests.get(url, timeout=8, headers={'User-Agent': 'Mozilla/5.0'})
            root = ET.fromstring(r.content)
            ct   = 0
            for item in root.iter('item'):
                title = item.findtext('title', '').strip()
                if title and len(title) > 10:
                    headlines.append(f'[{category}] {title}')
                    ct += 1
                    if ct >= 6: break
            feed_counts[url.split('/')[2]] = ct
        except:
            continue
    active = sum(1 for v in feed_counts.values() if v > 0)
    print(f'  RSS feeds active: {active}/{len(feeds)} | Headlines: {len(headlines)}')
    return headlines[:60]


def fetch_sector_etf_news():
    """Fetch news for all sector ETFs."""
    sector_news = {}
    failed      = []
    for sector, etf in SECTOR_ETF_MAP.items():
        _, news = _fetch_stock_news_single(etf)
        if news:
            sector_news[sector] = news
        else:
            try:
                ticker = yf.Ticker(etf)
                raw    = ticker.news or []
                titles = []
                for n in raw[:5]:
                    t = n.get('content', {}).get('title', '') or n.get('title', '')
                    if t:
                        titles.append(t.lower())
                if titles:
                    sector_news[sector] = titles
                else:
                    failed.append(f'{etf}({sector})')
            except:
                failed.append(f'{etf}({sector})')
    print(f'  Sector news fetched: {len(sector_news)}/{len(SECTOR_ETF_MAP)} sectors')
    return sector_news


def compute_indicators(df, spy_return_today=0.0):
    """Compute all technical indicators from OHLCV DataFrame."""
    if df is None or len(df) < 20:
        return None
    try:
        hi = df['High']
        lo = df['Low']
        cl = df['Close']
        vo = df['Volume']
        p  = float(cl.iloc[-1])

        if p <= 0:
            return None

        m20  = float(cl.rolling(20).mean().iloc[-1])
        m50  = float(cl.rolling(50).mean().iloc[-1]) if len(cl) >= 50 else p
        m200 = float(cl.rolling(200).mean().iloc[-1]) if len(cl) >= 200 else p

        av     = float(vo.rolling(30).mean().iloc[-1]) if len(vo) >= 30 else float(vo.mean())
        vr     = float(vo.iloc[-1]) / av if av > 0 else 0.0
        dvol_m = round((p * av) / 1e6, 1)

        if len(vo) >= 3:
            v1, v2, v3 = float(vo.iloc[-3]), float(vo.iloc[-2]), float(vo.iloc[-1])
            vol_accel = v3 > v2 > v1
        else:
            vol_accel = False

        delta     = cl.diff()
        gain      = delta.where(delta > 0, 0.0).rolling(14).mean()
        loss      = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
        loss_safe = loss.replace(0, 1e-10)
        rsi_val   = float((100 - (100 / (1 + gain / loss_safe))).iloc[-1]) if len(cl) >= 14 else 50.0

        p5  = float(cl.iloc[-5])  if len(cl) >= 5  else p
        p20 = float(cl.iloc[-20]) if len(cl) >= 20 else p

        w52_high  = float(cl.rolling(min(252, len(cl))).max().iloc[-1])
        w52_low   = float(cl.rolling(min(252, len(cl))).min().iloc[-1])
        pct_from_52h = round(((p - w52_high) / w52_high) * 100, 1) if w52_high > 0 else 0.0

        prev_cl = cl.shift(1)
        tr = pd.concat([hi - lo,
                        (hi - prev_cl).abs(),
                        (lo - prev_cl).abs()], axis=1).max(axis=1)
        atr     = float(tr.rolling(14).mean().iloc[-1]) if len(tr) >= 14 else float(tr.mean())
        atr_pct = round((atr / p) * 100, 2) if p > 0 else 0.0

        ema12     = cl.ewm(span=12, adjust=False).mean()
        ema26     = cl.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        sig_line  = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - sig_line
        macd_bull = bool(macd_line.iloc[-1] > sig_line.iloc[-1] and float(macd_hist.iloc[-1]) > 0)

        up_move  = hi.diff()
        dn_move  = -lo.diff()
        plus_dm  = pd.Series(
            np.where((up_move > dn_move) & (up_move > 0), up_move, 0.0),
            index=cl.index)
        minus_dm = pd.Series(
            np.where((dn_move > up_move) & (dn_move > 0), dn_move, 0.0),
            index=cl.index)
        atr_w    = tr.ewm(alpha=1/14, adjust=False).mean()
        safe_atr = atr_w.replace(0, 1e-10)
        plus_di  = 100 * (plus_dm.ewm(alpha=1/14, adjust=False).mean() / safe_atr)
        minus_di = 100 * (minus_dm.ewm(alpha=1/14, adjust=False).mean() / safe_atr)
        dx       = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1e-10)
        adx_val  = float(dx.ewm(alpha=1/14, adjust=False).mean().iloc[-1]) if len(dx) >= 14 else 0.0

        bb_mid   = cl.rolling(20).mean()
        bb_std   = cl.rolling(20).std()
        bb_upper = bb_mid + 2 * bb_std
        bb_lower = bb_mid - 2 * bb_std
        bb_range = float((bb_upper - bb_lower).iloc[-1])
        bb_pct_b = float((p - float(bb_lower.iloc[-1])) / bb_range) if bb_range > 0 else 0.5

        obv = (np.sign(cl.diff()) * vo).fillna(0).cumsum()
        obv_trend = float(obv.iloc[-1]) > float(obv.iloc[-10]) if len(obv) >= 10 else False

        mfv = ((cl - lo) - (hi - cl)) / (hi - lo).replace(0, 1e-10) * vo
        cmf_val = (float(mfv.rolling(20).sum().iloc[-1]) /
                   float(vo.rolling(20).sum().replace(0, 1e-10).iloc[-1])) if len(cl) >= 20 else 0.0
        cmf_val = round(max(-1.0, min(1.0, cmf_val)), 3)

        rsi_series = 100 - (100 / (1 + gain / loss_safe))
        if len(rsi_series) >= 28:
            rsi_min14 = float(rsi_series.rolling(14).min().iloc[-1])
            rsi_max14 = float(rsi_series.rolling(14).max().iloc[-1])
            rng = rsi_max14 - rsi_min14
            stoch_rsi_val = round((float(rsi_series.iloc[-1]) - rsi_min14) / (rng + 1e-10), 3) if rng > 1 else 0.5
        else:
            stoch_rsi_val = 0.5

        if len(hi) >= 5:
            h_vals = [float(hi.iloc[-i]) for i in range(1, 6)]
            l_vals = [float(lo.iloc[-i]) for i in range(1, 6)]
            hh_hl_val = (h_vals[0] > h_vals[1] > h_vals[2] and
                         l_vals[0] > l_vals[1] > l_vals[2])
        else:
            hh_hl_val = False

        tp      = (hi + lo + cl) / 3
        vwap_20 = float((tp * vo).rolling(20).sum().iloc[-1] /
                        vo.rolling(20).sum().replace(0, 1e-10).iloc[-1]) if len(cl) >= 20 else p
        vs_vwap = round(((p - vwap_20) / vwap_20) * 100, 2) if vwap_20 > 0 else 0.0

        if len(cl) >= 2:
            stock_ret_today = ((p - float(cl.iloc[-2])) / float(cl.iloc[-2])) * 100
            rs_vs_spy       = round(stock_ret_today - spy_return_today, 2)
        else:
            rs_vs_spy = 0.0

        return {
            'price':          round(p, 2),
            'ma20':           round(m20, 2),
            'ma50':           round(m50, 2),
            'ma200':          round(m200, 2),
            'vol_ratio':      round(vr, 2),
            'dollar_vol_m':   dvol_m,
            'vol_accel':      vol_accel,
            'rsi':            round(rsi_val, 1),
            'vs_ma20_pct':    round(((p - m20) / m20) * 100, 2) if m20 > 0 else 0.0,
            'vs_ma50_pct':    round(((p - m50) / m50) * 100, 2) if m50 > 0 else 0.0,
            'vs_ma200_pct':   round(((p - m200) / m200) * 100, 2) if m200 > 0 else 0.0,
            'momentum_5d':    round(((p - p5) / p5) * 100, 2) if p5 > 0 else 0.0,
            'momentum_20d':   round(((p - p20) / p20) * 100, 2) if p20 > 0 else 0.0,
            'pct_from_52h':   pct_from_52h,
            'w52_high':       round(w52_high, 2),
            'atr':            round(atr, 2),
            'atr_pct':        atr_pct,
            'macd_bullish':   macd_bull,
            'adx':            round(adx_val, 1),
            'bb_pct_b':       round(bb_pct_b, 2),
            'obv_rising':     obv_trend,
            'rs_vs_spy':      rs_vs_spy,
            'cmf':            cmf_val,
            'stoch_rsi':      stoch_rsi_val,
            'hh_hl':          hh_hl_val,
            'vs_vwap_pct':    vs_vwap,
        }
    except Exception as e:
        if os.environ.get('SCREENER_DEBUG'):
            print(f'  [debug] compute_indicators failed: {type(e).__name__}: {e}')
        return None


def screen_technical(batch_data, ctx):
    """Screen ALL tickers using batch data."""
    is_weekend    = datetime.now().weekday() >= 5
    vol_threshold = 1.0 if is_weekend else VOLUME_MIN_RATIO

    print(f'\nPhase 2 - Technical screening ({len(batch_data)} tickers)...')
    passed   = {}
    rejects  = {'price': 0, 'ma': 0, 'vol': 0, 'rsi': 0, 'liquidity': 0, 'adx': 0}

    for t, df in batch_data.items():
        if t in ETF_SET:
            continue
        ind = compute_indicators(df, ctx['spy_return_today'])
        if ind is None:
            rejects['price'] += 1; continue
        if ind['vs_ma20_pct'] < 0 or ind['vs_ma50_pct'] < 0:
            rejects['ma'] += 1; continue
        if ind['vol_ratio'] < vol_threshold:
            rejects['vol'] += 1; continue
        if not (RSI_MIN <= ind['rsi'] <= RSI_MAX):
            rejects['rsi'] += 1; continue
        if ind['dollar_vol_m'] < MIN_DOLLAR_VOLUME_M:
            rejects['liquidity'] += 1; continue
        if ind['adx'] < ADX_MIN:
            rejects['adx'] += 1; continue
        passed[t] = ind

    print(f'  Passed: {len(passed)} | Rejected: '
          f'Price:{rejects["price"]} MA:{rejects["ma"]} '
          f'Vol:{rejects["vol"]} RSI:{rejects["rsi"]} '
          f'Liq:{rejects["liquidity"]} ADX:{rejects["adx"]}')
    return passed


def screen_news(batch_data, all_stock_news, technical_passed, ctx):
    """Check ALL stocks for significant news, rescue failed ones with major news."""
    print(f'\nPhase 3 - News screening all {len(batch_data)} stocks...')
    rescued = {}
    checked = rescued_count = 0

    for t, news_titles in all_stock_news.items():
        if t in ETF_SET or t in technical_passed or not news_titles:
            continue
        checked += 1
        rescue, t1, t2r, t2c = has_significant_news(news_titles)
        keyword_hits = t1 + t2r + t2c
        if not rescue:
            continue
        df  = batch_data.get(t)
        ind = compute_indicators(df, ctx['spy_return_today']) if df is not None else None
        if ind is None or ind['price'] <= MIN_PRICE or ind['dollar_vol_m'] < MIN_DOLLAR_VOLUME_M:
            continue
        rescued[t] = ind
        rescued[t]['rescue_keywords'] = keyword_hits[:5]
        rescued_count += 1
        print(f'  Rescued: {t} | ${ind["price"]} | Keywords: {keyword_hits[:3]}')

    print(f'  Checked {checked} failed stocks | Rescued: {rescued_count}')
    return rescued


def merge_candidates(technical_passed, news_rescued, all_stock_news, fundamentals):
    """Merge technical and news pools."""
    print(f'\nPhase 4 - Merging candidate pools...')
    candidates = []

    for t, ind in technical_passed.items():
        news = all_stock_news.get(t, [])
        has_news, t1, t2r, t2c = has_significant_news(news)
        keyword_hits = t1 + t2r + t2c
        source = 'BOTH' if has_news else 'TECHNICAL'
        fund   = fundamentals.get(t, {})
        candidates.append({
            'ticker': t, 'source': source, 'news_sourced': source in ('NEWS','BOTH'),
            **ind,
            'sector':             fund.get('sector', 'Unknown'),
            'mkt_cap_b':          fund.get('mkt_cap_b', 0.0),
            'analyst_rating':     fund.get('analyst_rating'),
            'analyst_target':     fund.get('analyst_target'),
            'upside_pct':         fund.get('upside_pct'),
            'short_ratio':        fund.get('short_ratio'),
            'earnings_date':      fund.get('earnings_date', 'Unknown'),
            'earnings_days_away': fund.get('earnings_days_away', -1),
            'earnings_risk':      fund.get('earnings_risk', False),
            'stock_news':         news[:5],
            'rescue_keywords':    keyword_hits[:5] if keyword_hits else [],
        })

    for t, ind in news_rescued.items():
        fund = fundamentals.get(t, {})
        news = all_stock_news.get(t, [])
        candidates.append({
            'ticker': t, 'source': 'NEWS', 'news_sourced': True,
            **ind,
            'sector':             fund.get('sector', 'Unknown'),
            'mkt_cap_b':          fund.get('mkt_cap_b', 0.0),
            'analyst_rating':     fund.get('analyst_rating'),
            'analyst_target':     fund.get('analyst_target'),
            'upside_pct':         fund.get('upside_pct'),
            'short_ratio':        fund.get('short_ratio'),
            'earnings_date':      fund.get('earnings_date', 'Unknown'),
            'earnings_days_away': fund.get('earnings_days_away', -1),
            'earnings_risk':      fund.get('earnings_risk', False),
            'stock_news':         news[:5],
            'rescue_keywords':    ind.get('rescue_keywords', []),
        })

    both = sum(1 for c in candidates if c['source'] == 'BOTH')
    tech = sum(1 for c in candidates if c['source'] == 'TECHNICAL')
    news = sum(1 for c in candidates if c['source'] == 'NEWS')
    print(f'  Total candidates: {len(candidates)}')
    print(f'    BOTH (tech+news): {both}  TECHNICAL: {tech}  NEWS: {news}')
    er = [c['ticker'] for c in candidates if c.get('earnings_risk')]
    if er: print(f'  Earnings risk flags: {er}')
    return candidates


def get_news_intelligence(candidates, ctx, headlines, sector_news, all_stock_news):
    """3-layer news intelligence via Claude."""
    print(f'\nPhase 5 - News intelligence ({len(candidates)} candidates, 3 layers)...')

    sectors_in_pool  = list(set([c['sector'] for c in candidates if c['sector'] != 'Unknown']))
    macro_text       = '\n'.join(headlines[:20])
    sector_text      = '\n'.join([f'[{sec}] {" | ".join(n[:3])}' for sec, n in sector_news.items() if sec in sectors_in_pool]) or 'No sector news'
    stock_news_pool  = {c['ticker']: all_stock_news.get(c['ticker'], []) for c in candidates}
    stock_text       = '\n'.join([f'{t}: {h[0]}' for t, h in stock_news_pool.items() if h]) or 'No individual stock news'

    all_tickers    = [c['ticker'] for c in candidates]
    both_tickers   = [c['ticker'] for c in candidates if c['source'] == 'BOTH']
    news_tickers   = [c['ticker'] for c in candidates if c['source'] in ('NEWS','BOTH')]
    earnings_risks = [c['ticker'] for c in candidates if c.get('earnings_risk')]
    analyst_lines  = [f'{c["ticker"]}:rating={c["analyst_rating"]},upside={c["upside_pct"]}%' for c in candidates if c.get('analyst_rating') is not None]
    today          = datetime.now().strftime('%B %d %Y')

    both_note     = f'\nSTRONGEST (passed tech AND have news): {both_tickers}' if both_tickers else ''
    news_note     = f'\nNEWS-SOURCED (relaxed tech filters): {news_tickers}'   if news_tickers else ''
    earnings_note = f'\nEARNINGS RISK (within 5 days): {earnings_risks}'       if earnings_risks else ''

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    system = 'You are a financial analyst. Respond with ONLY a valid JSON object. Start with { and end with }. No markdown, no explanation.'
    user = (
        f'Today: {today}\n'
        f'VIX={ctx["vix_level"]} (p{ctx["vix_percentile"]}) | QQQ={ctx["qqq_trend"]} ({ctx["qqq_vs_ma50"]:+.2f}% vs 50MA) | SPY today={ctx["spy_return_today"]:+.2f}%\n\n'
        f'LAYER 1 - MACRO HEADLINES:\n{macro_text}\n\n'
        f'LAYER 2 - SECTOR NEWS:\n{sector_text}\n\n'
        f'LAYER 3 - STOCK NEWS:\n{stock_text}\n\n'
        f'ANALYST CONSENSUS:\n{chr(10).join(analyst_lines) if analyst_lines else "No data"}\n\n'
        f'ALL CANDIDATES: {all_tickers}{both_note}{news_note}{earnings_note}\n'
        f'SECTORS: {sectors_in_pool}\n\n'
        'Return this JSON:\n'
        '{"macro_summary":"2-3 sentences",'
        '"trump_signal":{"detected":false,"detail":"none","affected_sectors":[],"score_adjustment":0},'
        '"fed_signal":{"detected":false,"detail":"none","tone":"neutral","score_adjustment":0},'
        '"macro_data_signal":{"detected":false,"detail":"none","score_adjustment":0},'
        '"geopolitical_signal":{"detected":false,"detail":"none","score_adjustment":0},'
        '"stock_signals":[{"ticker":"X","news":"summary","auto_drop":false,"score_adjustment":0}],'
        '"sector_signals":[{"sector":"X","news":"summary","score_adjustment":0}],'
        '"overall_market_adjustment":0,'
        '"market_sentiment":"NEUTRAL"}'
    )

    try:
        resp = client.messages.create(model=CLAUDE_MODEL, max_tokens=2000, system=system, messages=[{'role':'user','content':user}])
        raw = resp.content[0].text.strip()
        if '```' in raw:
            for part in raw.split('```'):
                part = part.strip()
                if part.startswith('json'): part = part[4:].strip()
                if part.startswith('{'): raw = part; break
        if '{' in raw: raw = raw[raw.index('{'):]
        nd = json.loads(raw)
        print(f'  Sentiment: {nd.get("market_sentiment","?")} | Adj: {nd.get("overall_market_adjustment",0):+d}')
        return nd
    except Exception as e:
        print(f'  News intelligence failed ({e}) - neutral baseline')
        return {'macro_summary':'Unavailable','trump_signal':{'detected':False,'score_adjustment':0,'affected_sectors':[]},'fed_signal':{'detected':False,'score_adjustment':0,'tone':'neutral'},'macro_data_signal':{'detected':False,'score_adjustment':0},'geopolitical_signal':{'detected':False,'score_adjustment':0},'stock_signals':[],'sector_signals':[],'overall_market_adjustment':0,'market_sentiment':'NEUTRAL'}


def apply_news(candidates, nd):
    """Apply news adjustments including auto-drops and earnings penalties."""
    sm   = {s['ticker']: s for s in nd.get('stock_signals', [])}
    secm = {s['sector']:  s for s in nd.get('sector_signals', [])}
    oadj = nd.get('overall_market_adjustment', 0)
    tr   = nd.get('trump_signal', {})

    enriched = []
    for c in candidates:
        adj, drop, notes = oadj, False, []
        if tr.get('detected') and c['sector'] in tr.get('affected_sectors', []):
            ta = tr.get('score_adjustment', 0)
            if ta <= -20: drop = True; notes.append('AUTO DROP: Trump targeting sector')
            else: adj += ta; notes.append(f'Trump:{ta:+d}')
        if c['ticker'] in sm:
            sn = sm[c['ticker']]
            if sn.get('auto_drop'): drop = True; notes.append('AUTO DROP: negative stock news')
            else:
                sa = sn.get('score_adjustment', 0); adj += sa
                if sa: notes.append(f'News:{sa:+d}')
        if c['sector'] in secm:
            sa = secm[c['sector']].get('score_adjustment', 0); adj += sa
            if sa: notes.append(f'Sector:{sa:+d}')
        if c.get('earnings_risk'):
            adj -= 15; notes.append('Earnings:-15')
        c['news_adjustment'] = adj
        c['auto_drop']       = drop
        c['news_notes']      = ' | '.join(notes) if notes else 'No major news'
        enriched.append(c)

    dropped = [c for c in enriched if c['auto_drop']]
    remain  = [c for c in enriched if not c['auto_drop']]
    if dropped: print(f'  AUTO DROPPED: {", ".join([c["ticker"] for c in dropped])}')
    print(f'  {len(remain)} candidates after news filter')
    return remain


def stream_b_from_headlines(headlines, batch_data, technical_passed, all_stock_news, fundamentals, ctx):
    """Extract tickers specifically mentioned in macro headlines."""
    if not headlines: return []
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    try:
        resp = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=200,
            system='Extract US stock tickers from headlines. Return ONLY a JSON array like ["AAPL","GOOGL"]. No explanation.',
            messages=[{'role':'user','content':f'HEADLINES:\n{chr(10).join(headlines)}\n\nRules: US stocks only, no ETFs, no indices, max 15 tickers, [] if none.'}]
        )
        raw = resp.content[0].text.strip()
        if '[' in raw:
            raw = raw[raw.index('['):]
            depth = 0
            for i, ch in enumerate(raw):
                if ch=='[': depth+=1
                elif ch==']':
                    depth-=1
                    if depth==0: raw=raw[:i+1]; break
        news_tickers = [t.upper().strip() for t in json.loads(raw) if isinstance(t,str)]
    except:
        return []

    existing   = set(technical_passed.keys())
    new_tickers = [t for t in news_tickers if t in UNIVERSE_SET and t not in existing and t not in ETF_SET]
    if not new_tickers: return []

    print(f'  Stream B: Processing headline mentions: {new_tickers}')
    b_cands = []
    for t in new_tickers:
        df  = batch_data.get(t)
        ind = compute_indicators(df, ctx['spy_return_today']) if df is not None else None
        if ind is None or ind['price'] <= MIN_PRICE: continue
        fund = fundamentals.get(t, {})
        news = all_stock_news.get(t, [])
        b_cands.append({
            'ticker':t,'source':'NEWS','news_sourced':True,**ind,
            'sector':fund.get('sector','Unknown'),'mkt_cap_b':fund.get('mkt_cap_b',0.0),
            'analyst_rating':fund.get('analyst_rating'),'analyst_target':fund.get('analyst_target'),
            'upside_pct':fund.get('upside_pct'),'short_ratio':fund.get('short_ratio'),
            'earnings_date':fund.get('earnings_date','Unknown'),'earnings_days_away':fund.get('earnings_days_away',-1),
            'earnings_risk':fund.get('earnings_risk',False),'stock_news':news[:5],'rescue_keywords':[],
        })
        print(f'  Stream B added: {t} | ${ind["price"]} | RSI {ind["rsi"]}')
    return b_cands


def analyze_with_claude(candidates, ctx, nd, pick_history=None):
    """Final scoring with pre_score awareness + self-calibration."""
    if not candidates: return None
    print(f'\nPhase 6 - Claude final scoring ({len(candidates)} candidates)...')

    if len(candidates) > 30:
        candidates = sorted(candidates, key=lambda x: x.get('pre_score',0), reverse=True)[:30]
        print(f'  Trimmed to top 30 by pre_score')

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    mult   = ctx['vix_multiplier']

    hard_rule_flags = {}
    filtered = []
    for c in candidates:
        rsi=c.get('rsi',50); upside=c.get('upside_pct'); t=c['ticker']; flags=[]
        if rsi>80 and upside is not None and upside<-20:
            print(f'  DISQUALIFIED: {t} | RSI={rsi}>80 AND analyst target {upside}% below price'); continue
        if rsi>80:  flags.append(f'RSI_CAP(rsi={rsi}>80,max_conf=70)')
        if upside is not None and upside<-20: flags.append(f'ANALYST_CAP(upside={upside}%<-20,max_conf=65)')
        if flags: hard_rule_flags[t]=flags
        filtered.append(c)

    if not filtered:
        return {'top_pick':{'ticker':'NONE','confidence':0,'signal':'NO PICK','reasoning':'All disqualified.','key_risk':'N/A','sector':'N/A','source':'N/A'},'watch_candidates':[]}
    candidates = filtered

    compact = [{
        'ticker':c['ticker'],'source':c['source'],'price':c['price'],'sector':c['sector'],
        'tech_score':c.get('tech_score',0),'news_score':c.get('news_score',0),'pre_score':c.get('pre_score',0),
        'tech_bd':c.get('tech_score_breakdown',{}),'news_bd':c.get('news_score_breakdown',{}),
        'rsi':c['rsi'],'adx':c['adx'],'vol_ratio':c['vol_ratio'],'macd_bull':c['macd_bullish'],
        'momentum_5d':c['momentum_5d'],'rs_vs_spy':c['rs_vs_spy'],'pct_from_52h':c['pct_from_52h'],
        'cmf':c.get('cmf',0.0),'stoch_rsi':c.get('stoch_rsi',0.5),'hh_hl':c.get('hh_hl',False),
        'vader_label':c.get('vader_label','NEUTRAL'),'options_label':c.get('options_label','NEUTRAL'),
        'insider_label':c.get('insider_label','NEUTRAL'),'news_adjustment':c.get('news_adjustment',0),
        'news_notes':c.get('news_notes',''),'mkt_cap_b':c['mkt_cap_b'],
        **({k:c[k] for k in ['analyst_rating','upside_pct','short_ratio','earnings_risk','rescue_keywords','options_pc'] if c.get(k) is not None})
    } for c in candidates]

    hist_block = ''
    if pick_history:
        wins=sum(1 for h in pick_history if h['result']=='Win'); total=len(pick_history)
        wr=round(wins/total*100,1) if total else 0
        hist_block = (f'\nYOUR RECENT PICK HISTORY (last {total} evaluated picks - {wr}% win rate):\n'
                      + '\n'.join([f'  {h["ticker"]} conf={h["confidence"]} [{h["source"]}] -> {h["result"]} ({h["return_30d"]}%30d)' for h in pick_history[-8:]])
                      + '\nSelf-calibrate: raise the bar if recent picks are losing.\n')

    hard_note = ('\nHARD CAPS APPLIED:\n' + '\n'.join([f'{t}: {" ".join(fs)}' for t,fs in hard_rule_flags.items()]) + '\nRSI_CAP=max 70 | ANALYST_CAP=max 65\n') if hard_rule_flags else ''
    both_t=[c['ticker'] for c in candidates if c['source']=='BOTH']
    news_t=[c['ticker'] for c in candidates if c['source']=='NEWS']
    src_note = (f'\nBOTH (strongest): {both_t}' if both_t else '') + (f'\nNEWS-SOURCED: {news_t}' if news_t else '')

    system = ('You are a senior equity trader specialising in momentum and catalyst trades. '
              'Respond with ONLY a valid JSON object. Start with { end with }. Nothing else.')
    user = (
        f'MARKET REGIME:\nVIX={ctx["vix_level"]} (p{ctx["vix_percentile"]}) {ctx["vix_regime"]} | '
        f'mult={mult}x | QQQ={ctx["qqq_trend"]} {ctx["qqq_vs_ma50"]:+.2f}% | SPY today={ctx["spy_return_today"]:+.2f}% | Defensive={"YES" if ctx["defensive_mode"] else "NO"}\n\n'
        f'MACRO CONTEXT:\n{nd.get("macro_summary","N/A")}\nSentiment: {nd.get("market_sentiment","NEUTRAL")} | Adj: {nd.get("overall_market_adjustment",0):+d}\n'
        f'FED: {nd.get("fed_signal",{}).get("detail","none")}\nTRUMP: {nd.get("trump_signal",{}).get("detail","none")}\n'
        f'DATA: {nd.get("macro_data_signal",{}).get("detail","none")}\nGEO: {nd.get("geopolitical_signal",{}).get("detail","none")}'
        f'{src_note}{hard_note}{hist_block}\n\n'
        f'CANDIDATES (with pre-computed scores):\n{json.dumps(compact, indent=2)}\n\n'
        f'YOUR JOB: Review pre_score as grounded baseline. Apply qualitative adjustments Claude formulas cannot capture. Add news_adjustment. Apply VIX multiplier {mult}x. Clamp 0-100.\n\n'
        f'Return ONLY this JSON:\n'
        f'{{"top_pick":{{"ticker":"X","confidence":85,"signal":"BUY","tech_score":48,"news_score":32,"pre_score":80,"vix_multiplier":{mult},'
        f'"score_breakdown":"Tech:48/60 | News:32/40 | VIX:{mult}x = 85","reasoning":"Two sentences max.","devils_advocate":"Two risks.","key_risk":"One sentence.","sector":"X","source":"TECHNICAL"}},'
        f'"watch_candidates":[{{"ticker":"X","confidence":74,"signal":"WATCH","tech_score":40,"news_score":28,"pre_score":68,"score_breakdown":"Tech:40 | News:28 | VIX:{mult}x = 74","reasoning":"One sentence.","key_risk":"One sentence.","sector":"X","source":"TECHNICAL"}}]}}\n\n'
        f'signal=NO PICK if confidence < {BUY_THRESHOLD}. Watch range: {WATCH_THRESHOLD}-{BUY_THRESHOLD-1}.'
    )

    try:
        resp = client.messages.create(model=CLAUDE_MODEL, max_tokens=2000, system=system, messages=[{'role':'user','content':user}])
        raw = resp.content[0].text.strip()
        if '```' in raw:
            for part in raw.split('```'):
                part=part.strip()
                if part.startswith('json'): part=part[4:].strip()
                if part.startswith('{'): raw=part; break
        if '{' in raw:
            start=raw.index('{'); depth=0; end=start
            for i,ch in enumerate(raw[start:],start):
                if ch=='{': depth+=1
                elif ch=='}':
                    depth-=1
                    if depth==0: end=i; break
            raw=raw[start:end+1]
        result=json.loads(raw)
        pick=result.get('top_pick',{})
        print(f'  Pick: {pick.get("ticker")} | pre={pick.get("pre_score","?")} -> final={pick.get("confidence")}/100 | {pick.get("signal")}')
        return result
    except Exception as e:
        print(f'  Claude failed: {e}'); return None


PICK_COLS = [
    'Date','Ticker','Confidence','Signal','Source','Sector',
    'Reasoning','Key_Risk','Devils_Advocate',
    'Entry_Price','Realistic_Entry','Stop_Zone','Target_Zone',
    'ATR','ATR_Pct','ADX','MACD_Bull','BB_PctB','OBV_Rising',
    'RS_vs_SPY','Pct_From_52H','Dollar_Vol_M',
    'VIX','VIX_Pct','QQQ_Trend',
    'Analyst_Rating','Upside_Pct','Short_Ratio','Earnings_Risk',
    'Sector_Concentration',
    'Tech_Score','News_Score','Pre_Score','Score_Breakdown',
    'Vader_Label','Options_Label','Insider_Label',
    'Price_10d','Return_10d_pct','vs_QQQ_10d',
    'Price_30d','Return_30d_pct','vs_QQQ_30d','Result'
]
WATCH_COLS = PICK_COLS + ['Watch_Score']


def load_csv(fp, cols):
    if os.path.exists(fp):
        df=pd.read_csv(fp)
        for col in cols:
            if col not in df.columns: df[col]=''
        return df
    return pd.DataFrame(columns=cols)


def check_sector_concentration(sector, picks_csv_path):
    if not os.path.exists(picks_csv_path): return False, 0, []
    try:
        df=pd.read_csv(picks_csv_path)
        if df.empty or 'Sector' not in df.columns: return False, 0, []
        recent=df.tail(SECTOR_CONC_LOOKBACK)
        last_sects=recent['Sector'].tolist()
        same_count=sum(1 for s in last_sects if s==sector)
        return same_count>=SECTOR_CONC_MAX, same_count, last_sects
    except:
        return False, 0, []


def save_pick(pick_data, ctx, price, fp, cols, all_candidates=None, watch_score=None):
    df=load_csv(fp,cols); today=datetime.now().strftime('%Y-%m-%d'); ticker=pick_data['ticker']
    if ((df['Date']==today)&(df['Ticker']==ticker)).any():
        print(f'  Already have {ticker} on {today} - skipping'); return
    match=next((c for c in (all_candidates or []) if c['ticker']==ticker),{})
    atr=match.get('atr',0.0)
    stop_zone   = f'${round(price-ATR_STOP_MULT*atr,2)}'   if atr and isinstance(price,(int,float)) else 'N/A'
    target_zone = f'${round(price+ATR_TARGET_MULT*atr,2)}' if atr and isinstance(price,(int,float)) else 'N/A'
    sector=pick_data.get('sector','Unknown'); conc_str=''
    if fp==PICKS_CSV:
        is_conc,cnt,recent=check_sector_concentration(sector,PICKS_CSV)
        if is_conc:
            conc_str=f'{cnt}/{SECTOR_CONC_LOOKBACK} recent in {sector}'
            pick_data['confidence']=max(0,pick_data['confidence']-SECTOR_CONC_PENALTY)
    row={
        'Date':today,'Ticker':ticker,'Confidence':pick_data['confidence'],'Signal':pick_data['signal'],
        'Source':pick_data.get('source','TECHNICAL'),'Sector':sector,
        'Reasoning':pick_data.get('reasoning',''),'Key_Risk':pick_data.get('key_risk',''),
        'Devils_Advocate':pick_data.get('devils_advocate',''),
        'Entry_Price':price,'Realistic_Entry':'','Stop_Zone':stop_zone,'Target_Zone':target_zone,
        'ATR':match.get('atr',''),'ATR_Pct':match.get('atr_pct',''),'ADX':match.get('adx',''),
        'MACD_Bull':match.get('macd_bullish',''),'BB_PctB':match.get('bb_pct_b',''),
        'OBV_Rising':match.get('obv_rising',''),'RS_vs_SPY':match.get('rs_vs_spy',''),
        'Pct_From_52H':match.get('pct_from_52h',''),'Dollar_Vol_M':match.get('dollar_vol_m',''),
        'VIX':ctx['vix_level'],'VIX_Pct':ctx.get('vix_percentile',''),'QQQ_Trend':ctx['qqq_trend'],
        'Analyst_Rating':match.get('analyst_rating',''),'Upside_Pct':match.get('upside_pct',''),
        'Short_Ratio':match.get('short_ratio',''),'Earnings_Risk':match.get('earnings_risk',False),
        'Sector_Concentration':conc_str,
        'Tech_Score':pick_data.get('tech_score',match.get('tech_score','')),'News_Score':pick_data.get('news_score',match.get('news_score','')),'Pre_Score':pick_data.get('pre_score',match.get('pre_score','')),'Score_Breakdown':pick_data.get('score_breakdown',''),
        'Vader_Label':match.get('vader_label',''),'Options_Label':match.get('options_label',''),'Insider_Label':match.get('insider_label',''),
        'Price_10d':'','Return_10d_pct':'','vs_QQQ_10d':'','Price_30d':'','Return_30d_pct':'','vs_QQQ_30d':'','Result':'Pending'
    }
    if watch_score is not None: row['Watch_Score']=watch_score
    pd.concat([df,pd.DataFrame([row])],ignore_index=True).to_csv(fp,index=False)
    print(f'  ✅ Saved {ticker} | Stop:{stop_zone} Target:{target_zone}')


def update_results(fp, cols):
    if not os.path.exists(fp): return
    df=pd.read_csv(fp); today=datetime.now(); updated=False
    for idx,row in df.iterrows():
        if str(row.get('Ticker','')).strip() in ['NONE','nan','']: continue
        if str(row.get('Entry_Price','')).strip() in ['N/A','nan','']: continue
        try:
            pd_=datetime.strptime(str(row['Date']),'%Y-%m-%d'); el=(today-pd_).days; ep=float(row['Entry_Price'])
            st=yf.Ticker(str(row['Ticker'])); qq=yf.Ticker('QQQ')
            if str(row.get('Realistic_Entry','')).strip() in ['','nan','NaN']:
                ns=pd_+timedelta(days=1)
                if ns.date()<=today.date():
                    h=st.history(start=ns,end=pd_+timedelta(days=5))
                    if not h.empty: df.at[idx,'Realistic_Entry']=round(float(h['Open'].iloc[0]),2); updated=True
            def gp(t):
                h=st.history(start=t-timedelta(days=4),end=t+timedelta(days=4))
                return round(float(h['Close'].iloc[0]),2) if not h.empty else None
            def gq(s,e):
                h=qq.history(start=s-timedelta(days=2),end=e+timedelta(days=4))
                if h.empty or len(h)<2: return None
                return ((float(h['Close'].iloc[-1])-float(h['Close'].iloc[0]))/float(h['Close'].iloc[0]))*100
            if el>=10 and str(row.get('Price_10d','')).strip() in ['','nan','NaN']:
                p10=gp(pd_+timedelta(days=10))
                if p10:
                    r10=round(((p10-ep)/ep)*100,2); qr=gq(pd_,pd_+timedelta(days=10))
                    df.at[idx,'Price_10d']=p10; df.at[idx,'Return_10d_pct']=r10; df.at[idx,'vs_QQQ_10d']=round(r10-qr,2) if qr else ''; updated=True
            if el>=30 and str(row.get('Price_30d','')).strip() in ['','nan','NaN']:
                p30=gp(pd_+timedelta(days=30))
                if p30:
                    r30=round(((p30-ep)/ep)*100,2); qr=gq(pd_,pd_+timedelta(days=30)); vs30=round(r30-qr,2) if qr else ''
                    res='Win' if r30>3 and vs30!='' and float(str(vs30))>0 else 'Loss' if r30<-2 else 'Neutral'
                    df.at[idx,'Price_30d']=p30; df.at[idx,'Return_30d_pct']=r30; df.at[idx,'vs_QQQ_30d']=vs30; df.at[idx,'Result']=res; updated=True
        except: continue
    if updated: df.to_csv(fp,index=False); print(f'  {os.path.basename(fp)} updated')


def display_result(result, ctx, nd, ep, wl, all_candidates=None):
    pk=result.get('top_pick',{}); conf=pk.get('confidence',0); sig=pk.get('signal','NO PICK')
    src=pk.get('source','TECHNICAL'); ticker=pk.get('ticker','')
    fund=next((c for c in (all_candidates or []) if c['ticker']==ticker),{})
    print('\n'+'='*65)
    print('  DAILY STOCK SCREENER v6.2 - RESULT')
    print('='*65)
    print(f'  Date: {datetime.now().strftime("%Y-%m-%d")} | VIX: {ctx["vix_level"]} (p{ctx["vix_percentile"]}) | QQQ: {ctx["qqq_trend"]} | SPY: {ctx["spy_return_today"]:+.2f}%')
    print(f'  Sentiment: {nd.get("market_sentiment","NEUTRAL")} | Macro: {nd.get("macro_summary","")[:90]}...')
    print('-'*65)
    if sig=='NO PICK' or conf<BUY_THRESHOLD:
        print('  NO PICK TODAY')
        print(f'  Reason: {pk.get("reasoning","Nothing cleared the threshold")}')
    else:
        atr=fund.get('atr',0)
        stop_price  = round(ep-ATR_STOP_MULT*atr,2)  if atr and isinstance(ep,(int,float)) else 'N/A'
        tgt_price   = round(ep+ATR_TARGET_MULT*atr,2) if atr and isinstance(ep,(int,float)) else 'N/A'
        risk_share  = round(ep-stop_price,2)           if atr and isinstance(ep,(int,float)) else 'N/A'
        er_warn = ' ⚠️ EARNINGS RISK' if fund.get('earnings_risk') else ''
        print(f'  TICKER:     {ticker} [{src}]{er_warn}')
        print(f'  SIGNAL:     {sig} | CONFIDENCE: {conf}/100 | SECTOR: {pk.get("sector")}')
        print(f'  ENTRY REF:  ${ep} | STOP: ~${stop_price} | TARGET: ~${tgt_price} | R:R ~1:2')
        print(f'  Score:      {pk.get("score_breakdown","")}')
        print(f'  Reasoning:  {pk.get("reasoning","")}')
        print(f'  Key Risk:   {pk.get("key_risk","")}')
        if pk.get('devils_advocate'): print(f'  Devils Adv: {pk.get("devils_advocate","")}')
    if wl:
        print('\n  WATCH LIST:')
        for w in wl:
            wf=next((c for c in (all_candidates or []) if c['ticker']==w.get('ticker')), {})
            print(f'  WATCH: {w.get("ticker")} | {w.get("confidence")}/100 | {w.get("sector")} | {w.get("reasoning","")}')
    print('='*65)


def display_scorecard():
    print('\n'+'='*65+'\n  SCORECARD\n'+'='*65)
    for label, fp in [('CONFIRMED PICKS',PICKS_CSV),('WATCH LIST',WATCH_CSV)]:
        if not os.path.exists(fp): continue
        df=pd.read_csv(fp); done=df[df['Result'].isin(['Win','Loss','Neutral'])]; pend=df[df['Result']=='Pending']
        print(f'\n  {label}: {len(df)} total | {len(pend)} pending | {len(done)} evaluated')
        if not done.empty:
            w=len(done[done['Result']=='Win']); l=len(done[done['Result']=='Loss']); n=len(done[done['Result']=='Neutral'])
            wr=round((w/len(done))*100,1); ar=pd.to_numeric(done['Return_30d_pct'],errors='coerce').mean()
            print(f'  Win:{w} Loss:{l} Neutral:{n} | Win Rate:{wr}% | Avg30d:{ar:+.2f}%')
        base=['Date','Ticker','Confidence','Source','Entry_Price','Stop_Zone','Target_Zone','Return_10d_pct','Return_30d_pct','Result']
        show=[c for c in base if c in df.columns]
        print(df[show].tail(5).to_string(index=False))
    print(f'\n  📁 Folder: {DRIVE_FOLDER}\n'+'='*65)


print('\n✅ All functions loaded - v6.2')
print('▶  Run Cell 5 to start the screener')


# ============================================================
# CELL 5 - RUN DAILY SCREENER
# ============================================================

def run_screener():
    print('🚀 DAILY STOCK SCREENER v6.2')
    print(f'   Time:   {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print(f'   Folder: {DRIVE_FOLDER}')
    print(f'   Stocks: {len(STOCK_UNIVERSE)} | ETFs: {len(KEY_ETFS)}')
    print('='*65)

    print('\nStep 1/8: Updating past results...')
    update_results(PICKS_CSV, PICK_COLS)
    update_results(WATCH_CSV, WATCH_COLS)

    print('\nStep 2/8: Market context...')
    ctx = get_market_context()

    print('\nStep 3/8: Fetching all data (parallel)...')
    batch_data = batch_download(STOCK_UNIVERSE + KEY_ETFS)
    if not batch_data:
        print('  Batch download failed - cannot proceed')
        display_scorecard(); return None

    headlines        = fetch_macro_news()
    all_stock_news   = fetch_all_stock_news_parallel(STOCK_UNIVERSE)
    all_fundamentals = fetch_all_fundamentals_parallel(STOCK_UNIVERSE)
    sector_news      = fetch_sector_etf_news()
    sector_ranks, _  = compute_sector_ranks(batch_data)

    print('\nStep 4/8: Bidirectional screening...')
    technical_passed = screen_technical(batch_data, ctx)
    news_rescued     = screen_news(batch_data, all_stock_news, technical_passed, ctx)
    candidates       = merge_candidates(technical_passed, news_rescued, all_stock_news, all_fundamentals)

    print('\nStep 4.5/8: Stream B - headline ticker extraction...')
    b_cands = stream_b_from_headlines(headlines, batch_data, technical_passed, all_stock_news, all_fundamentals, ctx)
    existing = {c['ticker'] for c in candidates}
    for c in b_cands:
        if c['ticker'] not in existing:
            candidates.append(c); existing.add(c['ticker'])

    if not candidates:
        print('\nNo candidates from any stream today - NO PICK')
        display_scorecard(); return None

    print('\nStep 4.6/8: Options P/C + insider signals...')
    options_data, insider_data = fetch_options_and_insider_parallel(candidates)

    print('\nStep 5/8: News intelligence (3 layers)...')
    nd         = get_news_intelligence(candidates, ctx, headlines, sector_news, all_stock_news)
    candidates = apply_news(candidates, nd)

    if not candidates:
        print('All candidates dropped by news filter')
        display_scorecard(); return None

    market_sentiment = nd.get('market_sentiment','NEUTRAL')
    candidates = enrich_with_scores(candidates, ctx, market_sentiment, sector_ranks, options_data, insider_data)

    pick_history = load_performance_history(PICKS_CSV)
    if pick_history:
        wins=sum(1 for h in pick_history if h['result']=='Win')
        print(f'  Self-calibration: {len(pick_history)} prior picks, {wins/len(pick_history)*100:.0f}% win rate')

    print('\nStep 6/8: Claude final scoring...')
    result = analyze_with_claude(candidates, ctx, nd, pick_history=pick_history)
    if not result:
        print('Claude analysis failed - check your API key in Cell 3'); return None

    # Post-hoc hard cap enforcement
    pick = result.get('top_pick',{})
    if pick and pick.get('ticker') not in (None,'','NONE'):
        match=next((c for c in candidates if c['ticker']==pick['ticker']),{})
        rsi=match.get('rsi',50); upside=match.get('upside_pct'); orig=pick.get('confidence',0)
        if rsi>80 and pick.get('confidence',0)>70:
            pick['confidence']=70; print(f'  Hard cap: RSI={rsi}>80, clamped {orig}->70')
        if upside is not None and upside<-20 and pick.get('confidence',0)>65:
            pick['confidence']=65; print(f'  Hard cap: analyst upside={upside}%<-20, clamped ->65')
        if pick['confidence']<BUY_THRESHOLD:
            pick['signal']='WATCH' if pick['confidence']>=WATCH_THRESHOLD else 'NO PICK'

    wl=result.get('watch_candidates',[]); conf=pick.get('confidence',0); sig=pick.get('signal','NO PICK')
    ep='N/A'
    if sig=='BUY' and conf>=BUY_THRESHOLD:
        try: ep=round(float(yf.Ticker(pick['ticker']).history(period='2d')['Close'].iloc[-1]),2)
        except: ep='N/A'

    display_result(result, ctx, nd, ep, wl, all_candidates=candidates)

    print('\nStep 8/8: Saving results...')
    if sig=='BUY' and conf>=BUY_THRESHOLD:
        save_pick(pick, ctx, ep, PICKS_CSV, PICK_COLS, all_candidates=candidates)
    for w in wl:
        if w.get('confidence',0)>=WATCH_THRESHOLD:
            try: wp=round(float(yf.Ticker(w['ticker']).history(period='2d')['Close'].iloc[-1]),2)
            except: wp='N/A'
            save_pick(w, ctx, wp, WATCH_CSV, WATCH_COLS, all_candidates=candidates, watch_score=w.get('confidence'))

    display_scorecard()
    return result


result = run_screener()
