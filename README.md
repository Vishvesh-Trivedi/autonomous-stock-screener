# 🤖 Autonomous Stock Screener — Powered by Claude AI

> Built by an OSS Architect. Driven by curiosity. Running every day.

A fully automated daily stock screener that combines technical analysis, NLP sentiment, macro news intelligence, and Claude AI reasoning to surface high-conviction **BUY / WATCH / NO PICK** candidates — every evening after market close.

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![Anthropic](https://img.shields.io/badge/Powered%20by-Claude%20AI-orange.svg)](https://anthropic.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Cost](https://img.shields.io/badge/Cost%20per%20run-~%240.05--0.10-brightgreen.svg)]()

---

## 🧠 Philosophy

**Architecture first, code second.**

This project was designed as a complete pipeline before a single line was written. Claude (Anthropic) acts as the reasoning layer — not just a chatbot, but a structured analytical engine with full market context fed at inference time.

---

## ✨ Features

- **375+ stocks screened** in a single API call every evening
- **Bidirectional screening** — technical filters AND news-driven rescue running in parallel
- **10 pre-score indicators**: RSI, MACD, ADX, CMF, StochRSI, VWAP, OBV, Options P/C ratio, Insider flow, VADER NLP sentiment
- **3-layer news intelligence**: macro RSS headlines → sector ETF news → per-stock news
- **Claude AI reasoning layer** — top 30 candidates with full macro context, sector rotation, earnings risk, self-calibration
- **Structured output** — BUY/WATCH/NO PICK with stop zones, targets, R:R ratio, devil's advocate, score breakdown
- **Zero-infrastructure deployment** — Google Colab + Google Drive
- **Performance tracking** — 10d and 30d return tracking, win rate by confidence band

---

## 🛠️ Tech Stack

| Component | Tool |
|---|---|
| Market data | `yfinance` (free) |
| NLP sentiment | `VADER` (free, no API key) |
| Macro news | 16 RSS feeds — Reuters, CNBC, MarketWatch, BBC, NYT |
| AI reasoning | Anthropic Claude API (`claude-sonnet-4-5`) |
| Execution | Google Colab |
| Storage | Google Drive |

---

## ⚙️ Setup

### API Keys Required

| Service | Where | Cost |
|---|---|---|
| Anthropic API | [console.anthropic.com](https://console.anthropic.com) | ~$0.05–0.10/run |
| Google Drive | Your Google account | Free |

---

## 🚀 Option 1 — Google Colab (Recommended)

1. Upload the `.py` file to [colab.research.google.com](https://colab.research.google.com)
2. Click 🔑 **Secrets** → add `ANTHROPIC_API_KEY` = your key
3. Mount Google Drive when prompted
4. **Runtime → Run all**

## 💻 Option 2 — Local

```bash
git clone https://github.com/Vishvesh-Trivedi/autonomous-stock-screener.git
cd autonomous-stock-screener
pip install anthropic yfinance vaderSentiment pandas numpy requests python-dotenv
```

Create `.env`:
```
ANTHROPIC_API_KEY=your-key-here
```

Run:
```bash
python Daily_Stock_Screener_v6_2_GitHub.py
```

---

## 📊 Sample Output

```
=================================================================
  DAILY STOCK SCREENER v6.2 - RESULT
=================================================================
  Date:       2026-04-28
  VIX:        17.83 (p56) | MODERATE | 1.0x multiplier
  QQQ:        $657.55 | BULLISH (+8.07% vs 50MA)
  Sentiment:  CAUTIOUS | Adj: -8
-----------------------------------------------------------------
  TICKER:     CTRA [NEWS]
  SIGNAL:     BUY | CONFIDENCE: 82/100 | SECTOR: Energy
  ENTRY:      $34.63 | STOP: ~$33.03 | TARGET: ~$37.84  R:R 1:2
  Score:      Tech:38/60 | News:28/40 | VIX:1.0x = 82
  Options P/C: 0.52 (BULLISH)
  RSI=58.0 | ADX=19.0 | MACD=bull | RS_vs_SPY=+3.34%
  Reasoning:  Energy rotation with UAE/OPEC tensions + bullish options
  Devils Adv: Low volume limits conviction. Reversal risk on OPEC news.
  WATCH: DVN 78 | TRGP 76 | VTR 74
=================================================================
```

---

## 🗺️ Roadmap

- [x] Bidirectional screening (technical + news rescue)
- [x] Deterministic pre-scoring before Claude
- [x] Options P/C + insider signals
- [x] VADER NLP sentiment
- [x] Self-calibration from historical picks
- [x] Devil's advocate on every pick
- [ ] **Sharesies API** → automated order execution
- [ ] Backtesting module on historical CSV
- [ ] Telegram/email alerts
- [ ] Web dashboard

> 💬 **@Sharesies** — 3 years as a loyal user. If you launch a trading API, this is the use case. The screener already knows what to buy, the stop, the target, and the R:R. It just needs the green light.

---

## 🏗️ Architecture

![Architecture Diagram](architecture/screener_v62_architecture.png)

---

## ⚠️ Disclaimer

This is a personal learning project. It is **not financial advice**. Always do your own research before making any investment decisions.

---

## 🙏 Acknowledgements

Built with [Claude](https://anthropic.com) by Anthropic · Data via [yfinance](https://github.com/ranaroussi/yfinance) · Sentiment via [VADER](https://github.com/cjhutto/vaderSentiment)

---

## 🤝 Contributing

- Found a bug? [Open an issue](https://github.com/Vishvesh-Trivedi/autonomous-stock-screener/issues)
- Got a better indicator? Submit a PR
- Want to discuss results? [Start a discussion](https://github.com/Vishvesh-Trivedi/autonomous-stock-screener/discussions)

---

## 📬 Connect

Built by **Vishvesh Trivedi** — OSS Architect | AI/ML Automation | 12 Patents | Rakuten Mobile Alumni

[LinkedIn](https://www.linkedin.com/in/vishvesh-trivedi) · #TheRoadToAutonomousNetworks

> *"Sometimes the best way to learn a domain is to build something real in it."* 🚀
