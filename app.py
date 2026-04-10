import math
import re
from datetime import datetime, date, time
from zoneinfo import ZoneInfo
from xml.etree import ElementTree as ET

import numpy as np
import pandas as pd
import requests
import streamlit as st
import yfinance as yf
from bs4 import BeautifulSoup
from streamlit_autorefresh import st_autorefresh

# =========================================================
# APP CONFIG
# =========================================================
st.set_page_config(
    page_title="Indian Market Intelligence Pro",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Refresh every 15 minutes
st_autorefresh(interval=900_000, key="market_intel_refresh")
IST = ZoneInfo("Asia/Kolkata")

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# =========================================================
# UNIVERSE / SECTORS
# =========================================================
SCAN_UNIVERSE = {
    "RELIANCE": {"name": "Reliance Industries", "sector": "Energy"},
    "TCS": {"name": "TCS", "sector": "IT"},
    "INFY": {"name": "Infosys", "sector": "IT"},
    "HCLTECH": {"name": "HCL Tech", "sector": "IT"},
    "WIPRO": {"name": "Wipro", "sector": "IT"},
    "TECHM": {"name": "Tech Mahindra", "sector": "IT"},
    "HDFCBANK": {"name": "HDFC Bank", "sector": "Banking"},
    "ICICIBANK": {"name": "ICICI Bank", "sector": "Banking"},
    "AXISBANK": {"name": "Axis Bank", "sector": "Banking"},
    "SBIN": {"name": "State Bank of India", "sector": "Banking"},
    "KOTAKBANK": {"name": "Kotak Mahindra Bank", "sector": "Banking"},
    "INDUSINDBK": {"name": "IndusInd Bank", "sector": "Banking"},
    "LT": {"name": "Larsen & Toubro", "sector": "Infrastructure"},
    "RVNL": {"name": "RVNL", "sector": "Infrastructure"},
    "POWERGRID": {"name": "Power Grid", "sector": "Infrastructure"},
    "ADANIPORTS": {"name": "Adani Ports", "sector": "Infrastructure"},
    "NTPC": {"name": "NTPC", "sector": "Energy"},
    "TATAPOWER": {"name": "Tata Power", "sector": "Energy"},
    "COALINDIA": {"name": "Coal India", "sector": "Energy"},
    "BEL": {"name": "Bharat Electronics", "sector": "Defence"},
    "HAL": {"name": "HAL", "sector": "Defence"},
    "MAZDOCK": {"name": "Mazagon Dock", "sector": "Defence"},
    "TATAMOTORS": {"name": "Tata Motors", "sector": "Auto"},
    "MARUTI": {"name": "Maruti Suzuki", "sector": "Auto"},
    "M&M": {"name": "Mahindra & Mahindra", "sector": "Auto"},
    "EICHERMOT": {"name": "Eicher Motors", "sector": "Auto"},
    "DIXON": {"name": "Dixon Technologies", "sector": "Manufacturing"},
    "ULTRACEMCO": {"name": "UltraTech Cement", "sector": "Materials"},
    "JSWSTEEL": {"name": "JSW Steel", "sector": "Materials"},
    "TATASTEEL": {"name": "Tata Steel", "sector": "Materials"},
    "GRASIM": {"name": "Grasim", "sector": "Materials"},
    "ITC": {"name": "ITC", "sector": "Consumer"},
    "HINDUNILVR": {"name": "Hindustan Unilever", "sector": "Consumer"},
    "ASIANPAINT": {"name": "Asian Paints", "sector": "Consumer"},
    "TITAN": {"name": "Titan", "sector": "Consumer"},
    "SUNPHARMA": {"name": "Sun Pharma", "sector": "Pharma"},
    "BHARTIARTL": {"name": "Bharti Airtel", "sector": "Telecom"},
    "BAJFINANCE": {"name": "Bajaj Finance", "sector": "NBFC"},
    "BAJAJFINSV": {"name": "Bajaj Finserv", "sector": "NBFC"},
    "GODREJPROP": {"name": "Godrej Properties", "sector": "Realty"},
}

INDEX_TICKERS = {
    "NIFTY 50": "^NSEI",
    "BANK NIFTY": "^NSEBANK",
    "SENSEX": "^BSESN",
    "INDIA VIX": "^INDIAVIX",
}

GLOBAL_TICKERS = {
    "S&P 500": "^GSPC",
    "NASDAQ": "^IXIC",
    "DOW": "^DJI",
    "NIKKEI": "^N225",
    "HANG SENG": "^HSI",
    "SHANGHAI": "000001.SS",
    "BRENT": "BZ=F",
    "WTI": "CL=F",
    "GOLD": "GC=F",
    "USDINR_PROXY": "INR=X",
}

NSE_HOLIDAYS_URL = "https://www.nseindia.com/resources/exchange-communication-holidays"
NSE_HOME_URL = "https://www.nseindia.com/"
GIFT_NIFTY_URL = "https://in.investing.com/indices/gift-nifty-50-c1-futures-historical-data"

PIB_RSS_URL = "https://www.pib.gov.in/rssMain.aspx?reg=3&lang=1"
PIB_ECONOMY_RSS_URL = "https://www.pib.gov.in/ViewRss.aspx?reg=3&lang=1"
RBI_PRESS_URL = "https://www.rbi.org.in/commonman/english/scripts/PressReleases.aspx"
WHITEHOUSE_BRIEFING_URL = "https://www.whitehouse.gov/briefing-room/"
GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"

POSITIVE_WORDS = {
    "ceasefire", "cooling", "de-escalation", "eases", "rebound", "beats",
    "stimulus", "rate cut", "liquidity", "approval", "surge", "rally",
    "growth", "strong", "recover", "positive", "stable", "holding",
    "boost", "support", "inflow", "record order", "wins order",
    "expansion", "upgrade", "soft landing", "fall in oil", "order win",
    "guidance raised", "outperform"
}
NEGATIVE_WORDS = {
    "war", "tariff", "sanction", "inflation", "spike", "selloff", "fall",
    "crash", "downgrade", "tightening", "volatility", "ban", "concern",
    "fear", "outflow", "conflict", "disruption", "weak", "uncertain",
    "lawsuit", "fragile", "shortage", "misses", "cuts guidance",
    "hot inflation", "oil spike", "rate hike", "profit warning"
}
SECTOR_KEYWORDS = {
    "Banking": ["rbi", "repo", "bank", "liquidity", "rupee", "credit"],
    "IT": ["ai", "tech", "software", "cloud", "nasdaq"],
    "Energy": ["oil", "crude", "brent", "wti", "gas", "hormuz"],
    "Defence": ["defence", "military", "war", "border"],
    "Infrastructure": ["capex", "infrastructure", "rail", "road", "power", "construction"],
    "Consumer": ["inflation", "consumption", "retail", "demand"],
    "Materials": ["metal", "steel", "aluminium", "copper", "commodity", "cement"],
    "Auto": ["auto", "vehicle", "car", "ev"],
    "Pharma": ["drug", "pharma", "healthcare", "usfda"],
    "Telecom": ["telecom", "5g", "spectrum"],
    "NBFC": ["credit", "finance", "loan", "rate"],
    "Realty": ["property", "real estate", "housing"],
    "Manufacturing": ["manufacturing", "electronics", "pli"],
}

# =========================================================
# THEME / RESPONSIVE LAYOUT
# =========================================================
st.markdown("""
<style>
:root {
    --bg1:#0a1220;
    --bg2:#101a2c;
    --panel:#ffffff;
    --panel-soft:#f7f9fc;
    --border:#d9e2ef;
    --text:#0f172a;
    --muted:#5b6b85;
    --green:#16a34a;
    --red:#dc2626;
    --amber:#d97706;
    --blue:#2563eb;
}
.stApp {
    background:
      radial-gradient(circle at 0% 0%, rgba(37,99,235,0.10), transparent 28%),
      radial-gradient(circle at 100% 0%, rgba(14,165,233,0.08), transparent 25%),
      linear-gradient(180deg, #f5f7fb 0%, #eef3f9 100%);
}
.block-container {
    max-width: 1280px !important;
    padding-top: 0.9rem !important;
    padding-left: 1rem !important;
    padding-right: 1rem !important;
    padding-bottom: 2rem !important;
}
h1,h2,h3,h4,h5,h6,p,span,div,label { color: var(--text); }

[data-testid="stSidebar"] {
    width: 270px !important;
    min-width: 270px !important;
    background: linear-gradient(180deg, #0b1730 0%, #0f2142 100%);
    border-right: 1px solid rgba(255,255,255,0.06);
}
[data-testid="stSidebar"] * {
    color: #eef4ff !important;
}
.hero {
    border-radius: 22px;
    padding: 22px 22px 18px 22px;
    background: linear-gradient(135deg, #0f2b59 0%, #123870 45%, #1e3a8a 100%);
    border: 1px solid rgba(255,255,255,0.08);
    box-shadow: 0 14px 34px rgba(15,23,42,0.16);
    margin-bottom: 14px;
}
.main-title {
    font-size: 2.15rem;
    font-weight: 800;
    letter-spacing: -0.03em;
    line-height: 1.1;
    color: white;
}
.subtitle {
    color: #dbe7ff;
    margin-top: 0.2rem;
    margin-bottom: 0.85rem;
    font-size: 1rem;
}
.metric-card {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 16px;
    min-height: 132px;
    box-shadow: 0 8px 24px rgba(15,23,42,0.06);
    display: flex;
    flex-direction: column;
    justify-content: space-between;
}
.metric-label {
    color: #4f6aa3;
    text-transform: uppercase;
    font-size: 0.74rem;
    font-weight: 800;
    letter-spacing: 0.08em;
}
.metric-value {
    color: var(--text);
    font-size: 1.68rem;
    font-weight: 800;
    margin-top: 6px;
}
.metric-sub {
    color: var(--muted);
    font-size: 0.89rem;
    margin-top: 7px;
}
.section-title {
    font-size: 1.08rem;
    font-weight: 800;
    margin: 0.25rem 0 0.8rem 0;
    color: var(--text);
}
.note-box {
    background: var(--panel);
    border: 1px solid var(--border);
    border-left: 4px solid var(--blue);
    border-radius: 16px;
    padding: 14px 16px;
    margin-bottom: 12px;
    box-shadow: 0 8px 20px rgba(15,23,42,0.05);
}
.good-box {
    background: #effaf3;
    border: 1px solid #ccefd9;
    border-left: 4px solid var(--green);
    border-radius: 16px;
    padding: 14px 16px;
}
.warn-box {
    background: #fff8eb;
    border: 1px solid #f7dfb0;
    border-left: 4px solid var(--amber);
    border-radius: 16px;
    padding: 14px 16px;
}
.badge {
    display: inline-block;
    padding: 0.38rem 0.72rem;
    border-radius: 999px;
    font-size: 0.78rem;
    font-weight: 800;
    margin-right: 8px;
}
.badge-green { background: #e9f9ef; color: #15803d; border: 1px solid #bde6c9; }
.badge-red { background: #feeeee; color: #b91c1c; border: 1px solid #f6c7c7; }
.badge-amber { background: #fff5e6; color: #b45309; border: 1px solid #f0d5a5; }
.idea-card {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 16px;
    min-height: 250px;
    box-shadow: 0 8px 22px rgba(15,23,42,0.05);
}
.small-muted {
    color: var(--muted);
    font-size: 0.87rem;
}
hr.soft {
    border: 0;
    border-top: 1px solid #e7edf5;
    margin: 10px 0;
}
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
    flex-wrap: nowrap !important;
    overflow-x: auto;
}
.stTabs [data-baseweb="tab"] {
    white-space: nowrap;
    border-radius: 999px !important;
    background: #edf2fa !important;
    border: 1px solid #dbe5f2 !important;
    color: #17315f !important;
    padding: 8px 14px !important;
}
[data-testid="stDataFrame"] {
    border-radius: 14px;
    overflow: hidden;
}
@media (max-width: 1000px) {
    .block-container {
        padding-left: 0.8rem !important;
        padding-right: 0.8rem !important;
    }
    .main-title {
        font-size: 1.55rem;
    }
    .metric-value {
        font-size: 1.35rem;
    }
    .metric-card {
        min-height: 110px;
        padding: 14px;
    }
    [data-testid="stSidebar"] {
        width: 100% !important;
        min-width: auto !important;
    }
}
</style>
""", unsafe_allow_html=True)

# =========================================================
# HELPERS
# =========================================================
def safe_float(value, default=np.nan):
    try:
        return float(value)
    except Exception:
        return default

def pct_change(current, previous):
    if previous in [0, None] or pd.isna(previous) or pd.isna(current):
        return np.nan
    return ((current / previous) - 1) * 100

def clean_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        if len(set(df.columns.get_level_values(0))) == 1:
            df.columns = df.columns.get_level_values(1)
        elif len(set(df.columns.get_level_values(1))) == 1:
            df.columns = df.columns.get_level_values(0)
    cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    out = df[cols].copy()
    return out.dropna(subset=["Close"])

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    line = ema_fast - ema_slow
    sig = line.ewm(span=signal, adjust=False).mean()
    hist = line - sig
    return line, sig, hist

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high_low = df["High"] - df["Low"]
    high_close = (df["High"] - df["Close"].shift()).abs()
    low_close = (df["Low"] - df["Close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def enrich_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 60:
        return pd.DataFrame()
    x = df.copy()
    x["EMA20"] = x["Close"].ewm(span=20, adjust=False).mean()
    x["EMA50"] = x["Close"].ewm(span=50, adjust=False).mean()
    x["EMA200"] = x["Close"].ewm(span=200, adjust=False).mean()
    x["RSI14"] = rsi(x["Close"], 14)
    x["ATR14"] = atr(x, 14)
    x["VOL20"] = x["Volume"].rolling(20).mean()
    x["VOL_RATIO"] = x["Volume"] / x["VOL20"]
    x["HH20"] = x["High"].rolling(20).max()
    x["LL20"] = x["Low"].rolling(20).min()
    x["RET_5D"] = x["Close"].pct_change(5)
    x["RET_20D"] = x["Close"].pct_change(20)
    macd_line, macd_sig, macd_hist = macd(x["Close"])
    x["MACD"] = macd_line
    x["MACD_SIGNAL"] = macd_sig
    x["MACD_HIST"] = macd_hist
    return x.dropna()

def bias_badge(label: str) -> str:
    if label in ["BULLISH", "TRADABLE", "BUY NOW", "POSITIVE", "BULLISH OPEN"]:
        return "badge badge-green"
    if label in ["BEARISH", "AVOID / VERY SELECTIVE", "AVOID", "NEGATIVE", "WEAK OPEN"]:
        return "badge badge-red"
    return "badge badge-amber"

def sector_impact(text: str) -> list[str]:
    t = text.lower()
    out = []
    for sector, kws in SECTOR_KEYWORDS.items():
        if any(k in t for k in kws):
            out.append(sector)
    return out

# =========================================================
# DATA FETCH
# =========================================================
@st.cache_data(ttl=300)
def fetch_history(symbol: str, period: str = "6mo", interval: str = "1d") -> pd.DataFrame:
    try:
        df = yf.download(
            symbol,
            period=period,
            interval=interval,
            auto_adjust=True,
            progress=False,
            threads=False,
        )
        return clean_ohlcv(df)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_quote_snapshot(symbol: str) -> dict:
    df = fetch_history(symbol, period="10d", interval="1d")
    if df.empty or len(df) < 2:
        return {}
    last = safe_float(df["Close"].iloc[-1])
    prev = safe_float(df["Close"].iloc[-2])
    return {"last": last, "prev": prev, "chg_pct": pct_change(last, prev)}

@st.cache_data(ttl=3600)
def fetch_fundamentals(symbol: str) -> dict:
    out = {
        "symbol": symbol,
        "market_cap": np.nan,
        "trailing_pe": np.nan,
        "forward_pe": np.nan,
        "price_to_book": np.nan,
        "debt_to_equity": np.nan,
        "roe": np.nan,
        "roce": np.nan,
        "profit_margin": np.nan,
        "revenue_growth": np.nan,
        "earnings_growth": np.nan,
        "current_ratio": np.nan,
        "quick_ratio": np.nan,
        "next_earnings_date": None,
        "has_data": False,
    }
    try:
        tk = yf.Ticker(f"{symbol}.NS")
        info = {}
        try:
            info = tk.info or {}
        except Exception:
            info = {}

        out["market_cap"] = info.get("marketCap", np.nan)
        out["trailing_pe"] = info.get("trailingPE", np.nan)
        out["forward_pe"] = info.get("forwardPE", np.nan)
        out["price_to_book"] = info.get("priceToBook", np.nan)
        out["debt_to_equity"] = info.get("debtToEquity", np.nan)
        out["roe"] = info.get("returnOnEquity", np.nan)
        out["profit_margin"] = info.get("profitMargins", np.nan)
        out["revenue_growth"] = info.get("revenueGrowth", np.nan)
        out["earnings_growth"] = info.get("earningsGrowth", np.nan)
        out["current_ratio"] = info.get("currentRatio", np.nan)
        out["quick_ratio"] = info.get("quickRatio", np.nan)

        try:
            cal = tk.calendar
            if isinstance(cal, dict):
                dt = cal.get("Earnings Date")
                if isinstance(dt, (list, tuple)) and len(dt) > 0:
                    out["next_earnings_date"] = dt[0]
                else:
                    out["next_earnings_date"] = dt
            elif hasattr(cal, "iloc") and len(cal) > 0:
                if "Earnings Date" in cal.index:
                    out["next_earnings_date"] = cal.loc["Earnings Date"].values[0]
        except Exception:
            pass

        try:
            fin = tk.financials
            bs = tk.balance_sheet
            if not fin.empty and not bs.empty:
                ebit = np.nan
                total_assets = np.nan
                current_liabilities = np.nan
                for idx in fin.index:
                    low = str(idx).lower()
                    if low in ["ebit", "operating income"]:
                        ebit = safe_float(fin.iloc[fin.index.get_loc(idx), 0])
                        break
                for idx in bs.index:
                    low = str(idx).lower()
                    if low == "total assets":
                        total_assets = safe_float(bs.iloc[bs.index.get_loc(idx), 0])
                    if "current liabilities" in low:
                        current_liabilities = safe_float(bs.iloc[bs.index.get_loc(idx), 0])
                capital_employed = total_assets - current_liabilities
                if not np.isnan(ebit) and not np.isnan(capital_employed) and capital_employed > 0:
                    out["roce"] = ebit / capital_employed
        except Exception:
            pass

        out["has_data"] = True
    except Exception:
        pass
    return out

# =========================================================
# HOLIDAYS / BREADTH
# =========================================================
@st.cache_data(ttl=21600)
def fetch_nse_trading_holidays(year: int | None = None) -> pd.DataFrame:
    target_year = year or datetime.now(IST).year
    rows = []
    try:
        r = requests.get(NSE_HOLIDAYS_URL, headers=REQUEST_HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text("\n", strip=True)
        pattern = r"\d+,\s+(\d{1,2}-[A-Za-z]{3}-\d{4}),\s+[A-Za-z]+,\s+(.+)"
        matches = re.findall(pattern, text)
        for dt_str, desc in matches:
            try:
                dt = pd.to_datetime(dt_str, format="%d-%b-%Y").date()
                if dt.year == target_year:
                    rows.append({"date": dt, "holiday": desc.strip()})
            except Exception:
                pass
    except Exception:
        pass
    return pd.DataFrame(rows).drop_duplicates()

@st.cache_data(ttl=900)
def fetch_nse_official_breadth() -> dict:
    try:
        session = requests.Session()
        session.headers.update(REQUEST_HEADERS)
        session.get(NSE_HOME_URL, timeout=20)
        r = session.get(NSE_HOME_URL, timeout=20)
        r.raise_for_status()
        text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
        m_adv = re.search(r"Advances\s+(\d[\d,]*)", text, re.IGNORECASE)
        m_dec = re.search(r"Declines\s+(\d[\d,]*)", text, re.IGNORECASE)
        m_unch = re.search(r"Unchanged\s+(\d[\d,]*)", text, re.IGNORECASE)
        if m_adv and m_dec:
            adv = int(m_adv.group(1).replace(",", ""))
            dec = int(m_dec.group(1).replace(",", ""))
            unch = int(m_unch.group(1).replace(",", "")) if m_unch else 0
            total = adv + dec + unch
            return {
                "source": "NSE official scrape",
                "advances": adv,
                "declines": dec,
                "unchanged": unch,
                "adv_pct": round((adv / total) * 100, 2) if total else np.nan,
                "total": total,
            }
    except Exception:
        pass
    return {}

def compute_universe_breadth(universe: dict) -> dict:
    total = 0
    above_ema20 = 0
    above_ema50 = 0
    strong = 0
    for sym in universe:
        df = enrich_daily(fetch_history(f"{sym}.NS", period="6mo", interval="1d"))
        if df.empty:
            continue
        total += 1
        last = df.iloc[-1]
        c = safe_float(last["Close"])
        e20 = safe_float(last["EMA20"])
        e50 = safe_float(last["EMA50"])
        r14 = safe_float(last["RSI14"])
        if c > e20:
            above_ema20 += 1
        if c > e50:
            above_ema50 += 1
        if c > e20 and c > e50 and r14 > 55:
            strong += 1
    return {
        "source": "Computed scan breadth",
        "total": total,
        "above_ema20": above_ema20,
        "above_ema50": above_ema50,
        "strong_count": strong,
        "adv_pct": round((above_ema20 / total) * 100, 2) if total else 0.0,
    }

def market_session_status(trading_holidays: set[date]) -> str:
    now = datetime.now(IST)
    today = now.date()
    if now.weekday() >= 5 or today in trading_holidays:
        return "Closed"
    t = now.time()
    if time(9, 0) <= t < time(9, 15):
        return "Pre-Open"
    if time(9, 15) <= t <= time(15, 30):
        return "Live"
    return "Closed"

# =========================================================
# NEWS LAYER
# =========================================================
def headline_sentiment_score(text: str) -> int:
    t = text.lower()
    score = 0
    for word in POSITIVE_WORDS:
        if word in t:
            score += 1
    for word in NEGATIVE_WORDS:
        if word in t:
            score -= 1
    if ("oil" in t or "crude" in t) and any(w in t for w in ["spike", "surge", "jumps", "rises sharply"]):
        score -= 2
    if ("oil" in t or "crude" in t) and any(w in t for w in ["falls", "drops", "eases", "cooling"]):
        score += 1
    if "ceasefire" in t:
        score += 2
    if "tariff" in t:
        score -= 2
    if "inflation" in t and any(w in t for w in ["hot", "surge", "spike"]):
        score -= 2
    return score

@st.cache_data(ttl=900)
def fetch_gdelt_news(query: str, max_records: int = 20) -> list[dict]:
    items = []
    params = {
        "query": query,
        "mode": "ArtList",
        "maxrecords": max_records,
        "format": "json",
        "sort": "DateDesc",
    }
    try:
        r = requests.get(GDELT_DOC_API, params=params, headers=REQUEST_HEADERS, timeout=25)
        r.raise_for_status()
        data = r.json()
        for a in data.get("articles", []):
            title = a.get("title", "").strip()
            link = a.get("url", "").strip()
            source = a.get("domain", "").strip()
            seendate = a.get("seendate", "")
            if title and link:
                items.append({"title": title, "link": link, "source": source, "pub_date": seendate})
    except Exception:
        pass
    return items

@st.cache_data(ttl=1800)
def fetch_rss_feed(url: str, source_name: str) -> list[dict]:
    items = []
    try:
        r = requests.get(url, headers=REQUEST_HEADERS, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        for item in root.findall(".//item")[:20]:
            title = item.findtext("title", default="").strip()
            link = item.findtext("link", default="").strip()
            pub_date = item.findtext("pubDate", default="").strip()
            if title and link:
                items.append({"title": title, "link": link, "source": source_name, "pub_date": pub_date})
    except Exception:
        pass
    return items

@st.cache_data(ttl=1800)
def fetch_rbi_press_releases() -> list[dict]:
    items = []
    try:
        r = requests.get(RBI_PRESS_URL, headers=REQUEST_HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        links = soup.find_all("a", href=True)
        count = 0
        for a in links:
            title = a.get_text(" ", strip=True)
            href = a["href"]
            if title and len(title) > 15:
                full_url = href if href.startswith("http") else f"https://www.rbi.org.in/{href.lstrip('/')}"
                items.append({"title": title, "link": full_url, "source": "RBI", "pub_date": ""})
                count += 1
                if count >= 15:
                    break
    except Exception:
        pass
    return items

@st.cache_data(ttl=1800)
def fetch_whitehouse_briefings() -> list[dict]:
    items = []
    try:
        r = requests.get(WHITEHOUSE_BRIEFING_URL, headers=REQUEST_HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        seen = set()
        for a in soup.find_all("a", href=True):
            title = a.get_text(" ", strip=True)
            href = a.get("href", "")
            if not title or len(title) < 15 or "/briefing-room/" not in href:
                continue
            full_url = href if href.startswith("http") else f"https://www.whitehouse.gov{href}"
            key = (title, full_url)
            if key in seen:
                continue
            seen.add(key)
            items.append({"title": title, "link": full_url, "source": "White House", "pub_date": ""})
            if len(items) >= 15:
                break
    except Exception:
        pass
    return items

@st.cache_data(ttl=900)
def collect_news() -> pd.DataFrame:
    rows = []
    gdelt_queries = {
        "global_macro": '(sourcecountry:US OR sourcecountry:GB OR sourcecountry:SG OR sourcecountry:IN) AND ("global markets" OR "Wall Street" OR "Asian markets" OR inflation OR tariffs OR oil OR crude)',
        "india_market": '("India stock market" OR Nifty OR Sensex OR RBI OR rupee OR FII OR DII) AND sourcecountry:IN',
        "us_president": '("Trump" OR "White House") AND (tariffs OR oil OR Iran OR markets OR economy)',
    }
    for bucket, q in gdelt_queries.items():
        for item in fetch_gdelt_news(q, max_records=18):
            rows.append({
                "bucket": bucket,
                "title": item["title"],
                "link": item["link"],
                "source": item["source"],
                "pub_date": item["pub_date"],
                "score": headline_sentiment_score(item["title"]),
                "sector_tags": ", ".join(sector_impact(item["title"])),
            })

    for item in fetch_rss_feed(PIB_RSS_URL, "PIB"):
        rows.append({
            "bucket": "govt_policy",
            "title": item["title"],
            "link": item["link"],
            "source": item["source"],
            "pub_date": item["pub_date"],
            "score": headline_sentiment_score(item["title"]),
            "sector_tags": ", ".join(sector_impact(item["title"])),
        })
    for item in fetch_rss_feed(PIB_ECONOMY_RSS_URL, "PIB"):
        rows.append({
            "bucket": "govt_policy",
            "title": item["title"],
            "link": item["link"],
            "source": item["source"],
            "pub_date": item["pub_date"],
            "score": headline_sentiment_score(item["title"]),
            "sector_tags": ", ".join(sector_impact(item["title"])),
        })
    for item in fetch_rbi_press_releases():
        rows.append({
            "bucket": "rbi_policy",
            "title": item["title"],
            "link": item["link"],
            "source": item["source"],
            "pub_date": item["pub_date"],
            "score": headline_sentiment_score(item["title"]),
            "sector_tags": ", ".join(sector_impact(item["title"])),
        })
    for item in fetch_whitehouse_briefings():
        rows.append({
            "bucket": "whitehouse",
            "title": item["title"],
            "link": item["link"],
            "source": item["source"],
            "pub_date": item["pub_date"],
            "score": headline_sentiment_score(item["title"]),
            "sector_tags": ", ".join(sector_impact(item["title"])),
        })

    if not rows:
        return pd.DataFrame(columns=["bucket", "title", "link", "source", "pub_date", "score", "sector_tags"])

    df = pd.DataFrame(rows).drop_duplicates(subset=["title", "link"]).copy()
    df["pub_date"] = df["pub_date"].fillna("")
    return df.sort_values("pub_date", ascending=False)

def summarize_news_bias(news_df: pd.DataFrame) -> dict:
    if news_df.empty:
        return {"score": 0, "label": "NEUTRAL", "reason": "No recent news fetched.", "top_positive": [], "top_negative": []}

    total_score = int(news_df["score"].sum())
    if total_score >= 8:
        label = "POSITIVE"
    elif total_score <= -8:
        label = "NEGATIVE"
    else:
        label = "MIXED"

    top_positive = news_df.sort_values("score", ascending=False).head(6)["title"].tolist()
    top_negative = news_df.sort_values("score", ascending=True).head(6)["title"].tolist()

    reason_parts = []
    if any(news_df["bucket"].eq("rbi_policy")):
        reason_parts.append("RBI policy/news active")
    if any(news_df["title"].str.contains("oil|crude|brent|wti", case=False, na=False)):
        reason_parts.append("oil-sensitive headlines active")
    if any(news_df["title"].str.contains("Trump|White House|tariff", case=False, na=False)):
        reason_parts.append("U.S. policy headlines active")
    if any(news_df["title"].str.contains("election", case=False, na=False)):
        reason_parts.append("election-event risk active")

    return {
        "score": total_score,
        "label": label,
        "reason": "; ".join(reason_parts) if reason_parts else "Mixed headline flow",
        "top_positive": top_positive,
        "top_negative": top_negative,
    }

# =========================================================
# OVERNIGHT / GAP PLAN
# =========================================================
@st.cache_data(ttl=900)
def fetch_gift_nifty_value() -> dict:
    try:
        r = requests.get(GIFT_NIFTY_URL, headers=REQUEST_HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        numbers = re.findall(r"\b\d{1,3},\d{3}\.\d{2}\b", text)
        if numbers:
            return {"value": float(numbers[0].replace(",", ""))}
    except Exception:
        pass
    return {}

def overnight_score() -> dict:
    us_spx = get_quote_snapshot("^GSPC")
    us_ndx = get_quote_snapshot("^IXIC")
    us_dow = get_quote_snapshot("^DJI")
    nikkei = get_quote_snapshot("^N225")
    hsi = get_quote_snapshot("^HSI")
    shanghai = get_quote_snapshot("000001.SS")
    brent = get_quote_snapshot("BZ=F")
    usd_inr = get_quote_snapshot("INR=X")
    gift = fetch_gift_nifty_value()

    score = 0
    reasons = []

    us_avg = np.nanmean([us_spx.get("chg_pct", np.nan), us_ndx.get("chg_pct", np.nan), us_dow.get("chg_pct", np.nan)])
    if not np.isnan(us_avg):
        if us_avg > 0.5:
            score += 2
            reasons.append("Wall Street supportive")
        elif us_avg < -0.5:
            score -= 2
            reasons.append("Wall Street weak")

    asia_avg = np.nanmean([nikkei.get("chg_pct", np.nan), hsi.get("chg_pct", np.nan), shanghai.get("chg_pct", np.nan)])
    if not np.isnan(asia_avg):
        if asia_avg > 0.4:
            score += 2
            reasons.append("Asian markets firm")
        elif asia_avg < -0.4:
            score -= 2
            reasons.append("Asian markets weak")

    brent_chg = brent.get("chg_pct", np.nan)
    if not np.isnan(brent_chg):
        if brent_chg > 1.5:
            score -= 2
            reasons.append("Brent rising sharply")
        elif brent_chg < -1.0:
            score += 1
            reasons.append("Brent easing")

    fx_chg = usd_inr.get("chg_pct", np.nan)
    if not np.isnan(fx_chg) and abs(fx_chg) > 0.5:
        reasons.append("USDINR volatility elevated")

    if gift.get("value"):
        reasons.append(f"GIFT Nifty near {gift['value']:.2f}")

    if score >= 3:
        label = "BULLISH OPEN"
    elif score <= -3:
        label = "WEAK OPEN"
    else:
        label = "MIXED OPEN"

    return {
        "label": label,
        "score": score,
        "reasons": reasons,
        "us_avg": us_avg,
        "asia_avg": asia_avg,
        "brent_chg": brent_chg,
        "gift": gift.get("value", np.nan),
    }

def build_gap_plan(regime: dict, overnight: dict, nifty_df: pd.DataFrame) -> str:
    if nifty_df.empty or len(nifty_df) < 2:
        return "Not enough data for gap plan."
    prev_close = safe_float(nifty_df["Close"].iloc[-1])
    gift = safe_float(overnight.get("gift"), np.nan)
    if np.isnan(gift) or prev_close <= 0:
        return "Wait for first 15 minutes after open. Trade only if index and breadth confirm."
    gap_pct = ((gift / prev_close) - 1) * 100

    if gap_pct > 0.75:
        if regime["label"] == "BULLISH":
            return "Strong positive gap likely. Avoid chasing the first candle. Prefer buy-on-dip after first 15-minute hold."
        return "Positive gap likely, but regime is not strong enough. Wait for opening-range breakout confirmation."
    if 0.20 < gap_pct <= 0.75:
        return "Moderate positive gap likely. Let first 15 minutes settle, then buy strongest sectors on confirmation."
    if -0.20 <= gap_pct <= 0.20:
        return "Flat to muted open likely. Focus on stock-specific strength rather than index direction."
    if -0.75 <= gap_pct < -0.20:
        return "Moderate weak open likely. Avoid early panic selling. Watch whether NIFTY reclaims opening range."
    return "Deep gap-down risk. Protect capital first. Trade only if sharp recovery and breadth improve."

# =========================================================
# REGIME / FUNDAMENTALS / STOCK SCORE
# =========================================================
def score_fundamentals(fund: dict) -> dict:
    score = 50
    notes = []

    pe = safe_float(fund.get("trailing_pe"))
    pb = safe_float(fund.get("price_to_book"))
    dte = safe_float(fund.get("debt_to_equity"))
    roe = safe_float(fund.get("roe"))
    roce = safe_float(fund.get("roce"))
    pm = safe_float(fund.get("profit_margin"))
    rev_g = safe_float(fund.get("revenue_growth"))
    earn_g = safe_float(fund.get("earnings_growth"))
    curr = safe_float(fund.get("current_ratio"))
    quick = safe_float(fund.get("quick_ratio"))

    if not np.isnan(rev_g):
        if rev_g > 0.10:
            score += 10
            notes.append("good revenue growth")
        elif rev_g < 0:
            score -= 10
            notes.append("revenue shrinking")

    if not np.isnan(earn_g):
        if earn_g > 0.10:
            score += 12
            notes.append("good earnings growth")
        elif earn_g < 0:
            score -= 12
            notes.append("earnings shrinking")

    if not np.isnan(roe):
        if roe > 0.15:
            score += 8
            notes.append("healthy ROE")
        elif roe < 0.08:
            score -= 6

    if not np.isnan(roce):
        if roce > 0.12:
            score += 8
            notes.append("healthy ROCE")
        elif roce < 0.08:
            score -= 5

    if not np.isnan(pm):
        if pm > 0.10:
            score += 6
            notes.append("healthy margins")
        elif pm < 0.03:
            score -= 6

    if not np.isnan(dte):
        if dte < 0.7:
            score += 8
            notes.append("manageable debt")
        elif dte > 1.5:
            score -= 10
            notes.append("high debt")

    if not np.isnan(curr):
        if curr >= 1.2:
            score += 4
        elif curr < 0.9:
            score -= 4

    if not np.isnan(quick) and quick >= 0.8:
        score += 2

    if not np.isnan(pe):
        if pe > 60:
            score -= 6
            notes.append("valuation rich")
        elif 0 < pe < 30:
            score += 3

    if not np.isnan(pb) and pb > 8:
        score -= 4

    score = max(0, min(100, score))
    block_long = False
    if (not np.isnan(earn_g) and earn_g < -0.20) and (not np.isnan(dte) and dte > 1.5):
        block_long = True
        notes.append("weak fundamentals")

    return {
        "fund_score": score,
        "fund_notes": ", ".join(notes[:5]) if notes else "mixed fundamentals",
        "block_long": block_long,
    }

def earnings_risk_adjustment(fund: dict) -> dict:
    penalty = 0
    note = "no near earnings risk"
    dt_val = fund.get("next_earnings_date")
    if dt_val is None or dt_val is pd.NaT:
        return {"earnings_penalty": penalty, "earnings_note": note}
    try:
        ed = pd.to_datetime(dt_val).to_pydatetime().replace(tzinfo=None)
        now = datetime.now(IST).replace(tzinfo=None)
        days = (ed.date() - now.date()).days
        if 0 <= days <= 3:
            penalty = 12
            note = f"earnings in {days} day(s)"
        elif 4 <= days <= 7:
            penalty = 6
            note = f"earnings in {days} day(s)"
        elif days < 0:
            note = "recent earnings passed"
        else:
            note = f"earnings later ({days} days)"
    except Exception:
        pass
    return {"earnings_penalty": penalty, "earnings_note": note}

def score_market_regime(nifty_df_raw: pd.DataFrame, news_bias: dict, overnight: dict, breadth: dict, vix_df: pd.DataFrame) -> dict:
    x = enrich_daily(nifty_df_raw)
    if x.empty:
        return {"label": "NEUTRAL", "score": 50, "reason": "Not enough data."}

    score = 50
    reasons = []
    last = x.iloc[-1]

    if last["Close"] > last["EMA20"]:
        score += 10
        reasons.append("NIFTY above EMA20")
    else:
        score -= 10

    if last["EMA20"] > last["EMA50"]:
        score += 12
        reasons.append("EMA20 above EMA50")
    else:
        score -= 12

    if safe_float(last["RSI14"]) > 55:
        score += 8
        reasons.append("RSI supportive")
    elif safe_float(last["RSI14"]) < 45:
        score -= 8

    if safe_float(last["MACD_HIST"]) > 0:
        score += 8
        reasons.append("MACD positive")
    else:
        score -= 8

    breadth_pct = safe_float(breadth.get("adv_pct"), 50)
    if breadth_pct >= 65:
        score += 12
        reasons.append("strong breadth")
    elif breadth_pct >= 55:
        score += 6
        reasons.append("healthy breadth")
    elif breadth_pct <= 40:
        score -= 10

    if not vix_df.empty:
        try:
            vix_last = safe_float(vix_df["Close"].iloc[-1])
            if vix_last > 22:
                score -= 8
                reasons.append("VIX elevated")
            elif vix_last < 14:
                score += 4
                reasons.append("VIX calm")
        except Exception:
            pass

    score += max(-10, min(10, overnight["score"] * 2))
    # reduced news impact so good technical day doesn't become neutral too easily
    score += max(-6, min(6, news_bias["score"]))

    score = max(0, min(100, score))

    if score >= 68:
        label = "BULLISH"
    elif score <= 36:
        label = "BEARISH"
    else:
        label = "NEUTRAL"

    return {
        "label": label,
        "score": round(score, 2),
        "reason": ", ".join(reasons[:6]) + f" | Overnight: {overnight['label']} | News: {news_bias['label']}",
    }

def trading_day_verdict(regime: dict, overnight: dict, news_bias: dict, vix_df: pd.DataFrame) -> dict:
    score = 0
    score += (regime["score"] - 50) / 4.5
    score += overnight["score"]
    score += max(-3, min(3, news_bias["score"] / 3))

    if not vix_df.empty:
        try:
            vix_last = safe_float(vix_df["Close"].iloc[-1])
            if vix_last > 22:
                score -= 3
            elif vix_last < 14:
                score += 1
        except Exception:
            pass

    if score >= 6:
        verdict = "TRADABLE"
        message = "Bias is supportive. Focus on strongest setups with discipline."
    elif score <= -6:
        verdict = "AVOID / VERY SELECTIVE"
        message = "Conditions are unstable or weak. Capital protection matters more than activity."
    else:
        verdict = "SELECTIVE"
        message = "Trade only top-quality setups. Avoid low-conviction entries."

    return {"verdict": verdict, "message": message, "score": round(score, 2)}

def previous_day_reasoning(nifty_df: pd.DataFrame, news_df: pd.DataFrame) -> str:
    if nifty_df.empty or len(nifty_df) < 2:
        return "Not enough market data to explain previous day move."
    last_close = safe_float(nifty_df["Close"].iloc[-1])
    prev_close = safe_float(nifty_df["Close"].iloc[-2])
    chg = pct_change(last_close, prev_close)
    if pd.isna(chg):
        return "Unable to compute previous day move."
    move_word = "rose" if chg > 0 else "fell"
    if not news_df.empty:
        drivers = news_df.sort_values("score", ascending=False if chg > 0 else True).head(3)["title"].tolist()
        driver_txt = " | ".join(drivers)
    else:
        driver_txt = "No major headlines fetched."
    return f"NIFTY {move_word} {abs(chg):.2f}% in the latest session. Likely drivers: {driver_txt}"

def compute_sector_strength(rank_df: pd.DataFrame) -> pd.DataFrame:
    if rank_df.empty or "sector" not in rank_df.columns:
        return pd.DataFrame()
    grp = rank_df.groupby("sector").agg(
        avg_score=("score", "mean"),
        avg_tech=("tech_score", "mean"),
        avg_fund=("fund_score", "mean"),
        buy_count=("signal", lambda s: (s == "BUY").sum()),
        watch_count=("signal", lambda s: (s == "WATCH").sum()),
        stocks=("symbol", "count")
    ).reset_index()
    grp["sector_score"] = (
        0.50 * grp["avg_score"] +
        0.20 * grp["avg_tech"] +
        0.15 * grp["avg_fund"] +
        0.15 * (grp["buy_count"] / grp["stocks"] * 100)
    ).round(2)
    return grp.sort_values(["sector_score", "buy_count"], ascending=[False, False]).reset_index(drop=True)

def stock_options_ideas(ideas: pd.DataFrame, regime: dict, vix_df: pd.DataFrame) -> pd.DataFrame:
    if ideas.empty:
        return pd.DataFrame()

    vix_ok = True
    if not vix_df.empty:
        try:
            vix_val = safe_float(vix_df["Close"].iloc[-1])
            if vix_val > 20:
                vix_ok = False
        except Exception:
            pass

    # relaxed so neutral days can still show ideas
    if regime["label"] not in ["BULLISH", "NEUTRAL"] or not vix_ok:
        return pd.DataFrame()

    rows = []
    for _, row in ideas.head(5).iterrows():
        ltp = safe_float(row["ltp"])
        rounded = int(round(ltp / 50.0) * 50) if ltp > 300 else int(round(ltp / 10.0) * 10)
        rows.append({
            "symbol": row["symbol"],
            "name": row["name"],
            "strategy": "Bullish call idea",
            "suggested_strike": f"{rounded} CE",
            "view": "Prefer next liquid expiry; enter only after spot confirms strength",
            "reason": f"Score {row['score']}, regime {regime['label']}, VIX acceptable",
        })
    return pd.DataFrame(rows)

def score_stock(symbol: str, info: dict, regime_label: str, news_df: pd.DataFrame, sector_rank: dict | None = None) -> dict:
    name = info["name"]
    sector = info["sector"]
    daily = enrich_daily(fetch_history(f"{symbol}.NS", period="6mo", interval="1d"))
    if daily.empty:
        return {"symbol": symbol, "name": name, "sector": sector, "signal": "NO DATA", "score": 0}

    d = daily.iloc[-1]
    close_ = safe_float(d["Close"])
    atr_ = safe_float(d["ATR14"])

    tech_score = 0
    tech_notes = []

    if close_ > safe_float(d["EMA20"]):
        tech_score += 15
        tech_notes.append("above EMA20")
    if close_ > safe_float(d["EMA50"]):
        tech_score += 15
        tech_notes.append("above EMA50")
    if safe_float(d["EMA20"]) > safe_float(d["EMA50"]):
        tech_score += 10
        tech_notes.append("EMA20 > EMA50")
    if close_ > safe_float(d["EMA200"]):
        tech_score += 6
        tech_notes.append("above EMA200")

    rsi_ = safe_float(d["RSI14"])
    if 55 <= rsi_ <= 70:
        tech_score += 15
        tech_notes.append("healthy RSI")
    elif rsi_ > 70:
        tech_score += 6
        tech_notes.append("strong momentum")
    elif rsi_ < 45:
        tech_score -= 10

    if safe_float(d["MACD_HIST"]) > 0:
        tech_score += 10
        tech_notes.append("MACD positive")

    vol_ratio = safe_float(d["VOL_RATIO"], 1.0)
    if vol_ratio >= 1.2:
        tech_score += 10
        tech_notes.append("volume expansion")

    hh20 = safe_float(d["HH20"])
    if close_ >= hh20 * 0.99:
        tech_score += 8
        tech_notes.append("near breakout")

    ret20 = safe_float(d["RET_20D"])
    if not np.isnan(ret20):
        if 0.03 <= ret20 <= 0.25:
            tech_score += 6
            tech_notes.append("healthy 1M trend")
        elif ret20 < -0.05:
            tech_score -= 6

    tech_score = max(0, min(100, tech_score))

    news_score = 50
    news_hits = 0
    for headline in news_df["title"].head(30).tolist():
        t = headline.lower()
        if symbol.lower() in t or name.lower() in t:
            news_hits += 1
        if sector.lower() in t:
            news_hits += 1
        if sector == "Banking" and "rbi" in t:
            news_hits += 1
        if sector == "IT" and ("ai" in t or "tech" in t or "nasdaq" in t):
            news_hits += 1
        if sector == "Defence" and ("defence" in t or "war" in t):
            news_hits += 1
        if sector == "Energy" and ("oil" in t or "energy" in t):
            news_hits += 1
        if sector == "Infrastructure" and ("capex" in t or "infrastructure" in t or "rail" in t or "power" in t):
            news_hits += 1

    news_score += min(news_hits * 2.5, 15)

    if regime_label == "BULLISH":
        news_score += 5
    elif regime_label == "NEUTRAL":
        news_score += 1
    elif regime_label == "BEARISH":
        news_score -= 6

    sector_boost = 0
    if sector_rank and sector in sector_rank:
        rank_score = sector_rank[sector]
        if rank_score >= 75:
            sector_boost = 8
        elif rank_score >= 65:
            sector_boost = 5
        elif rank_score < 45:
            sector_boost = -5

    news_score += sector_boost
    news_score = max(0, min(100, news_score))

    fund = fetch_fundamentals(symbol)
    fund_eval = score_fundamentals(fund)
    earn_eval = earnings_risk_adjustment(fund)

    final_score = (
        0.45 * tech_score +
        0.30 * news_score +
        0.25 * fund_eval["fund_score"]
    ) - earn_eval["earnings_penalty"]

    if fund_eval["block_long"]:
        final_score = min(final_score, 45)

    final_score = round(max(0, min(100, final_score)), 2)

    if fund_eval["block_long"]:
        signal = "AVOID"
    elif final_score >= 66:
        signal = "BUY"
    elif final_score >= 52:
        signal = "WATCH"
    elif final_score >= 45:
        signal = "EARLY"
    else:
        signal = "AVOID"

    if math.isnan(atr_) or atr_ <= 0:
        sl = round(close_ * 0.95, 2)
    else:
        sl = round(close_ - (1.5 * atr_), 2)

    risk = max(close_ - sl, 0.01)
    target1 = round(close_ + 2 * risk, 2)
    target2 = round(close_ + 3 * risk, 2)
    profit_pct_1 = round(((target1 / close_) - 1) * 100, 2)
    profit_pct_2 = round(((target2 / close_) - 1) * 100, 2)
    stoploss_pct = round((1 - (sl / close_)) * 100, 2)
    rr = round((target1 - close_) / max(close_ - sl, 0.01), 2)

    if earn_eval["earnings_penalty"] >= 12:
        horizon = "Avoid fresh swing before results"
    elif profit_pct_2 >= 10:
        horizon = "2 to 4 weeks"
    else:
        horizon = "1 to 2 weeks"

    notes = []
    notes.extend(tech_notes[:4])
    notes.append(fund_eval["fund_notes"])
    notes.append(earn_eval["earnings_note"])

    return {
        "symbol": symbol,
        "name": name,
        "sector": sector,
        "signal": signal,
        "score": final_score,
        "tech_score": round(tech_score, 2),
        "news_score": round(news_score, 2),
        "fund_score": round(fund_eval["fund_score"], 2),
        "ltp": round(close_, 2),
        "target1": target1,
        "target2": target2,
        "profit_pct_1": profit_pct_1,
        "profit_pct_2": profit_pct_2,
        "sl": sl,
        "stoploss_pct": stoploss_pct,
        "rr": rr,
        "horizon": horizon,
        "notes": " | ".join(notes[:6]),
    }
# =========================================================
# SIDEBAR
# =========================================================
with st.sidebar:
    st.markdown("## Control Center")
    max_ideas = st.slider("Top buy ideas", min_value=3, max_value=15, value=8, step=1)
    min_fund_score = st.slider("Minimum fundamental score", min_value=20, max_value=80, value=45, step=5)
    max_stoploss = st.slider("Max stoploss %", min_value=3, max_value=12, value=8, step=1)
    min_rr = st.slider("Minimum risk:reward", min_value=1.0, max_value=3.0, value=1.8, step=0.1)
    capital = st.number_input("Capital (₹)", min_value=10000, value=100000, step=10000)
    risk_pct = st.slider("Risk per trade (%)", min_value=0.25, max_value=2.00, value=1.00, step=0.25)

# =========================================================
# LOAD CORE DATA
# =========================================================
holiday_df = fetch_nse_trading_holidays()
holiday_set = set(holiday_df["date"].tolist()) if not holiday_df.empty else set()
now_ist = datetime.now(IST)
session_state = market_session_status(holiday_set)

with st.spinner("Pulling prices, holidays, breadth, global cues, and news intelligence..."):
    news_df = collect_news()
    news_bias = summarize_news_bias(news_df)
    overnight = overnight_score()
    official_breadth = fetch_nse_official_breadth()
    scan_breadth = compute_universe_breadth(SCAN_UNIVERSE)
    breadth = official_breadth if official_breadth else scan_breadth

    nifty_daily_raw = fetch_history("^NSEI", period="6mo", interval="1d")
    vix_daily = fetch_history("^INDIAVIX", period="6mo", interval="1d")

    regime = score_market_regime(nifty_daily_raw, news_bias, overnight, breadth, vix_daily)
    day_view = trading_day_verdict(regime, overnight, news_bias, vix_daily)
    prev_reason = previous_day_reasoning(fetch_history("^NSEI", period="10d", interval="1d"), news_df)
    gap_plan = build_gap_plan(regime, overnight, fetch_history("^NSEI", period="10d", interval="1d"))

# =========================================================
# HERO
# =========================================================
st.markdown(f"""
<div class="note-box">
    <b>Position sizing example for top idea ({best['name']}):</b><br>
    Capital: ₹{capital:,.0f}<br>
    Max risk: ₹{risk_amount:,.0f}<br>
    Risk per share: ₹{per_share_risk:,.2f}<br>
    Approx quantity: <b>{qty}</b>
</div>
""", unsafe_allow_html=True)

m1, m2, m3, m4, m5 = st.columns(5)
cards = [
    ("Date", now_ist.strftime("%d-%b-%Y"), now_ist.strftime("%A")),
    ("Time (IST)", now_ist.strftime("%H:%M:%S"), session_state),
    ("Market Bias", regime["label"], f"Confidence {regime['score']}/100"),
    ("Breadth", f"{safe_float(breadth.get('adv_pct'), 0):.1f}%", breadth.get("source", "")),
    ("Refresh", "15 min", "Auto rerun"),
]
for col, (lab, val, sub) in zip([m1, m2, m3, m4, m5], cards):
    with col:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{lab}</div>
            <div class="metric-value">{val}</div>
            <div class="metric-sub">{sub}</div>
        </div>
        """, unsafe_allow_html=True)

tabs = st.tabs(["Overview", "India & Global", "Top Ideas", "Sector Strength", "Options", "Full Scan", "News", "Holidays"])

with tabs[0]:
    left, right = st.columns([1.05, 1.95])
    with left:
        verdict_class = "good-box" if day_view["verdict"] == "TRADABLE" else "warn-box"
        st.markdown(f'<div class="{verdict_class}"><b>Today:</b> {day_view["verdict"]}<br>{day_view["message"]}</div>', unsafe_allow_html=True)
        st.markdown(f"""
        <div class="note-box">
            <b>Overnight Trend:</b> {overnight['label']}<br>
            <b>Drivers:</b> {" | ".join(overnight['reasons'][:5]) if overnight['reasons'] else "No major overnight cues"}<br><br>
            <b>News Bias:</b> {news_bias['label']} ({news_bias['score']})<br>
            <b>News Context:</b> {news_bias['reason']}
        </div>
        """, unsafe_allow_html=True)
    with right:
        st.markdown(f"""
        <div class="note-box">
            <b>Previous Day Reason:</b> {prev_reason}<br><br>
            <b>Pre-Open Gap Plan:</b> {gap_plan}<br><br>
            <b>Regime Note:</b> {regime['reason']}<br><br>
            <b>Breadth Source:</b> {breadth.get('source', 'N/A')}
        </div>
        """, unsafe_allow_html=True)

with tabs[1]:
    st.markdown('<div class="section-title">India Indices</div>', unsafe_allow_html=True)
    idx_cols = st.columns(4)
    for i, (name, ticker) in enumerate(INDEX_TICKERS.items()):
        df = enrich_daily(fetch_history(ticker, period="6mo", interval="1d"))
        with idx_cols[i % 4]:
            if df.empty:
                st.markdown(f'<div class="metric-card"><div class="metric-label">{name}</div><div class="metric-sub">Data unavailable</div></div>', unsafe_allow_html=True)
            else:
                last = df.iloc[-1]
                prev = safe_float(df["Close"].iloc[-2]) if len(df) > 1 else np.nan
                chg = pct_change(safe_float(last["Close"]), prev)
                color = "#16a34a" if chg >= 0 else "#dc2626"
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">{name}</div>
                    <div class="metric-value">{safe_float(last["Close"]):,.2f}</div>
                    <div class="metric-sub" style="color:{color};">Day change: {chg:.2f}%</div>
                    <div class="metric-sub">EMA20: {safe_float(last["EMA20"]):,.2f}</div>
                    <div class="metric-sub">RSI14: {safe_float(last["RSI14"]):.2f}</div>
                </div>
                """, unsafe_allow_html=True)

    st.markdown('<div class="section-title">Global Snapshot</div>', unsafe_allow_html=True)
    g_cols = st.columns(4)
    for i, (name, ticker) in enumerate(GLOBAL_TICKERS.items()):
        q = get_quote_snapshot(ticker)
        chg = safe_float(q.get("chg_pct"), np.nan)
        color = "#16a34a" if chg >= 0 else "#dc2626"
        with g_cols[i % 4]:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">{name}</div>
                <div class="metric-value">{safe_float(q.get('last'), 0):,.2f}</div>
                <div class="metric-sub" style="color:{color};">Day change: {chg:.2f}%</div>
            </div>
            """, unsafe_allow_html=True)

with st.spinner("Scanning universe..."):
    base_rows = []
    progress = st.progress(0, text="Initial scan...")
    universe_items = list(SCAN_UNIVERSE.items())
    for n, (sym, info) in enumerate(universe_items, start=1):
        base_rows.append(score_stock(sym, info, regime["label"], news_df, sector_rank=None))
        progress.progress(int((n / len(universe_items)) * 100), text=f"Scanning {info['name']}...")
    progress.empty()

base_rank_df = pd.DataFrame(base_rows).sort_values(
    by=["score", "profit_pct_2", "fund_score", "tech_score"],
    ascending=[False, False, False, False]
).reset_index(drop=True)

sector_df = compute_sector_strength(base_rank_df)
sector_rank_map = {row["sector"]: row["sector_score"] for _, row in sector_df.iterrows()}

with st.spinner("Refining ranks with sector-relative strength..."):
    final_rows = []
    progress2 = st.progress(0, text="Applying sector strength...")
    for n, (sym, info) in enumerate(universe_items, start=1):
        final_rows.append(score_stock(sym, info, regime["label"], news_df, sector_rank=sector_rank_map))
        progress2.progress(int((n / len(universe_items)) * 100), text=f"Refining {info['name']}...")
    progress2.empty()

rank_df = pd.DataFrame(final_rows).sort_values(
    by=["score", "profit_pct_2", "fund_score", "tech_score"],
    ascending=[False, False, False, False]
).reset_index(drop=True)

sector_df = compute_sector_strength(rank_df)

# primary filter
ideas = rank_df[
    (rank_df["signal"].isin(["BUY", "WATCH", "EARLY"])) &
    (rank_df["fund_score"] >= max(28, min_fund_score - 18)) &
    (rank_df["stoploss_pct"] <= max_stoploss + 3) &
    (rank_df["rr"] >= max(1.2, min_rr - 0.4))
].head(max_ideas)

if ideas.empty:
    ideas = rank_df[
        rank_df["signal"].isin(["BUY", "WATCH", "EARLY"])
    ].head(max_ideas)

if regime["label"] == "BEARISH":
    ideas = ideas.head(0)

with tabs[2]:
    st.markdown('<div class="section-title">Short-Term Swing Candidates</div>', unsafe_allow_html=True)
if ideas.empty:
    st.info("No eligible short-term idea found right now.")
else:
    best = ideas.iloc[0]
    risk_amount = capital * (risk_pct / 100)
    per_share_risk = max(best["ltp"] - best["sl"], 0.01)
    qty = int(risk_amount // per_share_risk)

    st.markdown(f"""
    <div class="note-box">
        <b>Position sizing example for top idea ({best['name']}):</b><br>
        Capital: ₹{capital:,.0f}<br>
        Max risk: ₹{risk_amount:,.0f}<br>
        Risk per share: ₹{per_share_risk:,.2f}<br>
        Approx quantity: <b>{qty}</b>
    </div>
    """, unsafe_allow_html=True)
st.dataframe(
            ideas[[
                "symbol", "name", "sector", "signal", "score",
                "tech_score", "news_score", "fund_score",
                "ltp", "target1", "target2",
                "profit_pct_1", "profit_pct_2",
                "sl", "stoploss_pct", "rr", "horizon", "notes"
            ]],
            use_container_width=True,
            hide_index=True,
        )
    if not ideas.empty:
    best = ideas.iloc[0]
    risk_amount = capital * (risk_pct / 100)
    per_share_risk = max(best["ltp"] - best["sl"], 0.01)
    qty = int(risk_amount // per_share_risk)

    st.markdown(f"""
    <div class="note-box">
        <b>Position sizing example for top idea ({best['name']}):</b><br>
        Capital: ₹{capital:,.0f}<br>
        Max risk: ₹{risk_amount:,.0f}<br>
        Risk per share: ₹{per_share_risk:,.2f}<br>
        Approx quantity: <b>{qty}</b>
    </div>
    """, unsafe_allow_html=True)
else:
    st.info("No eligible idea available right now for position sizing.")
high_conviction = rank_df[
    (rank_df["signal"] == "BUY") &
    (rank_df["fund_score"] >= 35) &
    (rank_df["rr"] >= 1.5)
].head(6)

watch_candidates = rank_df[
    (rank_df["signal"].isin(["BUY", "WATCH"])) &
    (rank_df["fund_score"] >= 30)
].head(8)

aggressive_swings = rank_df[
    (rank_df["signal"].isin(["BUY", "WATCH", "EARLY"])) &
    (rank_df["fund_score"] >= 25)
].head(10)

with tabs[3]:
    st.markdown('<div class="section-title">Sector Relative Strength</div>', unsafe_allow_html=True)
    if sector_df.empty:
        st.caption("Sector table unavailable.")
    else:
        st.dataframe(
            sector_df[["sector", "sector_score", "avg_score", "avg_tech", "avg_fund", "buy_count", "watch_count", "stocks"]],
            use_container_width=True,
            hide_index=True,
        )

with tabs[4]:
    st.markdown('<div class="section-title">Options Ideas</div>', unsafe_allow_html=True)
    options_df = stock_options_ideas(ideas, regime, vix_daily)
    if options_df.empty:
        st.info("Options ideas are hidden because volatility is elevated or overall conditions are not supportive enough.")
    else:
        st.dataframe(options_df, use_container_width=True, hide_index=True)
        st.markdown("""
        <div class="note-box">
            <b>Options rule:</b> use only when the day is tradable enough and volatility is acceptable.
            Enter only after spot confirms strength. Avoid blindly buying calls at the open.
        </div>
        """, unsafe_allow_html=True)

with tabs[5]:
    st.markdown('<div class="section-title">Full Ranked Scan</div>', unsafe_allow_html=True)
    st.dataframe(rank_df, use_container_width=True, hide_index=True)

    watch_df = rank_df[
        (rank_df["signal"] == "WATCH") &
        (rank_df["fund_score"] >= 30)
    ].head(12)

    st.markdown('<div class="section-title">Watchlist Names</div>', unsafe_allow_html=True)
    if watch_df.empty:
        st.caption("No notable watchlist names at the moment.")
    else:
        st.dataframe(
            watch_df[[
                "symbol", "name", "sector", "score", "tech_score",
                "news_score", "fund_score", "ltp", "sl", "horizon", "notes"
            ]],
            use_container_width=True,
            hide_index=True,
        )
with tabs[6]:
    st.markdown('<div class="section-title">Market Headlines</div>', unsafe_allow_html=True)
    n1, n2 = st.columns(2)
    with n1:
        st.markdown("**Supportive headlines**")
        for h in news_bias["top_positive"][:6]:
            st.markdown(f"- {h}")
    with n2:
        st.markdown("**Risk headlines**")
        for h in news_bias["top_negative"][:6]:
            st.markdown(f"- {h}")

    show_news = news_df.head(30).copy()
    if not show_news.empty:
        st.dataframe(
            show_news[["bucket", "source", "title", "score", "sector_tags", "link"]],
            use_container_width=True,
            hide_index=True,
        )

with tabs[7]:
    st.markdown('<div class="section-title">NSE Trading Holidays</div>', unsafe_allow_html=True)
    if holiday_df.empty:
        st.caption("Could not load official NSE holiday table right now.")
    else:
        st.dataframe(holiday_df, use_container_width=True, hide_index=True)

st.warning("This is a probability-based research tool, not certainty. Use strict risk control, position sizing, and judgment before taking trades.")
