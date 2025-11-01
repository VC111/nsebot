import streamlit as st
import pandas as pd
import threading
import time
import requests
from datetime import datetime
from pathlib import Path
from nselib.derivatives import nse_live_option_chain

# ---------------- CONFIG ----------------
SYMBOL = "NIFTY"
POLL_INTERVAL_SECONDS = 15 * 60
OI_THRESHOLD = 50_0000
STRIKE_OFFSET = 200
STRIKE_RANGE = 250
SIGNALS_CSV = Path("signals_log.csv")
TRADES_CSV = Path("trades_log.csv")
SNAPSHOT_CSV = Path("latest_snapshot.csv")

# ---------------- GLOBALS ----------------
if SNAPSHOT_CSV.exists():
    latest_snapshot = pd.read_csv(SNAPSHOT_CSV)
else:
    latest_snapshot = pd.DataFrame()

signals_df = pd.read_csv(SIGNALS_CSV) if SIGNALS_CSV.exists() else pd.DataFrame(
    columns=["Timestamp", "Signal", "Strike", "Reason"]
)
open_trades_df = pd.read_csv(TRADES_CSV) if TRADES_CSV.exists() else pd.DataFrame(
    columns=["Timestamp", "Type", "Strike", "EntryPrice", "CurrentPrice", "P/L%"]
)
last_poll_time = None
expiry_list = []
latest_spot = 0.0


# ---------------- HELPER FUNCTIONS ----------------
def get_nifty_spot():
    """Fetch NIFTY spot price from NSE."""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        data = requests.get(url, headers=headers, timeout=10).json()
        return float(data["records"]["underlyingValue"])
    except Exception as e:
        print("Spot fetch error:", e)
        return 0.0


def process_option_chain(df):
    """Normalize option chain dataframe."""
    for c in [
        "CALLS_OI", "CALLS_Chng_in_OI", "CALLS_LTP", "CALLS_Net_Chng",
        "PUTS_OI", "PUTS_Chng_in_OI", "PUTS_LTP", "PUTS_Net_Chng", "Strike_Price"
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["Expiry_Date"] = pd.to_datetime(df["Expiry_Date"], errors="coerce")
    df = df.dropna(subset=["Expiry_Date"])
    return df


def filter_atm_range(df, spot):
    """Filter only ATM ±250 strikes."""
    if spot <= 0 or df.empty:
        return df
    lower = (spot // 50) * 50 - STRIKE_RANGE
    upper = (spot // 50) * 50 + STRIKE_RANGE
    return df[(df["Strike_Price"] >= lower) & (df["Strike_Price"] <= upper)]


def get_nearest_expiries(df):
    """Return nearest weekly and monthly expiries (even across months)."""
    if df.empty:
        return []
    all_exp = sorted(pd.to_datetime(df["Expiry_Date"].unique()))
    if len(all_exp) == 1:
        return [all_exp[0]]

    # Weekly = earliest upcoming expiry
    weekly = all_exp[0]

    # Monthly = last Thursday across available months
    month_groups = {}
    for d in all_exp:
        m = (d.year, d.month)
        if m not in month_groups:
            month_groups[m] = []
        month_groups[m].append(d)
    monthly = max([max(v) for v in month_groups.values()])

    return sorted(list(set([weekly, monthly])))


def prepare_display(df):
    """Arrange columns as per desired call-strike-put layout."""
    df = df.rename(columns={
        "Strike_Price": "Strike",
        "CALLS_OI": "CE OI",
        "CALLS_Chng_in_OI": "CE ΔOI",
        "CALLS_LTP": "CE LTP",
        "CALLS_Net_Chng": "CE ΔLTP",
        "PUTS_OI": "PE OI",
        "PUTS_Chng_in_OI": "PE ΔOI",
        "PUTS_LTP": "PE LTP",
        "PUTS_Net_Chng": "PE ΔLTP",
    })[
        [
            "Expiry_Date",
            "CE ΔLTP", "CE LTP", "CE ΔOI", "CE OI",
            "Strike",
            "PE OI", "PE ΔOI", "PE LTP", "PE ΔLTP"
        ]
    ].sort_values(["Expiry_Date", "Strike"])
    return df


def detect_signals(df):
    """Detect OI drop signals."""
    global signals_df
    call_drop = df[df["CE ΔOI"] <= -OI_THRESHOLD]
    put_drop = df[df["PE ΔOI"] <= -OI_THRESHOLD]

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not call_drop.empty:
        strike = call_drop.iloc[0]["Strike"] + STRIKE_OFFSET
        signals_df.loc[len(signals_df)] = [
            now, f"BUY CE {strike}", strike, f"CALL OI ↓ {OI_THRESHOLD}"
        ]
        print(f"📈 Signal: BUY CE {strike}")

    if not put_drop.empty:
        strike = put_drop.iloc[0]["Strike"] + STRIKE_OFFSET
        signals_df.loc[len(signals_df)] = [
            now, f"BUY PE {strike}", strike, f"PUT OI ↓ {OI_THRESHOLD}"
        ]
        print(f"📉 Signal: BUY PE {strike}")

    signals_df.to_csv(SIGNALS_CSV, index=False)


def poll_once():
    """Fetch live data once."""
    global latest_snapshot, expiry_list, latest_spot, last_poll_time
    df = nse_live_option_chain(SYMBOL)
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise Exception("Empty option chain")

    df = process_option_chain(df)
    latest_spot = get_nifty_spot()
    df = filter_atm_range(df, latest_spot)
    expiry_list = get_nearest_expiries(df)
    df = df[df["Expiry_Date"].isin(expiry_list)]
    df = prepare_display(df)
    latest_snapshot = df.copy()
    detect_signals(df)
    last_poll_time = datetime.now()
    df.to_csv(SNAPSHOT_CSV, index=False)
    print(f"[{last_poll_time}] ✅ Poll complete ({len(df)} rows, spot={latest_spot})")
    return df


def worker_background():
    while True:
        try:
            poll_once()
        except Exception as e:
            print("Worker error:", e)
        time.sleep(POLL_INTERVAL_SECONDS)


# ---------------- STREAMLIT UI ----------------
st.set_page_config(page_title="Nifty Options Tool", layout="wide")

# --- Compact styling ---
st.markdown("""
    <style>
    div[data-testid="stDataFrame"] td, div[data-testid="stDataFrame"] th {
        padding: 2px 6px !important;
        font-size: 13px !important;
        line-height: 1.1em !important;
    }
    div[data-testid="stDataFrame"] th {
        font-weight: 600 !important;
        background-color: #f5f5f5 !important;
    }
    </style>
""", unsafe_allow_html=True)

st.title("💹 NIFTY Option Chain (ATM ±250 | Weekly + Monthly | Compact View)")

col1, col2, col3 = st.columns(3)
col1.metric("Symbol", SYMBOL)
col2.metric("Poll Interval", "15 min")
col3.metric("OI Δ Threshold", f"{OI_THRESHOLD:,}")

if last_poll_time:
    st.caption(f"Last update: {last_poll_time.strftime('%Y-%m-%d %H:%M:%S')}")
else:
    st.caption("Waiting for background data...")

tab1, tab2, tab3 = st.tabs(["📊 Option Chain", "⚡ Signals", "📈 Open Trades / P&L"])

# ----- TAB 1 -----
with tab1:
    if SNAPSHOT_CSV.exists():
        latest_snapshot = pd.read_csv(SNAPSHOT_CSV)

    if latest_snapshot.empty:
        st.info("No data yet — wait or click Manual Poll.")
    else:
        expiry_list = sorted(latest_snapshot["Expiry_Date"].unique())
        expiry_choice = st.selectbox("Select Expiry", expiry_list, format_func=lambda x: x)
        df_display = latest_snapshot.copy()
        if expiry_choice is not None:
            df_display = df_display[df_display["Expiry_Date"] == expiry_choice]
        st.dataframe(df_display.drop(columns=["Expiry_Date"]), width="stretch")

    if st.button("🔄 Manual Poll"):
        try:
            df = poll_once()
            st.success(f"Manual poll complete — {len(df)} rows.")
        except Exception as e:
            st.error(f"Manual poll failed: {e}")

# ----- TAB 2 -----
with tab2:
    st.subheader("Generated Signals")
    if SIGNALS_CSV.exists():
        signals_df = pd.read_csv(SIGNALS_CSV)
    if signals_df.empty:
        st.info("No signals yet.")
    else:
        st.dataframe(signals_df, width="stretch")

# ----- TAB 3 -----
with tab3:
    st.subheader("Open Trades / P&L")
    if TRADES_CSV.exists():
        open_trades_df = pd.read_csv(TRADES_CSV)
    if open_trades_df.empty:
        st.info("No open trades yet.")
    else:
        st.dataframe(open_trades_df, width="stretch")

# ----- BACKGROUND WORKER -----
if "worker_started" not in st.session_state:
    st.session_state.worker_started = True
    threading.Thread(target=worker_background, daemon=True).start()
    st.toast("✅ Background worker started", icon="🔄")

st.caption("© 2025 Vinay Kumar | NIFTY OI Tool v3.0 (Compact + Monthly Fix)")

