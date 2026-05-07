from flask import Flask, request, jsonify
import requests
import logging
import os
import csv
import time
import threading
from dotenv import load_dotenv

# ─── INIT ─────────────────────────────────────────────
load_dotenv()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# ─── CONFIG ───────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

CSV_URL  = "https://images.dhan.co/api-data/api-scrip-master.csv"
CSV_FILE = "scrip_master.csv"

# ─── BATCH CONFIG ─────────────────────────────────────
BATCH = []
LAST_SIGNAL_TIME = 0
LOCK = threading.Lock()

BATCH_TIMEOUT  = 1.2
MAX_BATCH_SIZE = 50

# ─── SYMBOL CACHE ─────────────────────────────────────
SYMBOL_CACHE = {}

# ─── CSV DOWNLOAD ─────────────────────────────────────
def ensure_csv():
    if os.path.exists(CSV_FILE):
        return
    logging.info("Downloading scrip master...")
    r = requests.get(CSV_URL, timeout=10)
    with open(CSV_FILE, "w") as f:
        f.write(r.text)

# ─── SYMBOL LOOKUP ────────────────────────────────────
def lookup_security_id(symbol):
    symbol = symbol.upper()

    if symbol in SYMBOL_CACHE:
        return SYMBOL_CACHE[symbol]

    ensure_csv()

    with open(CSV_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["SEM_TRADING_SYMBOL"].upper() == symbol:
                exch = row["SEM_EXM_EXCH_ID"]
                seg  = row["SEM_SEGMENT"]
                sid  = row["SEM_SMST_SECURITY_ID"]

                if exch == "NSE" and seg == "E":
                    exch_key = "NSE_EQ"
                elif exch == "BSE" and seg == "E":
                    exch_key = "BSE_EQ"
                elif seg == "I":
                    exch_key = "IDX_I"
                else:
                    exch_key = "NSE_EQ"

                SYMBOL_CACHE[symbol] = (exch_key, sid)
                return exch_key, sid

    logging.warning(f"Symbol not found in scrip master: {symbol}")  # ✅ Added warning
    return None, None

# ─── DHAN BULK API ────────────────────────────────────
def fetch_bulk_market_data(exchange_groups):
    logging.info(f"Requesting Dhan with groups: {exchange_groups}")  # ✅ Fixed: was logging undefined 'msg'

    url = "https://api.dhan.co/v2/marketfeed/quote"

    headers = {
        "access-token": DHAN_ACCESS_TOKEN,
        "client-id": DHAN_CLIENT_ID,
        "Content-Type": "application/json"
    }

    payload = {exch: [int(sid) for sid in sids] for exch, sids in exchange_groups.items()}
    logging.info(f"Dhan payload: {payload}")  # ✅ Log payload for debugging

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=5)
        logging.info(f"Dhan response status: {r.status_code}")  # ✅ Always log status
        logging.info(f"Dhan response body: {r.text}")            # ✅ Always log body

        if r.status_code != 200:
            logging.error(f"Dhan error: {r.text}")
            return {}

        data = r.json().get("data", {})
        logging.info("Market data fetch: Success")
        result = {}

        for exch, instruments in data.items():
            for sid, info in instruments.items():
                result[str(sid)] = info

        return result

    except Exception as e:
        logging.error(f"Dhan API exception: {e}")
        return {}

# ─── TELEGRAM ─────────────────────────────────────────
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    logging.info(f"Sending Telegram: {msg}")
    try:
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=5)
    except Exception as e:
        logging.error(f"Telegram error: {e}")

# ─── PARSE SIGNAL ─────────────────────────────────────
def parse_signal(raw):
    """
    Expected format from TradingView:
        BUY RELIANCE AT 1200
        SELL INFY AT 1500.50
    """
    raw_upper = raw.strip().upper()
    logging.info(f"Parsing signal: {raw_upper}")

    direction = None
    symbol    = None
    price     = None

    try:
        parts = raw_upper.split()
        # ✅ Fixed parsing: expects "BUY SYMBOL AT PRICE" or "SELL SYMBOL AT PRICE"
        if len(parts) >= 4 and parts[0] in ("BUY", "SELL") and parts[2] == "AT":
            direction = parts[0]
            symbol    = parts[1]
            price     = parts[3]
        else:
            # Fallback: try to extract direction and symbol loosely
            direction = "BUY" if "BUY" in raw_upper else "SELL" if "SELL" in raw_upper else None
            if "AT" in raw_upper:
                at_parts = raw_upper.split("AT")
                symbol = at_parts[0].split()[-1].strip()
                price  = at_parts[1].strip().split()[0]

    except Exception as e:
        logging.error(f"Signal parse error: {e}")

    logging.info(f"Parsed → direction={direction}, symbol={symbol}, price={price}")

    if not direction or not symbol:
        logging.warning(f"Could not parse signal: {raw}")

    return {"direction": direction, "symbol": symbol, "price": price}

# ─── PROCESS BATCH ────────────────────────────────────
def process_batch(batch):
    logging.info(f"Processing batch of {len(batch)} signals")

    exchange_groups = {}
    enriched = []

    for data, raw in batch:
        symbol    = data.get("symbol")
        direction = data.get("direction")

        if not symbol or not direction:
            logging.warning(f"Skipping invalid signal: {raw}")
            continue

        exch, sid = lookup_security_id(symbol)
        if not sid:
            logging.warning(f"No security ID for symbol: {symbol}")
            continue

        enriched.append((data, exch, sid))

        if exch not in exchange_groups:
            exchange_groups[exch] = []
        if sid not in exchange_groups[exch]:
            exchange_groups[exch].append(sid)

    if not exchange_groups:
        logging.warning("No valid symbols to fetch from Dhan")
        return

    logging.info(f"Fetching market data for: {exchange_groups}")
    market_data = fetch_bulk_market_data(exchange_groups)

    if not market_data:
        logging.warning("Empty market data returned from Dhan")
        return

    for data, exch, sid in enriched:
        instrument = market_data.get(str(sid))
        if not instrument:
            logging.warning(f"No market data for SID {sid}")
            continue

        ltp      = instrument.get("last_price", 0)
        buy_qty  = instrument.get("buy_quantity", 0)
        sell_qty = instrument.get("sell_quantity", 0)
        total    = buy_qty + sell_qty

        if total > 0:
            buyer_strength  = buy_qty / total
            seller_strength = sell_qty / total

            if data["direction"] == "BUY" and buyer_strength < seller_strength:
                logging.info(f"BUY filtered out — weak buyer strength for {data['symbol']}")
                continue
            if data["direction"] == "SELL" and seller_strength < buyer_strength:
                logging.info(f"SELL filtered out — weak seller strength for {data['symbol']}")
                continue

        msg = f"{data['direction']} {data['symbol']} @ {ltp}"
        send_telegram(msg)

# ─── BACKGROUND FLUSHER ───────────────────────────────
def batch_flusher():
    global BATCH, LAST_SIGNAL_TIME
    logging.info("Batch flusher started")

    while True:
        time.sleep(0.2)
        batch_copy = []

        try:
            with LOCK:
                if not BATCH:
                    continue
                now = time.time()
                if (
                    now - LAST_SIGNAL_TIME > BATCH_TIMEOUT
                    or len(BATCH) >= MAX_BATCH_SIZE
                ):
                    batch_copy = BATCH.copy()
                    BATCH = []

            if batch_copy:
                logging.info(f"Flushing batch of size: {len(batch_copy)}")
                process_batch(batch_copy)

        except Exception as e:
            logging.error(f"Batch flusher error: {e}")

# ─── START FLUSHER ONCE ───────────────────────────────
# ✅ Fixed: use app startup event instead of before_request
#    This avoids duplicate threads and works properly with Gunicorn
flusher_started = False

def ensure_flusher():
    global flusher_started
    if not flusher_started:
        t = threading.Thread(target=batch_flusher, daemon=True)
        t.start()
        flusher_started = True
        logging.info("Batch flusher thread started")

@app.route("/")
def home():
    return "Server Running", 200

# ─── WEBHOOK ──────────────────────────────────────────
@app.route('/webhook', methods=['POST'])
def webhook():
    global LAST_SIGNAL_TIME

    try:
        raw = request.get_data().decode('utf-8')
        logging.info(f"Webhook received: {raw}")

        data = parse_signal(raw)

        with LOCK:
            BATCH.append((data, raw))
            LAST_SIGNAL_TIME = time.time()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(f"Webhook error: {e}")
        return jsonify({"error": str(e)}), 500

# ─── ENTRYPOINT ───────────────────────────────────────
ensure_flusher()  # ✅ Start flusher at module load (works with both gunicorn & direct run)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
