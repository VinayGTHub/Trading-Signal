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

BATCH_TIMEOUT = 1.2     # wait after last signal (burst end)
MAX_BATCH_SIZE = 50     # safety cap

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

    return None, None

# ─── DHAN BULK API ────────────────────────────────────
def fetch_bulk_market_data(exchange_groups):
    logging.info(f"Requesting Dhan: {msg}")
    url = "https://api.dhan.co/v2/marketfeed/quote"

    headers = {
        "access-token": DHAN_ACCESS_TOKEN,
        "client-id": DHAN_CLIENT_ID,
        "Content-Type": "application/json"
    }

    payload = {exch: [int(sid) for sid in sids] for exch, sids in exchange_groups.items()}

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=5)
        if r.status_code != 200:
            logging.error(f"Dhan error: {r.text}")
            return {}
        
        data = r.json().get("data", {})
        logging.info("Market data: Success")
        result = {}

        for exch, instruments in data.items():
            for sid, info in instruments.items():
                result[str(sid)] = info

        return result

    except Exception as e:
        logging.error(f"Dhan API error: {e}")
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
    raw_upper = raw.upper()

    direction = "BUY" if "BUY" in raw_upper else "SELL" if "SELL" in raw_upper else None

    symbol = None
    price = None

    if "AT" in raw_upper:
        parts = raw_upper.split("AT")
        symbol = parts[0].split()[-1].strip()
        price  = parts[1].strip()

    return {
        "direction": direction,
        "symbol": symbol,
        "price": price
    }

# ─── PROCESS BATCH ────────────────────────────────────
def process_batch(batch):
    logging.info(f"Processing batch of {len(batch)} signals")
    logging.info("Entered process_batch")
    exchange_groups = {}
    enriched = []

    for data, raw in batch:
        symbol = data.get("symbol")
        direction = data.get("direction")

        if not symbol or not direction:
            continue

        exch, sid = lookup_security_id(symbol)
        if not sid:
            continue

        enriched.append((data, exch, sid))

        if exch not in exchange_groups:
            exchange_groups[exch] = []

        if sid not in exchange_groups[exch]:
            exchange_groups[exch].append(sid)

    if not exchange_groups:
        return

    market_data = fetch_bulk_market_data(exchange_groups)

    for data, exch, sid in enriched:
        instrument = market_data.get(str(sid))
        if not instrument:
            continue

        ltp = instrument.get("last_price", 0)
        buy_qty = instrument.get("buy_quantity", 0)
        sell_qty = instrument.get("sell_quantity", 0)

        total = buy_qty + sell_qty

        # Order flow filter
        if total > 0:
            buyer_strength = buy_qty / total
            seller_strength = sell_qty / total

            if data["direction"] == "BUY" and buyer_strength < seller_strength:
                continue

            if data["direction"] == "SELL" and seller_strength < buyer_strength:
                continue

        msg = f"{data['direction']} {data['symbol']} @ {ltp}"
        send_telegram(msg)

# ─── BACKGROUND FLUSHER (KEY PART) ────────────────────
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
                logging.info(f"Processing batch size: {len(batch_copy)}")
                process_batch(batch_copy)

        except Exception as e:
            logging.error(f"Batch flusher error: {e}")


@app.route("/")
def home():
    return "Server Running", 200

# ─── WEBHOOK ──────────────────────────────────────────
@app.route('/webhook', methods=['POST'])
def webhook():
    global LAST_SIGNAL_TIME

    try:
        raw = request.get_data().decode('utf-8')

        logging.info(f"Received webhook: {raw}")

        data = parse_signal(raw)

        with LOCK:
            BATCH.append((data, raw))
            LAST_SIGNAL_TIME = time.time()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(e)
        return jsonify({"error": str(e)}), 500

# ─── START ────────────────────────────────────────────
flusher_started = False

@app.before_request
def start_flusher():
    global flusher_started

    if not flusher_started:
        thread = threading.Thread(target=batch_flusher)
        thread.daemon = True
        thread.start()

        flusher_started = True

        logging.info("Batch flusher thread started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
