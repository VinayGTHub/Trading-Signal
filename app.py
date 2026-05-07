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
logging.basicConfig(level=logging.DEBUG)  # ✅ DEBUG level to catch everything

# ─── CONFIG ───────────────────────────────────────────
BOT_TOKEN         = os.getenv("BOT_TOKEN")
CHAT_ID           = os.getenv("CHAT_ID")
DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

CSV_URL  = "https://images.dhan.co/api-data/api-scrip-master.csv"
CSV_FILE = "scrip_master.csv"

# ─── BATCH CONFIG ─────────────────────────────────────
BATCH            = []
LAST_SIGNAL_TIME = 0
LOCK             = threading.Lock()
BATCH_TIMEOUT    = 1.2
MAX_BATCH_SIZE   = 50

SYMBOL_CACHE = {}

# ─── CSV ──────────────────────────────────────────────
def ensure_csv():
    if os.path.exists(CSV_FILE):
        logging.info(f"CSV already exists: {CSV_FILE}")
        return
    logging.info("Downloading scrip master CSV...")
    r = requests.get(CSV_URL, timeout=10)
    with open(CSV_FILE, "w") as f:
        f.write(r.text)
    logging.info("CSV downloaded successfully")

# ─── SYMBOL LOOKUP ────────────────────────────────────
def lookup_security_id(symbol):
    symbol = symbol.upper()
    logging.info(f"Looking up symbol: {symbol}")

    if symbol in SYMBOL_CACHE:
        logging.info(f"Cache hit for {symbol}: {SYMBOL_CACHE[symbol]}")
        return SYMBOL_CACHE[symbol]

    ensure_csv()

    found = False
    with open(CSV_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["SEM_TRADING_SYMBOL"].upper() == symbol:
                found = True
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
                logging.info(f"Symbol found: {symbol} → exch={exch_key}, sid={sid}")
                return exch_key, sid

    if not found:
        logging.error(f"❌ Symbol NOT found in CSV: {symbol}")
    return None, None

# ─── DHAN API ─────────────────────────────────────────
def fetch_bulk_market_data(exchange_groups):
    logging.info(f"Calling Dhan API with: {exchange_groups}")
    url = "https://api.dhan.co/v2/marketfeed/quote"
    headers = {
        "access-token": DHAN_ACCESS_TOKEN,
        "client-id": DHAN_CLIENT_ID,
        "Content-Type": "application/json"
    }
    payload = {exch: [int(sid) for sid in sids] for exch, sids in exchange_groups.items()}
    logging.info(f"Dhan payload: {payload}")
    logging.info(f"Dhan headers (token truncated): client-id={DHAN_CLIENT_ID}, token={str(DHAN_ACCESS_TOKEN)[:10]}...")

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=5)
        logging.info(f"Dhan status: {r.status_code}")
        logging.info(f"Dhan response: {r.text}")

        if r.status_code != 200:
            logging.error(f"❌ Dhan error {r.status_code}: {r.text}")
            return {}

        data = r.json().get("data", {})
        result = {}
        for exch, instruments in data.items():
            for sid, info in instruments.items():
                result[str(sid)] = info

        logging.info(f"Dhan parsed result keys: {list(result.keys())}")
        return result

    except Exception as e:
        logging.error(f"❌ Dhan exception: {e}", exc_info=True)
        return {}

# ─── TELEGRAM ─────────────────────────────────────────
def send_telegram(msg):
    logging.info(f"Sending Telegram message: {msg}")
    logging.info(f"BOT_TOKEN set: {bool(BOT_TOKEN)}, CHAT_ID set: {bool(CHAT_ID)}")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=5)
        logging.info(f"Telegram response: {r.status_code} {r.text}")
    except Exception as e:
        logging.error(f"❌ Telegram exception: {e}", exc_info=True)

# ─── PARSE SIGNAL ─────────────────────────────────────
def parse_signal(raw):
    raw_upper = raw.strip().upper()
    logging.info(f"Parsing signal: {raw_upper}")

    direction = None
    symbol    = None
    price     = None

    try:
        parts = raw_upper.split()
        logging.debug(f"Split parts: {parts}")

        if len(parts) >= 4 and parts[0] in ("BUY", "SELL") and parts[2] == "AT":
            direction = parts[0]
            symbol    = parts[1]
            price     = parts[3]
        else:
            direction = "BUY" if "BUY" in raw_upper else "SELL" if "SELL" in raw_upper else None
            if "AT" in raw_upper:
                at_parts  = raw_upper.split("AT")
                symbol    = at_parts[0].split()[-1].strip()
                price     = at_parts[1].strip().split()[0]

    except Exception as e:
        logging.error(f"❌ Parse error: {e}", exc_info=True)

    logging.info(f"Parsed → direction={direction}, symbol={symbol}, price={price}")
    return {"direction": direction, "symbol": symbol, "price": price}

# ─── PROCESS BATCH ────────────────────────────────────
def process_batch(batch):
    logging.info(f"========== PROCESS BATCH CALLED — {len(batch)} signals ==========")

    exchange_groups = {}
    enriched        = []

    for data, raw in batch:
        symbol    = data.get("symbol")
        direction = data.get("direction")
        logging.info(f"Processing signal: direction={direction}, symbol={symbol}")

        if not symbol or not direction:
            logging.warning(f"⚠️ Skipping — missing symbol or direction: {raw}")
            continue

        exch, sid = lookup_security_id(symbol)
        logging.info(f"Lookup result: exch={exch}, sid={sid}")

        if not sid:
            logging.error(f"❌ No SID for symbol {symbol} — skipping")
            # ✅ Still send Telegram even without market data
            send_telegram(f"⚠️ {direction} {symbol} @ {data.get('price','N/A')} [Symbol not in CSV]")
            continue

        enriched.append((data, exch, sid))
        if exch not in exchange_groups:
            exchange_groups[exch] = []
        if sid not in exchange_groups[exch]:
            exchange_groups[exch].append(sid)

    logging.info(f"Exchange groups built: {exchange_groups}")
    logging.info(f"Enriched signals: {len(enriched)}")

    if not exchange_groups:
        logging.warning("⚠️ No exchange groups — nothing to fetch")
        return

    market_data = fetch_bulk_market_data(exchange_groups)
    logging.info(f"Market data received: {market_data}")

    for data, exch, sid in enriched:
        instrument = market_data.get(str(sid))
        logging.info(f"Instrument data for SID {sid}: {instrument}")

        if not instrument:
            logging.warning(f"⚠️ No instrument data for {data['symbol']} — sending anyway")
            msg = f"{data['direction']} {data['symbol']} @ {data.get('price','N/A')} [Market Closed]"
            send_telegram(msg)
            continue

        ltp = instrument.get("last_price", 0)
        msg = f"{data['direction']} {data['symbol']} @ {ltp}"
        logging.info(f"Sending final Telegram: {msg}")
        send_telegram(msg)

# ─── BATCH FLUSHER ────────────────────────────────────
def batch_flusher():
    global BATCH, LAST_SIGNAL_TIME
    logging.info("========== BATCH FLUSHER THREAD RUNNING ==========")

    while True:
        time.sleep(0.2)

        try:
            with LOCK:
                batch_size = len(BATCH)

            # ✅ Log every 25 iterations (~5 sec) so we know thread is alive
            if not hasattr(batch_flusher, '_tick'):
                batch_flusher._tick = 0
            batch_flusher._tick += 1
            if batch_flusher._tick % 25 == 0:
                logging.debug(f"Flusher alive — BATCH size: {batch_size}")

            batch_copy = []
            with LOCK:
                if not BATCH:
                    continue

                now = time.time()
                elapsed = now - LAST_SIGNAL_TIME
                logging.info(f"Flusher check — BATCH={len(BATCH)}, elapsed={elapsed:.2f}s")

                if elapsed > BATCH_TIMEOUT or len(BATCH) >= MAX_BATCH_SIZE:
                    batch_copy   = BATCH.copy()
                    BATCH        = []
                    logging.info(f"✅ Flushing {len(batch_copy)} signals")

            if batch_copy:
                process_batch(batch_copy)

        except Exception as e:
            logging.error(f"❌ Flusher error: {e}", exc_info=True)

# ─── GUNICORN HOOK (start flusher safely) ─────────────
flusher_started = False
flusher_lock    = threading.Lock()

def ensure_flusher():
    global flusher_started
    with flusher_lock:
        if not flusher_started:
            t = threading.Thread(target=batch_flusher, daemon=True)
            t.start()
            flusher_started = True
            logging.info("✅ Batch flusher thread started")
        else:
            logging.info("Flusher already running — skipping")

# ─── ROUTES ───────────────────────────────────────────
@app.route("/")
def home():
    return "Server Running", 200

@app.route("/health")
def health():
    """Quick debug endpoint — hit this to check env vars"""
    return jsonify({
        "bot_token_set":         bool(BOT_TOKEN),
        "chat_id_set":           bool(CHAT_ID),
        "dhan_client_id_set":    bool(DHAN_CLIENT_ID),
        "dhan_access_token_set": bool(DHAN_ACCESS_TOKEN),
        "batch_size":            len(BATCH),
        "flusher_started":       flusher_started,
        "symbol_cache":          list(SYMBOL_CACHE.keys()),
    }), 200

@app.route('/webhook', methods=['POST'])
def webhook():
    global LAST_SIGNAL_TIME
    try:
        raw = request.get_data().decode('utf-8')
        logging.info(f"Webhook received: {raw}")

        data = parse_signal(raw)
        logging.info(f"Appending to BATCH: {data}")

        with LOCK:
            BATCH.append((data, raw))
            LAST_SIGNAL_TIME = time.time()
            logging.info(f"BATCH size now: {len(BATCH)}")

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(f"❌ Webhook error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

# ─── START ────────────────────────────────────────────
ensure_flusher()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
