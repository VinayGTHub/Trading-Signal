from gevent import monkey
monkey.patch_all()

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
BOT_TOKEN         = os.getenv("BOT_TOKEN")
CHAT_ID           = os.getenv("CHAT_ID")
DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

CSV_URL  = "https://images.dhan.co/api-data/api-scrip-master.csv"
CSV_FILE = "scrip_master.csv"

SYMBOL_CACHE = {}

# ─── CSV ──────────────────────────────────────────────
def ensure_csv():
    if os.path.exists(CSV_FILE):
        return
    logging.info("Downloading scrip master CSV...")
    r = requests.get(CSV_URL, timeout=10)
    with open(CSV_FILE, "w") as f:
        f.write(r.text)
    logging.info("CSV downloaded")

# ─── SYMBOL LOOKUP ────────────────────────────────────
def lookup_security_id(symbol):
    symbol = symbol.upper()
    if symbol in SYMBOL_CACHE:
        logging.info(f"Cache hit: {symbol} → {SYMBOL_CACHE[symbol]}")
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
                logging.info(f"Symbol found: {symbol} → {exch_key}, {sid}")
                return exch_key, sid

    logging.error(f"❌ Symbol not found in CSV: {symbol}")
    return None, None

# ─── DHAN API ─────────────────────────────────────────
def fetch_market_data(exch, sid):
    logging.info(f"Calling Dhan API: exch={exch}, sid={sid}")
    url = "https://api.dhan.co/v2/marketfeed/quote"
    headers = {
        "access-token": DHAN_ACCESS_TOKEN,
        "client-id":    DHAN_CLIENT_ID,
        "Content-Type": "application/json"
    }
    payload = {exch: [int(sid)]}
    logging.info(f"Dhan payload: {payload}")

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=5)
        logging.info(f"Dhan status: {r.status_code} | body: {r.text}")

        if r.status_code != 200:
            logging.error(f"❌ Dhan error: {r.text}")
            return None

        data = r.json().get("data", {})
        instruments = data.get(exch, {})
        return instruments.get(str(sid))

    except Exception as e:
        logging.error(f"❌ Dhan exception: {e}", exc_info=True)
        return None

# ─── TELEGRAM ─────────────────────────────────────────
def send_telegram(msg):
    logging.info(f"Sending Telegram: {msg}")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=5)
        logging.info(f"Telegram response: {r.status_code} | {r.text}")
    except Exception as e:
        logging.error(f"❌ Telegram exception: {e}", exc_info=True)

# ─── PARSE SIGNAL ─────────────────────────────────────
def parse_signal(raw):
    raw_upper = raw.strip().upper()
    logging.info(f"Parsing: {raw_upper}")

    direction = symbol = price = None
    try:
        parts = raw_upper.split()
        if len(parts) >= 4 and parts[0] in ("BUY", "SELL") and parts[2] == "AT":
            direction, symbol, price = parts[0], parts[1], parts[3]
        else:
            direction = "BUY" if "BUY" in raw_upper else "SELL" if "SELL" in raw_upper else None
            if "AT" in raw_upper:
                at_parts = raw_upper.split("AT")
                symbol   = at_parts[0].split()[-1].strip()
                price    = at_parts[1].strip().split()[0]
    except Exception as e:
        logging.error(f"❌ Parse error: {e}", exc_info=True)

    logging.info(f"Parsed → direction={direction}, symbol={symbol}, price={price}")
    return {"direction": direction, "symbol": symbol, "price": price}

# ─── PROCESS SIGNAL (runs in background thread) ───────
def process_signal(data):
    try:
        direction = data.get("direction")
        symbol    = data.get("symbol")
        price     = data.get("price", "N/A")

        logging.info(f"Processing signal: {direction} {symbol} @ {price}")

        if not direction or not symbol:
            logging.warning("❌ Missing direction or symbol — aborting")
            return

        exch, sid = lookup_security_id(symbol)

        if not sid:
            # Symbol not found — still notify
            msg = f"⚠️ {direction} {symbol} @ {price}\n[Symbol not found in scrip master]"
            send_telegram(msg)
            return

        instrument = fetch_market_data(exch, sid)

        if not instrument:
            # Market closed or Dhan error — use signal price
            msg = f"📊 {direction} {symbol} @ {price}\n[Market closed / No LTP from Dhan]"
            send_telegram(msg)
            return

        ltp      = instrument.get("last_price", price)
        buy_qty  = instrument.get("buy_quantity", 0)
        sell_qty = instrument.get("sell_quantity", 0)
        total    = buy_qty + sell_qty

        # Order flow filter — only active when market is open (total > 0)
        if total > 0:
            buyer_strength  = buy_qty / total
            seller_strength = sell_qty / total

            if direction == "BUY" and buyer_strength < seller_strength:
                logging.info(f"⛔ BUY filtered — weak buyer strength ({buyer_strength:.2f})")
                return
            if direction == "SELL" and seller_strength < buyer_strength:
                logging.info(f"⛔ SELL filtered — weak seller strength ({seller_strength:.2f})")
                return

        msg = f"✅ {direction} {symbol} @ {ltp}"
        send_telegram(msg)

    except Exception as e:
        logging.error(f"❌ process_signal error: {e}", exc_info=True)

# ─── ROUTES ───────────────────────────────────────────
@app.route("/")
def home():
    return "Server Running", 200

@app.route("/health")
def health():
    return jsonify({
        "bot_token_set":         bool(BOT_TOKEN),
        "chat_id_set":           bool(CHAT_ID),
        "dhan_client_id_set":    bool(DHAN_CLIENT_ID),
        "dhan_access_token_set": bool(DHAN_ACCESS_TOKEN),
        "symbol_cache":          list(SYMBOL_CACHE.keys()),
    }), 200

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        raw = request.get_data().decode('utf-8')
        logging.info(f"Webhook received: {raw}")

        data = parse_signal(raw)

        # ✅ Fire and forget in a thread — no shared batch, no inter-process issues
        t = threading.Thread(target=process_signal, args=(data,))
        t.daemon = True
        t.start()
        logging.info("Background thread started for signal processing")

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(f"❌ Webhook error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
