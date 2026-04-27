import os
import json
import time
import logging
import threading
import requests
from flask import Flask, jsonify
from web3 import Web3
from database import init_db, save_wallet, save_trade, get_stats, get_candidates

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

ALCHEMY_RPC_URL = os.environ.get('ALCHEMY_RPC_URL', '')
ALCHEMY_WS_URL = os.environ.get('ALCHEMY_WS_URL', '')
ETHERSCAN_API_KEY = os.environ.get('ETHERSCAN_API_KEY', '')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
MIN_ETH_THRESHOLD = float(os.environ.get('MIN_ETH_THRESHOLD', '100'))
COPYTRADE_MIN_SCORE = int(os.environ.get('COPYTRADE_MIN_SCORE', '50'))

seen_recently = {}
seen_lock = threading.Lock()
SEEN_TTL = 3600

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
        if r.status_code == 200:
            logger.info("Telegram alert sent OK")
        else:
            logger.error(f"Telegram error: {r.status_code}")
    except Exception as e:
        logger.error(f"Telegram exception: {e}")

def get_eth_balance(address):
    try:
        w3 = Web3(Web3.HTTPProvider(ALCHEMY_RPC_URL))
        bal = w3.eth.get_balance(Web3.to_checksum_address(address))
        return float(Web3.from_wei(bal, 'ether'))
    except Exception as e:
        logger.error(f"Balance error: {e}")
        return 0.0

def get_etherscan_history(address):
    url = "https://api.etherscan.io/v2/api"
    cutoff = int(time.time()) - 90 * 86400
    empty = {"tx_count_90d": 0, "volume_90d": 0.0}

    if not ETHERSCAN_API_KEY:
        return empty

    params = {
        "chainid": 1,
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "desc",
        "apikey": ETHERSCAN_API_KEY
    }

    for attempt in range(3):
        try:
            logger.info(f"Etherscan request for {address[:10]}... (attempt {attempt+1})")
            r = requests.get(url, params=params, timeout=20)
            data = r.json()
            status = data.get("status")
            message = data.get("message", "")
            logger.info(f"Etherscan response: status={status} message={message}")

            if status == "1":
                txs = data.get("result", [])
                recent = [tx for tx in txs if int(tx.get("timeStamp", 0)) >= cutoff]
                tx_count = len(recent)
                addr_lower = address.lower()
                total_in = sum(int(tx["value"]) for tx in recent if tx.get("to","").lower() == addr_lower and tx.get("isError") == "0")
                total_out = sum(int(tx["value"]) for tx in recent if tx.get("from","").lower() == addr_lower and tx.get("isError") == "0")
                volume = (total_in + total_out) / 1e18
                logger.info(f"Etherscan OK: {tx_count} txs, volume={volume:.1f} ETH")
                return {"tx_count_90d": tx_count, "volume_90d": round(volume, 2)}

            elif status == "0":
                logger.warning(f"Etherscan NOTOK: {message}, retrying in 2s...")
                time.sleep(2)
            else:
                logger.error(f"Etherscan unexpected: {status} {message}")
                return empty

        except Exception as e:
            logger.error(f"Etherscan exception (attempt {attempt+1}): {e}")
            time.sleep(2)

    logger.error(f"Etherscan failed after 3 attempts for {address[:10]}...")
    return empty

def score_wallet(balance, stats):
    score = 0
    # Balance (max 35)
    if balance >= 10000: score += 35
    elif balance >= 1000: score += 28
    elif balance >= 500: score += 20
    elif balance >= 100: score += 12
    elif balance >= 10: score += 5
    # Volume 90d (max 35)
    vol = stats["volume_90d"]
    if vol >= 100000: score += 35
    elif vol >= 10000: score += 28
    elif vol >= 1000: score += 20
    elif vol >= 100: score += 10
    # Tx count 90d (max 30)
    txc = stats["tx_count_90d"]
    if txc >= 1000: score += 30
    elif txc >= 500: score += 22
    elif txc >= 100: score += 15
    elif txc >= 10: score += 7
    return min(score, 100)

def get_tier(score):
    if score >= 80: return "🐋 Mega Whale"
    if score >= 60: return "🦈 Whale"
    if score >= 40: return "🐬 Mid Whale"
    if score >= 20: return "🐟 Small Fish"
    return "🦐 Noise"

def process_transaction(tx_hash, from_addr, to_addr, value_eth, is_pending=True):
    now = time.time()
    with seen_lock:
        if from_addr in seen_recently and (now - seen_recently[from_addr]) < SEEN_TTL:
            logger.info(f"Skip duplicate {from_addr[:10]}...")
            return
        seen_recently[from_addr] = now
        for k in [k for k, v in seen_recently.items() if now - v > SEEN_TTL]:
            del seen_recently[k]

    logger.info(f"Processing whale tx: {from_addr[:10]}... sent {value_eth:.1f} ETH")

    balance = get_eth_balance(from_addr)
    stats = get_etherscan_history(from_addr)
    score = score_wallet(balance, stats)
    tier = get_tier(score)

    save_wallet(from_addr, balance, score, stats)
    save_trade(from_addr, tx_hash, value_eth, "OUT")

    copytrade = "✅ КОПІТРЕЙД" if score >= COPYTRADE_MIN_SCORE else "❌ не рекомендовано"
    status_str = "⏳ Pending" if is_pending else "✅ Confirmed"

    msg = (
        f"{tier} <b>Whale Alert!</b>\n\n"
        f"{status_str}\n"
        f"💸 <b>{value_eth:.2f} ETH</b> (${value_eth * 3000:,.0f})\n\n"
        f"📤 From: <code>{from_addr}</code>\n"
        f"📥 To: <code>{to_addr[:20]}...</code>\n"
        f"🔗 TX: <a href='https://etherscan.io/tx/{tx_hash}'>Etherscan</a>\n\n"
        f"📊 <b>Аналіз гаманця (90д):</b>\n"
        f"  💰 Баланс: {balance:.1f} ETH\n"
        f"  📈 Обсяг: {stats['volume_90d']:,.0f} ETH\n"
        f"  🔄 Транзакцій: {stats['tx_count_90d']}\n\n"
        f"🏆 Score: <b>{score}/100</b>\n"
        f"{copytrade}\n"
        f"🌐 <a href='https://etherscan.io/address/{from_addr}'>Профіль</a>"
    )

    send_telegram(msg)
    logger.info(f"Alert sent: {from_addr[:10]}... score={score} value={value_eth:.1f} ETH")

def watch_pending_transactions():
    import websocket

    ws_url = ALCHEMY_WS_URL
    if not ws_url:
        if ALCHEMY_RPC_URL:
            key = ALCHEMY_RPC_URL.rstrip('/').split('/')[-1]
            ws_url = f"wss://eth-mainnet.g.alchemy.com/v2/{key}"
        else:
            logger.error("No Alchemy WS URL configured!")
            return

    subscribe_msg = json.dumps({
        "jsonrpc": "2.0", "id": 1,
        "method": "eth_subscribe",
        "params": ["alchemy_pendingTransactions"]
    })

    def on_message(ws, message):
        try:
            data = json.loads(message)
            result = data.get("params", {}).get("result", {})
            if not result:
                return
            value_eth = int(result.get("value", "0x0"), 16) / 1e18
            if value_eth < MIN_ETH_THRESHOLD:
                return
            from_addr = result.get("from", "")
            if not from_addr:
                return
            logger.info(f"Large tx detected: {value_eth:.1f} ETH from {from_addr[:10]}...")
            threading.Thread(
                target=process_transaction,
                args=(result.get("hash",""), from_addr, result.get("to","") or "Contract", value_eth, True),
                daemon=True
            ).start()
        except Exception as e:
            logger.error(f"on_message error: {e}")

    def on_error(ws, error): logger.error(f"WebSocket error: {error}")
    def on_close(ws, *args):
        logger.warning("WebSocket closed, reconnecting in 5s...")
        time.sleep(5)
        start_ws()
    def on_open(ws):
        logger.info("WebSocket connected, subscribing...")
        ws.send(subscribe_msg)

    def start_ws():
        websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message,
            on_error=on_error, on_close=on_close).run_forever(ping_interval=30, ping_timeout=10)

    start_ws()


@app.route('/health')
def health():
    return jsonify({"status": "ok", "threshold_eth": MIN_ETH_THRESHOLD, **get_stats()})

@app.route('/candidates')
def candidates():
    return jsonify(get_candidates(COPYTRADE_MIN_SCORE))

@app.route('/test')
def test_alert():
    send_telegram(f"🧪 <b>Test</b>\nThreshold: {MIN_ETH_THRESHOLD} ETH\nRunning OK")
    return jsonify({"status": "test sent"})


if __name__ == '__main__':
    init_db()
    logger.info(f"Starting Whale Monitor (threshold: {MIN_ETH_THRESHOLD} ETH)")
    threading.Thread(target=watch_pending_transactions, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
