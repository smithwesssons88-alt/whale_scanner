import os
import json
import time
import logging
import threading
import requests
from flask import Flask, jsonify
from web3 import Web3
from database import init_db, save_wallet, save_trade, get_stats, get_candidates

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Config
ALCHEMY_RPC_URL = os.environ.get('ALCHEMY_RPC_URL', '')
ALCHEMY_WS_URL = os.environ.get('ALCHEMY_WS_URL', '')  # wss://eth-mainnet.g.alchemy.com/v2/KEY
ETHERSCAN_API_KEY = os.environ.get('ETHERSCAN_API_KEY', '')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
MIN_ETH_THRESHOLD = float(os.environ.get('MIN_ETH_THRESHOLD', '100'))
COPYTRADE_MIN_SCORE = int(os.environ.get('COPYTRADE_MIN_SCORE', '50'))

# Dedupe cache: avoid spamming same wallet multiple times per hour
seen_recently = {}
seen_lock = threading.Lock()
SEEN_TTL = 3600  # 1 hour

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        if r.status_code == 200:
            logger.info("Telegram alert sent OK")
        else:
            logger.error(f"Telegram error: {r.status_code} {r.text}")
    except Exception as e:
        logger.error(f"Telegram exception: {e}")

def get_eth_balance(address):
    try:
        w3 = Web3(Web3.HTTPProvider(ALCHEMY_RPC_URL))
        balance_wei = w3.eth.get_balance(Web3.to_checksum_address(address))
        return float(Web3.from_wei(balance_wei, 'ether'))
    except Exception as e:
        logger.error(f"Balance error for {address}: {e}")
        return 0.0

def get_etherscan_history(address):
    """Get last 90 days tx stats from Etherscan with retry"""
    url = "https://api.etherscan.io/v2/api"
    cutoff = int(time.time()) - 90 * 86400
    empty = {"tx_count_90d": 0, "volume_90d": 0.0, "pnl_pct": 0.0, "deposit_change_pct": 0.0}

    if not ETHERSCAN_API_KEY:
        logger.error("ETHERSCAN_API_KEY not set!")
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

                total_in = sum(
                    int(tx["value"]) for tx in recent
                    if tx.get("to", "").lower() == address.lower() and tx.get("isError") == "0"
                )
                total_out = sum(
                    int(tx["value"]) for tx in recent
                    if tx.get("from", "").lower() == address.lower() and tx.get("isError") == "0"
                )

                total_in_eth = total_in / 1e18
                total_out_eth = total_out / 1e18
                volume = total_in_eth + total_out_eth
                pnl_pct = ((total_in_eth - total_out_eth) / total_out_eth * 100) if total_out_eth > 0 else 0

                logger.info(f"Etherscan OK: {tx_count} txs, volume={volume:.1f} ETH")
                return {
                    "tx_count_90d": tx_count,
                    "volume_90d": round(volume, 2),
                    "pnl_pct": round(pnl_pct, 2),
                    "deposit_change_pct": round(pnl_pct, 2)
                }

            elif "rate limit" in message.lower() or status == "0":
                logger.warning(f"Etherscan rate limit or no tx: {message}, retrying in 2s...")
                time.sleep(2)
                continue
            else:
                logger.error(f"Etherscan unexpected: status={status} message={message}")
                return empty

        except Exception as e:
            logger.error(f"Etherscan exception (attempt {attempt+1}): {e}")
            time.sleep(2)

    logger.error(f"Etherscan failed after 3 attempts for {address[:10]}...")
    return empty

def score_wallet(balance, stats):
    score = 0
    # Balance score (max 25)
    if balance >= 10000: score += 25
    elif balance >= 1000: score += 20
    elif balance >= 500: score += 15
    elif balance >= 100: score += 10
    elif balance >= 10: score += 5

    # Volume 90d (max 25)
    vol = stats["volume_90d"]
    if vol >= 100000: score += 25
    elif vol >= 10000: score += 20
    elif vol >= 1000: score += 15
    elif vol >= 100: score += 8

    # Tx count 90d (max 20)
    txc = stats["tx_count_90d"]
    if txc >= 1000: score += 20
    elif txc >= 500: score += 15
    elif txc >= 100: score += 10
    elif txc >= 10: score += 5

    # Deposit change (max 15)
    dc = stats["deposit_change_pct"]
    if dc >= 50: score += 15
    elif dc >= 20: score += 10
    elif dc >= 5: score += 5

    # PnL (max 15)
    pnl = stats["pnl_pct"]
    if pnl >= 50: score += 15
    elif pnl >= 20: score += 10
    elif pnl >= 5: score += 5

    return min(score, 100)

def get_tier(score):
    if score >= 80: return "🐋 Mega Whale"
    if score >= 60: return "🦈 Whale"
    if score >= 40: return "🐬 Mid Whale"
    if score >= 20: return "🐟 Small Fish"
    return "🦐 Noise"

def process_transaction(tx_hash, from_addr, to_addr, value_eth, is_pending=True):
    """Process a detected large transaction"""
    now = time.time()

    # Dedupe with lock: skip if we processed this address recently
    with seen_lock:
        if from_addr in seen_recently and (now - seen_recently[from_addr]) < SEEN_TTL:
            logger.info(f"Skip duplicate {from_addr[:10]}... (seen recently)")
            return
        seen_recently[from_addr] = now
        # Clean old entries
        expired = [k for k, v in seen_recently.items() if now - v > SEEN_TTL]
        for k in expired:
            del seen_recently[k]

    logger.info(f"Processing whale tx: {from_addr[:10]}... sent {value_eth:.1f} ETH")

    # Get balance and history
    balance = get_eth_balance(from_addr)
    stats = get_etherscan_history(from_addr)
    score = score_wallet(balance, stats)
    tier = get_tier(score)

    # Save to DB
    save_wallet(from_addr, balance, score, stats)
    save_trade(from_addr, tx_hash, value_eth, "OUT")

    # Build Telegram message
    copytrade_flag = "✅ КОПІТРЕЙД" if score >= COPYTRADE_MIN_SCORE else "❌ не рекомендовано"
    status = "⏳ Pending" if is_pending else "✅ Confirmed"

    msg = (
        f"{tier} <b>Whale Alert!</b>\n\n"
        f"{status}\n"
        f"💸 <b>{value_eth:.2f} ETH</b> (${value_eth * 3000:,.0f})\n\n"
        f"📤 From: <code>{from_addr}</code>\n"
        f"📥 To: <code>{to_addr[:20]}...</code>\n"
        f"🔗 TX: <a href='https://etherscan.io/tx/{tx_hash}'>Etherscan</a>\n\n"
        f"📊 <b>Аналіз гаманця (90д):</b>\n"
        f"  💰 Баланс: {balance:.1f} ETH\n"
        f"  📈 Обсяг: {stats['volume_90d']:,.0f} ETH\n"
        f"  🔄 Кількість tx: {stats['tx_count_90d']}\n"
        f"  📉 P&L: {stats['pnl_pct']:+.1f}%\n\n"
        f"🏆 Score: <b>{score}/100</b>\n"
        f"{copytrade_flag}\n"
        f"🌐 <a href='https://etherscan.io/address/{from_addr}'>Профіль</a>"
    )

    send_telegram(msg)
    logger.info(f"Alert sent: {from_addr[:10]}... score={score} value={value_eth:.1f} ETH")

def watch_pending_transactions():
    """WebSocket listener for pending transactions"""
    import websocket

    ws_url = ALCHEMY_WS_URL
    if not ws_url:
        # Derive from RPC URL
        if ALCHEMY_RPC_URL:
            key = ALCHEMY_RPC_URL.rstrip('/').split('/')[-1]
            ws_url = f"wss://eth-mainnet.g.alchemy.com/v2/{key}"
        else:
            logger.error("No Alchemy WS URL configured!")
            return

    subscribe_msg = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["alchemy_pendingTransactions"]
    })

    def on_message(ws, message):
        try:
            data = json.loads(message)
            params = data.get("params", {})
            result = params.get("result", {})

            if not result:
                return

            value_hex = result.get("value", "0x0")
            value_wei = int(value_hex, 16)
            value_eth = value_wei / 1e18

            if value_eth < MIN_ETH_THRESHOLD:
                return

            tx_hash = result.get("hash", "")
            from_addr = result.get("from", "")
            to_addr = result.get("to", "") or "Contract"

            if not from_addr:
                return

            logger.info(f"Large tx detected: {value_eth:.1f} ETH from {from_addr[:10]}...")

            # Process in separate thread to not block WS
            t = threading.Thread(
                target=process_transaction,
                args=(tx_hash, from_addr, to_addr, value_eth, True),
                daemon=True
            )
            t.start()

        except Exception as e:
            logger.error(f"on_message error: {e}")

    def on_error(ws, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        logger.warning(f"WebSocket closed: {close_status_code} {close_msg}")
        logger.info("Reconnecting in 5s...")
        time.sleep(5)
        start_ws()

    def on_open(ws):
        logger.info("WebSocket connected, subscribing to pending transactions...")
        ws.send(subscribe_msg)

    def start_ws():
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever(ping_interval=30, ping_timeout=10)

    start_ws()


# Flask endpoints
@app.route('/health')
def health():
    stats = get_stats()
    return jsonify({
        "status": "ok",
        "threshold_eth": MIN_ETH_THRESHOLD,
        **stats
    })

@app.route('/candidates')
def candidates():
    wallets = get_candidates(COPYTRADE_MIN_SCORE)
    return jsonify(wallets)

@app.route('/test')
def test_alert():
    """Send a test alert"""
    send_telegram(
        "🧪 <b>Test Alert</b>\n\n"
        "Whale monitor is running!\n"
        f"Threshold: {MIN_ETH_THRESHOLD} ETH\n"
        "Monitoring: ALL pending transactions"
    )
    return jsonify({"status": "test sent"})


if __name__ == '__main__':
    init_db()
    logger.info(f"Starting Whale Monitor (threshold: {MIN_ETH_THRESHOLD} ETH)")
    logger.info("Architecture: WebSocket → ALL pending tx → filter > threshold")

    # Start WebSocket listener in background thread
    ws_thread = threading.Thread(target=watch_pending_transactions, daemon=True)
    ws_thread.start()

    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
