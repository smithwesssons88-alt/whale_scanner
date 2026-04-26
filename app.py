"""
app.py — Whale Parser
Alchemy Webhook → глибокий аналіз (Etherscan) → скор → Telegram алерт → БД
"""
import os
import logging
import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from web3 import Web3

from database import (
    init_db, upsert_wallet, save_trade,
    is_cache_fresh, needs_deep_analysis, get_cached_profile,
    get_copytrade_candidates, get_stats, COPYTRADE_MIN_SCORE,
)
from etherscan import deep_analyze

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID")
MIN_ETH     = float(os.getenv("MIN_ETH_THRESHOLD", "10"))
ALCHEMY_RPC = os.getenv("ALCHEMY_RPC_URL", "")

app = Flask(__name__)

w3  = Web3(Web3.HTTPProvider(ALCHEMY_RPC)) if ALCHEMY_RPC else None

init_db()

WHALE_LABELS = {
    "0xd8da6bf26964af9d7eed9e03e53415d37aa96045": "Vitalik Buterin",
    "0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503": "Binance Hot Wallet",
    "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8": "Binance Cold",
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance 14",
}

def load_wallets(path="wallets.txt"):
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return {l.strip().lower() for l in f if l.strip().startswith("0x")}

WATCHED_WALLETS = load_wallets()
log.info(f"Watching {len(WATCHED_WALLETS)} wallets")


def get_label(address):
    return WHALE_LABELS.get(address.lower(), f"{address[:6]}...{address[-4:]}")


def fetch_and_score(address: str, label: str = ""):
    """
    1. Якщо є свіжий кеш — повертає з БД
    2. Якщо перший раз бачимо — робить глибокий аналіз через Etherscan
    3. Інакше — простий скор через RPC
    """
    if is_cache_fresh(address):
        return get_cached_profile(address)

    # Отримуємо баланс і tx_count через RPC
    balance_eth = 0.0
    tx_count    = 0
    if w3:
        try:
            checksum    = w3.to_checksum_address(address)
            balance_eth = float(w3.from_wei(w3.eth.get_balance(checksum), "ether"))
            tx_count    = w3.eth.get_transaction_count(checksum)
        except Exception as e:
            log.error(f"RPC error for {address}: {e}")

    # Глибокий аналіз через Etherscan (тільки якщо ще не робили)
    deep_metrics = None
    if needs_deep_analysis(address):
        deep_metrics = deep_analyze(address, balance_eth)

    return upsert_wallet(address, balance_eth, tx_count, label, deep_metrics)


def build_alert(activity: dict, profile):
    try:
        raw = activity.get("value", "0x0")
        value_eth = int(raw, 16) / 1e18 if isinstance(raw, str) and raw.startswith("0x") else float(raw or 0)

        if value_eth < MIN_ETH:
            return None

        frm   = activity.get("fromAddress", "")
        to    = activity.get("toAddress", "")
        asset = activity.get("asset", "ETH")
        tx    = activity.get("hash", "")
        block = activity.get("blockNum", "")

        direction = "📤 SEND" if frm.lower() in WATCHED_WALLETS else "📥 RECEIVE"

        # Базовий рядок скору
        score_line = ""
        pnl_line   = ""
        candidate_line = ""

        if profile:
            score_line = f"*Score:* `{profile.score}/100` {profile.tier}\n"

            if profile.deep_analyzed:
                dep_sign = "+" if profile.deposit_change_pct >= 0 else ""
                pnl_sign = "+" if profile.trading_pnl_pct >= 0 else ""
                pnl_line = (
                    f"*Депозит 90д:* `{dep_sign}{profile.deposit_change_pct:.1f}%` | "
                    f"*P&L угод:* `{pnl_sign}{profile.trading_pnl_pct:.1f}%`\n"
                    f"*Обсяг 90д:* `{profile.eth_volume_90d:,.0f} ETH` | "
                    f"*Txs 90д:* `{profile.tx_count_90d}`\n"
        

            if profile.is_copytrade_candidate:
                candidate_line = "⭐ *Кандидат для копітрейдингу*\n"

        msg = (
            f"🐋 *Whale Alert* {direction}\n\n"
            f"*From:* `{get_label(frm)}`\n"
            f"*To:* `{get_label(to)}`\n"
            f"*Amount:* `{value_eth:,.4f} {asset}`\n"
            f"{score_line}"
            f"{pnl_line}"
            f"*Block:* `{block}`\n"
            f"{candidate_line}\n"
            f"[🔍 Etherscan](https://etherscan.io/tx/{tx})"

        return msg, value_eth
    except Exception as e:
        log.error(f"build_alert error: {e}")
        return None


def send_telegram(msg: str):
    try:
        
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": CHAT_ID, "text": msg,
            "parse_mode": "Markdown", "disable_web_page_preview": True,
        }, timeout=10)

    except Exception as e:
        log.error(f"Telegram error: {e}")


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    if not data:
        return jsonify({"error": "no data"}), 400

    activities = data.get("activity", [])
    log.info(f"Webhook: {len(activities)} activities")

    alerts_sent = 0
    for activity in activities:
        frm = activity.get("fromAddress", "")
        if not frm:
            continue

        label   = WHALE_LABELS.get(frm.lower(), "")
        profile = fetch_and_score(frm, label)

        result = build_alert(activity, profile)
        if not result:
            continue

        msg, value_eth = result
        send_telegram(msg)
        alerts_sent += 1

        save_trade(
            tx_hash       = activity.get("hash", ""),
            from_addr     = frm,
            to_addr       = activity.get("toAddress", ""),
            value_eth     = value_eth,
            asset         = activity.get("asset", "ETH"),
            block_num     = activity.get("blockNum", ""),
            score_at_time = profile.score if profile else 0,


    return jsonify({"ok": True, "alerts_sent": alerts_sent}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", **get_stats()})


@app.route("/candidates", methods=["GET"])
def candidates():
    limit = int(request.args.get("limit", 50))
    data  = get_copytrade_candidates(limit)
    return jsonify({"min_score": COPYTRADE_MIN_SCORE, "count": len(data), "candidates": data})


@app.route("/", methods=["GET"])
def index():
    return "🐋 Whale Parser is running", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
