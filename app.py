"""
app.py — Whale Parser
Alchemy Webhook → скоринг → Telegram алерт → збереження в БД
"""
import os
import asyncio
import logging
from flask import Flask, request, jsonify
from telegram import Bot
from dotenv import load_dotenv
from web3 import Web3

from database import (
    init_db, upsert_wallet, save_trade,
    is_cache_fresh, get_cached_profile,
    get_copytrade_candidates, get_stats,
    COPYTRADE_MIN_SCORE,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Конфіг ──────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID")
MIN_ETH     = float(os.getenv("MIN_ETH_THRESHOLD", "10"))
ALCHEMY_RPC = os.getenv("ALCHEMY_RPC_URL", "")

app = Flask(__name__)
bot = Bot(token=BOT_TOKEN)
w3  = Web3(Web3.HTTPProvider(ALCHEMY_RPC)) if ALCHEMY_RPC else None

init_db()

# ── Відомі лейбли (lowercase) ────────────────────────────────────────────────
WHALE_LABELS: dict[str, str] = {
    "0xd8da6bf26964af9d7eed9e03e53415d37aa96045": "Vitalik Buterin",
    "0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503": "Binance Hot Wallet",
    "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8": "Binance Cold",
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance 14",
}

def load_wallets(path: str = "wallets.txt") -> set[str]:
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return {l.strip().lower() for l in f if l.strip().startswith("0x")}

WATCHED_WALLETS = load_wallets()
log.info(f"Watching {len(WATCHED_WALLETS)} wallets")


# ── Скоринг гаманця ──────────────────────────────────────────────────────────

def fetch_and_score(address: str, label: str = ""):
    """
    Свіжий кеш (< 1 год) → з БД.
    Інакше → запит до ноди → зберегти в БД.
    """
    if is_cache_fresh(address):
        return get_cached_profile(address)

    if not w3:
        log.warning("No ALCHEMY_RPC_URL — skipping on-chain scoring")
        return None

    try:
        checksum = w3.to_checksum_address(address)
        balance  = float(w3.from_wei(w3.eth.get_balance(checksum), "ether"))
        tx_count = w3.eth.get_transaction_count(checksum)
        return upsert_wallet(address, balance, tx_count, label)
    except Exception as e:
        log.error(f"Error fetching wallet {address}: {e}")
        return None


# ── Форматування алерту ──────────────────────────────────────────────────────

def get_label(address: str) -> str:
    return WHALE_LABELS.get(address.lower(), f"{address[:6]}...{address[-4:]}")


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

        score_line = ""
        candidate_line = ""
        if profile:
            score_line = f"*Score:* `{profile.score}/100` {profile.tier}\n"
            if profile.is_copytrade_candidate:
                candidate_line = "⭐ *Кандидат для копітрейдингу*\n"

        msg = (
            f"🐋 *Whale Alert* {direction}\n\n"
            f"*From:* `{get_label(frm)}`\n"
            f"*To:* `{get_label(to)}`\n"
            f"*Amount:* `{value_eth:,.4f} {asset}`\n"
            f"{score_line}"
            f"*Block:* `{block}`\n"
            f"{candidate_line}\n"
            f"[🔍 Etherscan](https://etherscan.io/tx/{tx})"
        )
        return msg, value_eth
    except Exception as e:
        log.error(f"build_alert error: {e}")
        return None


async def send_telegram(msg: str):
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=msg,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.error(f"Telegram error: {e}")


# ── Роути ────────────────────────────────────────────────────────────────────

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

        # 1. Скоринг (з кешем)
        label   = WHALE_LABELS.get(frm.lower(), "")
        profile = fetch_and_score(frm, label)

        # 2. Будуємо і надсилаємо алерт
        result = build_alert(activity, profile)
        if not result:
            continue

        msg, value_eth = result
        asyncio.run(send_telegram(msg))
        alerts_sent += 1

        # 3. Зберігаємо угоду в history
        save_trade(
            tx_hash       = activity.get("hash", ""),
            from_addr     = frm,
            to_addr       = activity.get("toAddress", ""),
            value_eth     = value_eth,
            asset         = activity.get("asset", "ETH"),
            block_num     = activity.get("blockNum", ""),
            score_at_time = profile.score if profile else 0,
        )

    return jsonify({"ok": True, "alerts_sent": alerts_sent}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", **get_stats()})


@app.route("/candidates", methods=["GET"])
def candidates():
    """Топ гаманці для копітрейдингу."""
    limit = int(request.args.get("limit", 50))
    data  = get_copytrade_candidates(limit)
    return jsonify({
        "min_score":  COPYTRADE_MIN_SCORE,
        "count":      len(data),
        "candidates": data,
    })


@app.route("/", methods=["GET"])
def index():
    return "🐋 Whale Parser is running", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
