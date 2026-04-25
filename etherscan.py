"""
etherscan.py — глибокий аналіз гаманця через Etherscan API
Викликається при першому контакті з новим гаманцем
"""
import os
import time
import requests
import logging
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
DAYS_BACK = 90
BASE_URL  = "https://api.etherscan.io/api"
STABLECOINS = {"USDC", "USDT", "DAI", "BUSD", "FRAX"}


def _get(params: dict, retries=3) -> list | str:
    if not ETHERSCAN_API_KEY:
        return []
    params["apikey"] = ETHERSCAN_API_KEY
    for attempt in range(retries):
        try:
            r = requests.get(BASE_URL, params=params, timeout=15)
            data = r.json()
            if data.get("status") == "1":
                return data["result"]
            if "rate limit" in str(data.get("result", "")).lower():
                time.sleep(2)
                continue
            return []
        except Exception as e:
            if attempt == retries - 1:
                log.warning(f"Etherscan error: {e}")
            time.sleep(1)
    return []


def get_block_by_timestamp(ts: int) -> int:
    result = _get({
        "module":    "block",
        "action":    "getblocknobytime",
        "timestamp": str(ts),
        "closest":   "before",
    })
    try:
        return int(result)
    except:
        return 0


def get_balance_at_block(address: str, block: int) -> float:
    result = _get({
        "module":  "account",
        "action":  "balance",
        "address": address,
        "tag":     hex(block),
    })
    try:
        return int(result) / 1e18
    except:
        return 0.0


def get_transactions(address: str, start_ts: int, end_ts: int) -> list:
    txs = _get({
        "module":     "account",
        "action":     "txlist",
        "address":    address,
        "startblock": 0,
        "endblock":   99999999,
        "sort":       "desc",
    })
    return [tx for tx in txs if start_ts <= int(tx["timeStamp"]) <= end_ts]


def get_token_transactions(address: str, start_ts: int, end_ts: int) -> list:
    txs = _get({
        "module":     "account",
        "action":     "tokentx",
        "address":    address,
        "startblock": 0,
        "endblock":   99999999,
        "sort":       "desc",
    })
    return [tx for tx in txs if start_ts <= int(tx["timeStamp"]) <= end_ts]


def deep_analyze(address: str, balance_now: float) -> dict:
    """
    Повний аналіз гаманця за 90 днів.
    Повертає словник з усіма метриками.
    """
    if not ETHERSCAN_API_KEY:
        log.warning("ETHERSCAN_API_KEY не встановлено — глибокий аналіз недоступний")
        return {}

    now      = datetime.now()
    start    = now - timedelta(days=DAYS_BACK)
    start_ts = int(start.timestamp())
    end_ts   = int(now.timestamp())

    log.info(f"Deep analysis for {address[:12]}...")

    # Баланс 90 днів тому
    block_90d     = get_block_by_timestamp(start_ts)
    balance_90d   = get_balance_at_block(address, block_90d) if block_90d else 0
    time.sleep(0.25)

    # Транзакції
    txs       = get_transactions(address, start_ts, end_ts)
    time.sleep(0.25)
    token_txs = get_token_transactions(address, start_ts, end_ts)
    time.sleep(0.25)

    # Аналіз ETH транзакцій
    addr = address.lower()
    eth_sent = eth_received = 0.0
    eth_profit_success = 0.0
    counterparts = set()
    success_count = 0
    eth_sizes = []

    for tx in txs:
        val       = int(tx["value"]) / 1e18
        gas       = int(tx.get("gasUsed", 0)) * int(tx.get("gasPrice", 0)) / 1e18
        is_sender = tx["from"].lower() == addr
        is_ok     = tx["isError"] == "0"

        if is_sender:
            eth_sent += val + gas
            counterparts.add((tx.get("to") or "").lower())
            if is_ok:
                eth_profit_success -= (val + gas)
                success_count += 1
        else:
            eth_received += val
            counterparts.add(tx["from"].lower())
            if is_ok:
                eth_profit_success += val

        eth_sizes.append(val)

    # Аналіз токен транзакцій
    token_volume_usd = 0.0
    for tx in token_txs:
        try:
            dec    = int(tx.get("tokenDecimal", 18))
            amount = int(tx["value"]) / (10 ** dec)
            if tx.get("tokenSymbol", "") in STABLECOINS:
                token_volume_usd += amount
        except:
            pass

    # Зміна депозиту
    if balance_90d > 0:
        deposit_change_pct = round((balance_now - balance_90d) / balance_90d * 100, 2)
        trading_pnl_pct    = round(eth_profit_success / balance_90d * 100, 2)
    elif balance_now > 0:
        deposit_change_pct = 100.0
        trading_pnl_pct    = 0.0
    else:
        deposit_change_pct = 0.0
        trading_pnl_pct    = 0.0

    return {
        "balance_90d_ago_eth":  round(balance_90d, 4),
        "deposit_change_pct":   deposit_change_pct,
        "trading_pnl_pct":      trading_pnl_pct,
        "trading_pnl_eth":      round(eth_profit_success, 4),
        "tx_count_90d":         len(txs),
        "success_tx_count":     success_count,
        "success_rate_pct":     round(success_count / len(txs) * 100, 1) if txs else 0,
        "eth_volume_90d":       round(eth_sent + eth_received, 4),
        "eth_sent_90d":         round(eth_sent, 4),
        "eth_received_90d":     round(eth_received, 4),
        "token_volume_usd_90d": round(token_volume_usd, 2),
        "unique_counterparts":  len(counterparts),
        "avg_tx_eth":           round(sum(eth_sizes) / len(eth_sizes), 4) if eth_sizes else 0,
        "max_tx_eth":           round(max(eth_sizes), 4) if eth_sizes else 0,
    }


def score_deep(balance_eth: float, metrics: dict) -> tuple[int, str]:
    """
    Розширений скоринг з урахуванням P&L і зміни депозиту.
    """
    score = 0

    # Баланс (макс 25)
    if balance_eth >= 10_000:   score += 25
    elif balance_eth >= 1_000:  score += 20
    elif balance_eth >= 500:    score += 15
    elif balance_eth >= 100:    score += 10
    elif balance_eth >= 10:     score += 5

    # Обсяг за 90 днів (макс 25)
    v = metrics.get("eth_volume_90d", 0)
    if v >= 100_000:   score += 25
    elif v >= 10_000:  score += 20
    elif v >= 1_000:   score += 15
    elif v >= 100:     score += 8
    elif v >= 10:      score += 3

    # Кількість транзакцій (макс 20)
    t = metrics.get("tx_count_90d", 0)
    if t >= 1000:   score += 20
    elif t >= 500:  score += 16
    elif t >= 100:  score += 10
    elif t >= 50:   score += 6
    elif t >= 10:   score += 3

    # Зміна депозиту (макс 15)
    d = metrics.get("deposit_change_pct", 0)
    if d >= 100:    score += 15
    elif d >= 50:   score += 12
    elif d >= 20:   score += 8
    elif d >= 5:    score += 4
    elif d > 0:     score += 2

    # P&L від угод (макс 15)
    p = metrics.get("trading_pnl_pct", 0)
    if p >= 50:    score += 15
    elif p >= 20:  score += 12
    elif p >= 10:  score += 8
    elif p >= 5:   score += 4
    elif p > 0:    score += 2

    score = min(score, 100)

    if score >= 80:   tier = "🐋 Mega Whale"
    elif score >= 60: tier = "🦈 Whale"
    elif score >= 40: tier = "🐬 Mid Whale"
    elif score >= 20: tier = "🐟 Small Fish"
    else:             tier = "🦐 Noise"

    return score, tier
