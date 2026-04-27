"""
analyze_wallets.py — глибокий аналіз гаманців через Etherscan API
Метрики: баланс, обсяг, P&L, зміна депозиту за 90 днів

Запуск: python analyze_wallets.py
"""
import os
import time
import csv
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YourApiKeyToken")
WALLETS_FILE      = "wallets.txt"
OUTPUT_CSV        = "whale_analysis_90d.csv"
DAYS_BACK         = 90

BASE_URL = "https://api.etherscan.io/api"

def load_wallets():
    with open(WALLETS_FILE) as f:
        return [l.strip() for l in f if l.strip().startswith("0x")]

def etherscan_get(params, retries=3):
    """GET запит до Etherscan з повторами при помилці."""
    params["apikey"] = ETHERSCAN_API_KEY
    for attempt in range(retries):
        try:
            r = requests.get(BASE_URL, params=params, timeout=15)
            data = r.json()
            if data.get("status") == "1":
                return data["result"]
            # Rate limit — чекаємо
            if "rate limit" in str(data.get("result", "")).lower():
                time.sleep(2)
                continue
            return []
        except Exception as e:
            if attempt == retries - 1:
                print(f" [err: {e}]", end="")
            time.sleep(1)
    return []

def get_eth_balance(address):
    """Поточний баланс ETH."""
    result = etherscan_get({
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
    })
    try:
        return int(result) / 1e18
    except:
        return 0.0

def get_balance_at_block(address, block):
    """Баланс ETH на конкретному блоці (приблизно N днів тому)."""
    result = etherscan_get({
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": hex(block),
    })
    try:
        return int(result) / 1e18
    except:
        return 0.0

def get_block_by_timestamp(timestamp):
    """Знаходить номер блоку для заданого часу."""
    result = etherscan_get({
        "module":    "block",
        "action":    "getblocknobytime",
        "timestamp": str(int(timestamp)),
        "closest":   "before",
    })
    try:
        return int(result)
    except:
        return 0

def get_transactions(address, start_ts, end_ts):
    """Всі ETH транзакції за період."""
    txs = etherscan_get({
        "module":     "account",
        "action":     "txlist",
        "address":    address,
        "startblock": 0,
        "endblock":   99999999,
        "sort":       "desc",
    })
    return [
        tx for tx in txs
        if start_ts <= int(tx["timeStamp"]) <= end_ts
    ]

def get_token_transactions(address, start_ts, end_ts):
    """ERC-20 токен транзакції за період (USDC, WETH тощо)."""
    txs = etherscan_get({
        "module":     "account",
        "action":     "tokentx",
        "address":    address,
        "startblock": 0,
        "endblock":   99999999,
        "sort":       "desc",
    })
    return [
        tx for tx in txs
        if start_ts <= int(tx["timeStamp"]) <= end_ts
    ]

def analyze(address, txs, token_txs, balance_now, balance_90d_ago):
    """
    Розраховує всі метрики включно з P&L і зміною депозиту.
    """
    addr = address.lower()

    # ── ETH метрики ────────────────────────────────────────────
    eth_sent = eth_received = 0.0
    eth_sizes = []
    counterparts = set()
    success_txs = 0
    eth_profit_from_success = 0.0

    for tx in txs:
        val = int(tx["value"]) / 1e18
        gas = int(tx["gasUsed"]) * int(tx["gasPrice"]) / 1e18 if tx["isError"] == "0" else 0
        is_sender = tx["from"].lower() == addr

        if is_sender:
            eth_sent += val + gas
            counterparts.add(tx["to"].lower() if tx["to"] else "")
        else:
            eth_received += val
            counterparts.add(tx["from"].lower())

        eth_sizes.append(val)

        if tx["isError"] == "0":
            success_txs += 1
            # Профіт від успішних угод: отримав більше ніж відправив
            if not is_sender:
                eth_profit_from_success += val
            else:
                eth_profit_from_success -= (val + gas)

    # ── Токен метрики (USDC, WETH тощо) ────────────────────────
    token_volume_usd = 0.0
    stablecoins = {"USDC", "USDT", "DAI", "BUSD", "FRAX"}

    for tx in token_txs:
        try:
            decimals = int(tx.get("tokenDecimal", 18))
            amount = int(tx["value"]) / (10 ** decimals)
            symbol = tx.get("tokenSymbol", "")
            # Стейблкоїни рахуємо як USD
            if symbol in stablecoins:
                token_volume_usd += amount
        except:
            pass

    # ── Зміна депозиту ─────────────────────────────────────────
    # Загальна зміна балансу за 90 днів (в %)
    if balance_90d_ago > 0:
        deposit_change_pct = round((balance_now - balance_90d_ago) / balance_90d_ago * 100, 2)
    elif balance_now > 0:
        deposit_change_pct = 100.0
    else:
        deposit_change_pct = 0.0

    # Зміна завдяки успішним угодам (ETH P&L як % від початкового балансу)
    if balance_90d_ago > 0:
        trading_pnl_pct = round(eth_profit_from_success / balance_90d_ago * 100, 2)
    else:
        trading_pnl_pct = 0.0

    eth_volume = round(eth_sent + eth_received, 4)

    return {
        # Основні
        "balance_now_eth":      round(balance_now, 4),
        "balance_90d_ago_eth":  round(balance_90d_ago, 4),

        # Зміна депозиту
        "deposit_change_pct":   deposit_change_pct,       # загальна зміна балансу %
        "trading_pnl_pct":      trading_pnl_pct,          # % зміни завдяки угодам
        "trading_pnl_eth":      round(eth_profit_from_success, 4),

        # Активність
        "tx_count_90d":         len(txs),
        "success_tx_count":     success_txs,
        "success_rate_pct":     round(success_txs / len(txs) * 100, 1) if txs else 0,
        "eth_volume_90d":       eth_volume,
        "eth_sent_90d":         round(eth_sent, 4),
        "eth_received_90d":     round(eth_received, 4),
        "token_volume_usd_90d": round(token_volume_usd, 2),
        "unique_counterparts":  len(counterparts),
        "avg_tx_eth":           round(sum(eth_sizes)/len(eth_sizes), 4) if eth_sizes else 0,
        "max_tx_eth":           round(max(eth_sizes), 4) if eth_sizes else 0,
    }

def score_wallet(m):
    """Скоринг 0–100 на основі всіх метрик."""
    score = 0

    # Баланс (макс 25)
    b = m["balance_now_eth"]
    if b >= 10_000:   score += 25
    elif b >= 1_000:  score += 20
    elif b >= 500:    score += 15
    elif b >= 100:    score += 10
    elif b >= 10:     score += 5

    # Обсяг за 90 днів (макс 25)
    v = m["eth_volume_90d"]
    if v >= 100_000:   score += 25
    elif v >= 10_000:  score += 20
    elif v >= 1_000:   score += 15
    elif v >= 100:     score += 8
    elif v >= 10:      score += 3

    # Кількість транзакцій (макс 20)
    t = m["tx_count_90d"]
    if t >= 1000:   score += 20
    elif t >= 500:  score += 16
    elif t >= 100:  score += 10
    elif t >= 50:   score += 6
    elif t >= 10:   score += 3

    # Зміна депозиту (макс 15) — нагороджуємо зростання
    d = m["deposit_change_pct"]
    if d >= 100:    score += 15
    elif d >= 50:   score += 12
    elif d >= 20:   score += 8
    elif d >= 5:    score += 4
    elif d > 0:     score += 2

    # P&L від угод (макс 15)
    p = m["trading_pnl_pct"]
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

def main():
    wallets = load_wallets()
    now     = datetime.now()
    start   = now - timedelta(days=DAYS_BACK)
    start_ts = int(start.timestamp())
    end_ts   = int(now.timestamp())

    print(f"Аналізуємо {len(wallets)} гаманців за {DAYS_BACK} днів ({start.strftime('%Y-%m-%d')} — {now.strftime('%Y-%m-%d')})")
    print(f"API key: {'встановлено ✅' if ETHERSCAN_API_KEY != 'YourApiKeyToken' else '⚠️ НЕ встановлено!'}\n")

    # Знаходимо блок 90 днів тому (один раз для всіх)
    print("Визначаємо блок 90 днів тому...")
    block_90d = get_block_by_timestamp(start_ts)
    print(f"Блок {block_90d} ({start.strftime('%Y-%m-%d')})\n")
    time.sleep(0.3)

    results = []

    for i, address in enumerate(wallets, 1):
        print(f"[{i:>2}/{len(wallets)}] {address[:14]}...", end=" ", flush=True)

        # Баланс зараз і 90 днів тому
        balance_now = get_eth_balance(address)
        time.sleep(0.25)
        balance_90d = get_balance_at_block(address, block_90d) if block_90d else 0
        time.sleep(0.25)

        # Транзакції
        txs       = get_transactions(address, start_ts, end_ts)
        time.sleep(0.25)
        token_txs = get_token_transactions(address, start_ts, end_ts)
        time.sleep(0.25)

        metrics = analyze(address, txs, token_txs, balance_now, balance_90d)
        score, tier = score_wallet(metrics)

        print(
            f"score={score:>3} | "
            f"bal={balance_now:>8.1f} ETH | "
            f"Δdep={metrics['deposit_change_pct']:>+7.1f}% | "
            f"P&L={metrics['trading_pnl_pct']:>+7.1f}% | "
            f"txs={metrics['tx_count_90d']:>4}"
        )

        results.append({
            "rank":    0,
            "address": address,
            "score":   score,
            "tier":    tier,
            **metrics,
        })

    # Сортування і нумерація
    results.sort(key=lambda x: x["score"], reverse=True)
    for i, r in enumerate(results, 1):
        r["rank"] = i

    # CSV
    fieldnames = [
        "rank", "address", "score", "tier",
        "balance_now_eth", "balance_90d_ago_eth",
        "deposit_change_pct", "trading_pnl_pct", "trading_pnl_eth",
        "tx_count_90d", "success_tx_count", "success_rate_pct",
        "eth_volume_90d", "eth_sent_90d", "eth_received_90d",
        "token_volume_usd_90d", "unique_counterparts",
        "avg_tx_eth", "max_tx_eth",
    ]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n✅ Збережено в {OUTPUT_CSV}")
    print(f"\n{'#':<4} {'Адреса':<16} {'Score':>5} {'Tier':<16} {'Баланс':>10} {'Δ Депозит':>10} {'P&L угод':>9} {'Txs':>5}")
    print("─" * 85)
    for r in results[:10]:
        print(
            f"{r['rank']:<4} {r['address'][:14]}.. "
            f"{r['score']:>5} {r['tier']:<16} "
            f"{r['balance_now_eth']:>10.1f} "
            f"{r['deposit_change_pct']:>+9.1f}% "
            f"{r['trading_pnl_pct']:>+8.1f}% "
            f"{r['tx_count_90d']:>5}"
        )

if __name__ == "__main__":
    main()
