"""
database.py — зберігання гаманців та їх скору
"""
import sqlite3
import time
import logging
import os
from dataclasses import dataclass

log = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "whales.db")

# Мінімальний скор щоб потрапити в базу кандидатів для копітрейдингу
COPYTRADE_MIN_SCORE = int(os.getenv("COPYTRADE_MIN_SCORE", "50"))


@dataclass
class WalletProfile:
    address: str
    balance_eth: float
    tx_count: int
    score: int
    tier: str
    label: str
    first_seen: int
    last_seen: int
    total_alerts: int
    is_copytrade_candidate: bool


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Створює таблиці якщо не існують."""
    with get_conn() as conn:
        conn.executescript("""
            -- Профілі гаманців
            CREATE TABLE IF NOT EXISTS wallets (
                address             TEXT PRIMARY KEY,
                label               TEXT,
                balance_eth         REAL DEFAULT 0,
                tx_count            INTEGER DEFAULT 0,
                score               INTEGER DEFAULT 0,
                tier                TEXT DEFAULT 'unknown',
                first_seen          INTEGER,
                last_seen           INTEGER,
                total_alerts        INTEGER DEFAULT 0,
                is_copytrade_candidate INTEGER DEFAULT 0,
                cache_updated_at    INTEGER DEFAULT 0
            );

            -- Кожна угода яку ми зафіксували
            CREATE TABLE IF NOT EXISTS trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_hash     TEXT UNIQUE,
                from_addr   TEXT,
                to_addr     TEXT,
                value_eth   REAL,
                asset       TEXT,
                block_num   TEXT,
                score_at_time INTEGER,
                created_at  INTEGER
            );

            -- Індекси для швидкого пошуку
            CREATE INDEX IF NOT EXISTS idx_wallets_score ON wallets(score DESC);
            CREATE INDEX IF NOT EXISTS idx_wallets_copytrade ON wallets(is_copytrade_candidate);
            CREATE INDEX IF NOT EXISTS idx_trades_from ON trades(from_addr);
            CREATE INDEX IF NOT EXISTS idx_trades_created ON trades(created_at DESC);
        """)
    log.info(f"Database initialized: {DB_PATH}")


def score_wallet(balance_eth: float, tx_count: int) -> tuple[int, str]:
    """
    Розраховує скор 0-100 та тир гаманця.

    Логіка:
      Баланс    — скільки ETH тримає прямо зараз   (макс 50 балів)
      Активність — скільки транзакцій зробив всього (макс 30 балів)
      Бонус     — якщо обидва показники високі      (макс 20 балів)
    """
    # --- Баланс ---
    if balance_eth >= 10_000:   balance_score = 50
    elif balance_eth >= 1_000:  balance_score = 40
    elif balance_eth >= 500:    balance_score = 30
    elif balance_eth >= 100:    balance_score = 20
    elif balance_eth >= 10:     balance_score = 10
    else:                       balance_score = 0

    # --- Активність ---
    if tx_count >= 100_000:     activity_score = 30
    elif tx_count >= 10_000:    activity_score = 25
    elif tx_count >= 1_000:     activity_score = 15
    elif tx_count >= 100:       activity_score = 8
    else:                       activity_score = 0

    # --- Бонус: багатий І активний = справжній кит ---
    bonus = 0
    if balance_eth >= 100 and tx_count >= 1_000:
        bonus = 20
    elif balance_eth >= 10 and tx_count >= 100:
        bonus = 10

    score = min(balance_score + activity_score + bonus, 100)

    # --- Тир ---
    if score >= 80:   tier = "🐋 Mega Whale"
    elif score >= 60: tier = "🦈 Whale"
    elif score >= 40: tier = "🐬 Mid Whale"
    elif score >= 20: tier = "🐟 Small Fish"
    else:             tier = "🦐 Noise"

    return score, tier


def upsert_wallet(
    address: str,
    balance_eth: float,
    tx_count: int,
    label: str = "",
) -> WalletProfile:
    """
    Зберігає або оновлює профіль гаманця.
    Якщо скор >= COPYTRADE_MIN_SCORE — позначає як кандидата для копітрейдингу.
    """
    score, tier = score_wallet(balance_eth, tx_count)
    now = int(time.time())
    is_candidate = 1 if score >= COPYTRADE_MIN_SCORE else 0

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT first_seen, total_alerts FROM wallets WHERE address=?",
            (address.lower(),)
        ).fetchone()

        first_seen   = existing["first_seen"] if existing else now
        total_alerts = (existing["total_alerts"] + 1) if existing else 1

        conn.execute("""
            INSERT INTO wallets
                (address, label, balance_eth, tx_count, score, tier,
                 first_seen, last_seen, total_alerts,
                 is_copytrade_candidate, cache_updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(address) DO UPDATE SET
                label                  = excluded.label,
                balance_eth            = excluded.balance_eth,
                tx_count               = excluded.tx_count,
                score                  = excluded.score,
                tier                   = excluded.tier,
                last_seen              = excluded.last_seen,
                total_alerts           = excluded.total_alerts,
                is_copytrade_candidate = excluded.is_copytrade_candidate,
                cache_updated_at       = excluded.cache_updated_at
        """, (
            address.lower(), label, balance_eth, tx_count, score, tier,
            first_seen, now, total_alerts, is_candidate, now
        ))

    log.info(f"Wallet saved: {address[:10]}... score={score} tier={tier} candidate={bool(is_candidate)}")

    return WalletProfile(
        address=address,
        balance_eth=balance_eth,
        tx_count=tx_count,
        score=score,
        tier=tier,
        label=label,
        first_seen=first_seen,
        last_seen=now,
        total_alerts=total_alerts,
        is_copytrade_candidate=bool(is_candidate),
    )


def save_trade(
    tx_hash: str,
    from_addr: str,
    to_addr: str,
    value_eth: float,
    asset: str,
    block_num: str,
    score_at_time: int,
):
    """Зберігає угоду в history."""
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO trades
                (tx_hash, from_addr, to_addr, value_eth, asset, block_num, score_at_time, created_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            tx_hash, from_addr.lower(), to_addr.lower(),
            value_eth, asset, block_num, score_at_time, int(time.time())
        ))


def is_cache_fresh(address: str, ttl_seconds: int = 3600) -> bool:
    """Перевіряє чи є свіжий кеш для гаманця (за замовчуванням 1 год)."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT cache_updated_at FROM wallets WHERE address=?",
            (address.lower(),)
        ).fetchone()
        if not row:
            return False
        return (int(time.time()) - row["cache_updated_at"]) < ttl_seconds


def get_cached_profile(address: str) -> WalletProfile | None:
    """Повертає збережений профіль з БД."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM wallets WHERE address=?",
            (address.lower(),)
        ).fetchone()
        if not row:
            return None
        return WalletProfile(
            address=row["address"],
            balance_eth=row["balance_eth"],
            tx_count=row["tx_count"],
            score=row["score"],
            tier=row["tier"],
            label=row["label"] or "",
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
            total_alerts=row["total_alerts"],
            is_copytrade_candidate=bool(row["is_copytrade_candidate"]),
        )


def get_copytrade_candidates(limit: int = 50) -> list[dict]:
    """Повертає топ кандидатів для копітрейдингу."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT address, label, score, tier, balance_eth, tx_count,
                   total_alerts, first_seen, last_seen
            FROM wallets
            WHERE is_copytrade_candidate = 1
            ORDER BY score DESC, total_alerts DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_stats() -> dict:
    """Загальна статистика бази."""
    with get_conn() as conn:
        total     = conn.execute("SELECT COUNT(*) FROM wallets").fetchone()[0]
        candidates = conn.execute(
            "SELECT COUNT(*) FROM wallets WHERE is_copytrade_candidate=1"
        ).fetchone()[0]
        trades    = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        top_score = conn.execute(
            "SELECT MAX(score) FROM wallets"
        ).fetchone()[0] or 0
    return {
        "total_wallets": total,
        "copytrade_candidates": candidates,
        "total_trades_logged": trades,
        "top_score": top_score,
    }
