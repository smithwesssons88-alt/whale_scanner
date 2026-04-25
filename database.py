"""
database.py — зберігання гаманців та їх скору
"""
import sqlite3
import time
import logging
import os
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

DB_PATH            = os.getenv("DB_PATH", "whales.db")
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
    # Розширені метрики (з Etherscan)
    deposit_change_pct: float = 0.0
    trading_pnl_pct: float    = 0.0
    trading_pnl_eth: float    = 0.0
    tx_count_90d: int         = 0
    eth_volume_90d: float     = 0.0
    success_rate_pct: float   = 0.0
    deep_analyzed: bool       = False


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS wallets (
                address              TEXT PRIMARY KEY,
                label                TEXT,
                balance_eth          REAL DEFAULT 0,
                tx_count             INTEGER DEFAULT 0,
                score                INTEGER DEFAULT 0,
                tier                 TEXT DEFAULT 'unknown',
                first_seen           INTEGER,
                last_seen            INTEGER,
                total_alerts         INTEGER DEFAULT 0,
                is_copytrade_candidate INTEGER DEFAULT 0,
                cache_updated_at     INTEGER DEFAULT 0,
                -- Розширені метрики
                deep_analyzed        INTEGER DEFAULT 0,
                deposit_change_pct   REAL DEFAULT 0,
                trading_pnl_pct      REAL DEFAULT 0,
                trading_pnl_eth      REAL DEFAULT 0,
                tx_count_90d         INTEGER DEFAULT 0,
                eth_volume_90d       REAL DEFAULT 0,
                success_rate_pct     REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS trades (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_hash        TEXT UNIQUE,
                from_addr      TEXT,
                to_addr        TEXT,
                value_eth      REAL,
                asset          TEXT,
                block_num      TEXT,
                score_at_time  INTEGER,
                created_at     INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_wallets_score     ON wallets(score DESC);
            CREATE INDEX IF NOT EXISTS idx_wallets_copytrade ON wallets(is_copytrade_candidate);
            CREATE INDEX IF NOT EXISTS idx_trades_from       ON trades(from_addr);
            CREATE INDEX IF NOT EXISTS idx_trades_created    ON trades(created_at DESC);
        """)
        # Додаємо нові колонки якщо БД вже існує
        for col, default in [
            ("deep_analyzed",      "INTEGER DEFAULT 0"),
            ("deposit_change_pct", "REAL DEFAULT 0"),
            ("trading_pnl_pct",    "REAL DEFAULT 0"),
            ("trading_pnl_eth",    "REAL DEFAULT 0"),
            ("tx_count_90d",       "INTEGER DEFAULT 0"),
            ("eth_volume_90d",     "REAL DEFAULT 0"),
            ("success_rate_pct",   "REAL DEFAULT 0"),
        ]:
            try:
                conn.execute(f"ALTER TABLE wallets ADD COLUMN {col} {default}")
            except:
                pass
    log.info(f"Database initialized: {DB_PATH}")


def simple_score(balance_eth: float, tx_count: int) -> tuple[int, str]:
    """Базовий скор по балансу і tx_count."""
    if balance_eth >= 10_000:   b = 50
    elif balance_eth >= 1_000:  b = 40
    elif balance_eth >= 500:    b = 30
    elif balance_eth >= 100:    b = 20
    elif balance_eth >= 10:     b = 10
    else:                       b = 0

    if tx_count >= 100_000:     a = 30
    elif tx_count >= 10_000:    a = 25
    elif tx_count >= 1_000:     a = 15
    elif tx_count >= 100:       a = 8
    else:                       a = 0

    bonus = 0
    if balance_eth >= 100 and tx_count >= 1_000:  bonus = 20
    elif balance_eth >= 10 and tx_count >= 100:   bonus = 10

    score = min(b + a + bonus, 100)
    tier  = (
        "🐋 Mega Whale" if score >= 80 else
        "🦈 Whale"      if score >= 60 else
        "🐬 Mid Whale"  if score >= 40 else
        "🐟 Small Fish" if score >= 20 else
        "🦐 Noise"
    )
    return score, tier


def upsert_wallet(address: str, balance_eth: float, tx_count: int,
                  label: str = "", deep_metrics: dict = None) -> WalletProfile:
    """
    Зберігає профіль гаманця.
    Якщо передані deep_metrics — використовує розширений скор.
    """
    from etherscan import score_deep

    if deep_metrics:
        score, tier = score_deep(balance_eth, deep_metrics)
    else:
        score, tier = simple_score(balance_eth, tx_count)

    now          = int(time.time())
    is_candidate = 1 if score >= COPYTRADE_MIN_SCORE else 0

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT first_seen, total_alerts FROM wallets WHERE address=?",
            (address.lower(),)
        ).fetchone()

        first_seen   = existing["first_seen"] if existing else now
        total_alerts = (existing["total_alerts"] + 1) if existing else 1

        dm = deep_metrics or {}
        conn.execute("""
            INSERT INTO wallets
                (address, label, balance_eth, tx_count, score, tier,
                 first_seen, last_seen, total_alerts, is_copytrade_candidate,
                 cache_updated_at, deep_analyzed,
                 deposit_change_pct, trading_pnl_pct, trading_pnl_eth,
                 tx_count_90d, eth_volume_90d, success_rate_pct)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(address) DO UPDATE SET
                label                  = excluded.label,
                balance_eth            = excluded.balance_eth,
                tx_count               = excluded.tx_count,
                score                  = excluded.score,
                tier                   = excluded.tier,
                last_seen              = excluded.last_seen,
                total_alerts           = excluded.total_alerts,
                is_copytrade_candidate = excluded.is_copytrade_candidate,
                cache_updated_at       = excluded.cache_updated_at,
                deep_analyzed          = excluded.deep_analyzed,
                deposit_change_pct     = excluded.deposit_change_pct,
                trading_pnl_pct        = excluded.trading_pnl_pct,
                trading_pnl_eth        = excluded.trading_pnl_eth,
                tx_count_90d           = excluded.tx_count_90d,
                eth_volume_90d         = excluded.eth_volume_90d,
                success_rate_pct       = excluded.success_rate_pct
        """, (
            address.lower(), label, balance_eth, tx_count, score, tier,
            first_seen, now, total_alerts, is_candidate, now,
            1 if deep_metrics else 0,
            dm.get("deposit_change_pct", 0),
            dm.get("trading_pnl_pct", 0),
            dm.get("trading_pnl_eth", 0),
            dm.get("tx_count_90d", 0),
            dm.get("eth_volume_90d", 0),
            dm.get("success_rate_pct", 0),
        ))

    return WalletProfile(
        address=address, balance_eth=balance_eth, tx_count=tx_count,
        score=score, tier=tier, label=label,
        first_seen=first_seen, last_seen=now,
        total_alerts=total_alerts,
        is_copytrade_candidate=bool(is_candidate),
        deposit_change_pct=dm.get("deposit_change_pct", 0),
        trading_pnl_pct=dm.get("trading_pnl_pct", 0),
        trading_pnl_eth=dm.get("trading_pnl_eth", 0),
        tx_count_90d=dm.get("tx_count_90d", 0),
        eth_volume_90d=dm.get("eth_volume_90d", 0),
        success_rate_pct=dm.get("success_rate_pct", 0),
        deep_analyzed=bool(deep_metrics),
    )


def is_cache_fresh(address: str, ttl_seconds: int = 3600) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT cache_updated_at FROM wallets WHERE address=?",
            (address.lower(),)
        ).fetchone()
        if not row:
            return False
        return (int(time.time()) - row["cache_updated_at"]) < ttl_seconds


def needs_deep_analysis(address: str) -> bool:
    """Повертає True якщо гаманець ще не проходив глибокий аналіз."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT deep_analyzed FROM wallets WHERE address=?",
            (address.lower(),)
        ).fetchone()
        if not row:
            return True
        return not bool(row["deep_analyzed"])


def get_cached_profile(address: str) -> WalletProfile | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM wallets WHERE address=?",
            (address.lower(),)
        ).fetchone()
        if not row:
            return None
        return WalletProfile(
            address=row["address"], balance_eth=row["balance_eth"],
            tx_count=row["tx_count"], score=row["score"], tier=row["tier"],
            label=row["label"] or "", first_seen=row["first_seen"],
            last_seen=row["last_seen"], total_alerts=row["total_alerts"],
            is_copytrade_candidate=bool(row["is_copytrade_candidate"]),
            deposit_change_pct=row["deposit_change_pct"] or 0,
            trading_pnl_pct=row["trading_pnl_pct"] or 0,
            trading_pnl_eth=row["trading_pnl_eth"] or 0,
            tx_count_90d=row["tx_count_90d"] or 0,
            eth_volume_90d=row["eth_volume_90d"] or 0,
            success_rate_pct=row["success_rate_pct"] or 0,
            deep_analyzed=bool(row["deep_analyzed"]),
        )


def save_trade(tx_hash, from_addr, to_addr, value_eth, asset, block_num, score_at_time):
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO trades
                (tx_hash, from_addr, to_addr, value_eth, asset, block_num, score_at_time, created_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (tx_hash, from_addr.lower(), to_addr.lower(),
              value_eth, asset, block_num, score_at_time, int(time.time())))


def get_copytrade_candidates(limit=50):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT address, label, score, tier, balance_eth, tx_count,
                   total_alerts, deposit_change_pct, trading_pnl_pct,
                   eth_volume_90d, deep_analyzed
            FROM wallets
            WHERE is_copytrade_candidate = 1
            ORDER BY score DESC, total_alerts DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_stats():
    with get_conn() as conn:
        total      = conn.execute("SELECT COUNT(*) FROM wallets").fetchone()[0]
        candidates = conn.execute("SELECT COUNT(*) FROM wallets WHERE is_copytrade_candidate=1").fetchone()[0]
        deep       = conn.execute("SELECT COUNT(*) FROM wallets WHERE deep_analyzed=1").fetchone()[0]
        trades     = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        top_score  = conn.execute("SELECT MAX(score) FROM wallets").fetchone()[0] or 0
    return {
        "total_wallets":        total,
        "copytrade_candidates": candidates,
        "deep_analyzed":        deep,
        "total_trades_logged":  trades,
        "top_score":            top_score,
    }
