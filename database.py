import sqlite3
import time
import os

DB_PATH = os.environ.get('DB_PATH', 'whales.db')

def get_conn():
    return sqlite3.connect(DB_PATH)

def init_db():
    with get_conn() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS wallets (
                address TEXT PRIMARY KEY,
                balance_eth REAL,
                score INTEGER,
                tx_count_90d INTEGER,
                volume_90d REAL,
                first_seen INTEGER,
                last_seen INTEGER
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT,
                tx_hash TEXT UNIQUE,
                value_eth REAL,
                direction TEXT,
                timestamp INTEGER
            )
        ''')
        conn.commit()

def save_wallet(address, balance, score, stats):
    now = int(time.time())
    with get_conn() as conn:
        conn.execute('''
            INSERT INTO wallets (address, balance_eth, score, tx_count_90d, volume_90d, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET
                balance_eth=excluded.balance_eth,
                score=excluded.score,
                tx_count_90d=excluded.tx_count_90d,
                volume_90d=excluded.volume_90d,
                last_seen=excluded.last_seen
        ''', (address, balance, score, stats['tx_count_90d'], stats['volume_90d'], now, now))
        conn.commit()

def save_trade(address, tx_hash, value_eth, direction):
    with get_conn() as conn:
        try:
            conn.execute('''
                INSERT OR IGNORE INTO trades (address, tx_hash, value_eth, direction, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (address, tx_hash, value_eth, direction, int(time.time())))
            conn.commit()
        except Exception:
            pass

def get_stats():
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM wallets").fetchone()[0]
        deep = conn.execute("SELECT COUNT(*) FROM wallets WHERE score > 0").fetchone()[0]
        top = conn.execute("SELECT MAX(score) FROM wallets").fetchone()[0] or 0
        trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        candidates = conn.execute("SELECT COUNT(*) FROM wallets WHERE score >= 50").fetchone()[0]
        return {
            "total_wallets": total,
            "deep_analyzed": deep,
            "top_score": top,
            "total_trades_logged": trades,
            "copytrade_candidates": candidates
        }

def get_candidates(min_score=50):
    with get_conn() as conn:
        rows = conn.execute('''
            SELECT address, balance_eth, score, tx_count_90d, volume_90d, last_seen
            FROM wallets WHERE score >= ?
            ORDER BY score DESC LIMIT 50
        ''', (min_score,)).fetchall()
        return [
            {
                "address": r[0],
                "balance_eth": r[1],
                "score": r[2],
                "tx_count_90d": r[3],
                "volume_90d": r[4],
                "last_seen": r[5]
            }
            for r in rows
        ]
