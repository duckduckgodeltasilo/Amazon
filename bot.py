# 🛒 Amazon Stock Tracker — Stable Build

import logging
import re
import html
import math
import time
import random
import threading
import os
import sys
from urllib.parse import quote
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError

import httpx
from bs4 import BeautifulSoup
from psycopg2 import pool, OperationalError
from psycopg2.extras import DictCursor

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


from telegram import (
    Update, ParseMode,
    InlineKeyboardButton, InlineKeyboardMarkup
)
from telegram.ext import (
    Updater, CommandHandler, MessageHandler,
    Filters, CallbackContext, CallbackQueryHandler
)
from telegram.error import TelegramError, NetworkError, Conflict, TimedOut

from flask import Flask

# ═══════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════

BOT_TOKEN    = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
PORT         = int(os.environ.get("PORT", 8080))
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "")  # e.g. https://mybot.onrender.com
GROUP_CHAT_ID = os.environ.get("GROUP_CHAT_ID")  # e.g. -1001234567890 — sab alerts isi group mein jayenge
AFFILIATE_TAG = "7015105-21"

CHECK_INTERVAL_MIN   = 60
CHECK_INTERVAL_MAX   = 90
MAX_WORKERS          = 3
MANUAL_MAX_WORKERS   = 6   # manual /status — user is watching, worth the higher 503 risk
MAX_PRODUCTS_PER_USER = 20
PER_PRODUCT_TIMEOUT  = 15   # seconds — hard limit per product fetch

# ── Cloudflare Worker proxy (Amazon 503/IP-block bypass) ──────────────
# Render ke shared IP se seedha Amazon.in hit karne par IP flag/block ho
# jata hai — Worker Cloudflare edge IP se relay karta hai.
CF_PROXY_URL = os.environ.get("CF_PROXY_URL")   # e.g. https://xxxx.workers.dev
CF_PROXY_KEY = os.environ.get("CF_PROXY_KEY")   # Worker ke PROXY_KEY secret jaisa hi

if not BOT_TOKEN:
    print("❌ BOT_TOKEN not set!")
    sys.exit(1)
if not DATABASE_URL:
    print("❌ DATABASE_URL not set!")
    sys.exit(1)
if not GROUP_CHAT_ID:
    print("❌ GROUP_CHAT_ID not set!")
    sys.exit(1)
GROUP_CHAT_ID = int(GROUP_CHAT_ID)

# ═══════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════

logging.basicConfig(
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════
#  DATABASE MANAGER
# ═══════════════════════════════════════════════

class DatabaseManager:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._initialized = False
                cls._instance = inst
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._pool = None
        self._pool_lock = Lock()
        self._connect_with_retry()

    def _connect_with_retry(self, max_retries=10):
        for attempt in range(1, max_retries + 1):
            try:
                self._pool = pool.ThreadedConnectionPool(
                    minconn=2, maxconn=10,
                    dsn=DATABASE_URL,
                    cursor_factory=DictCursor,
                    connect_timeout=15,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                    options="-c statement_timeout=30000",
                )
                logger.info("✅ Database pool created")
                self._setup_schema()
                return
            except Exception as e:
                logger.error(f"DB connect attempt {attempt}/{max_retries}: {e}")
                if attempt == max_retries:
                    raise
                time.sleep(min(5 * attempt, 30))

    def _get_conn(self):
        with self._pool_lock:
            try:
                conn = self._pool.getconn()
                if conn.closed:
                    logger.warning("Dead connection — replacing")
                    try:
                        self._pool.putconn(conn, close=True)
                    except Exception:
                        pass
                    conn = self._pool.getconn()
                return conn
            except Exception:
                logger.warning("Pool unavailable — reconnecting…")
                self._connect_with_retry(max_retries=3)
                return self._pool.getconn()

    def _put_conn(self, conn, broken=False):
        with self._pool_lock:
            try:
                self._pool.putconn(conn, close=broken)
            except Exception:
                pass

    def execute(self, query, params=None, fetch_one=False, fetch_all=False):
        last_error = None
        for attempt in range(3):
            conn = None
            broken = False
            try:
                conn = self._get_conn()
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    conn.commit()
                    if fetch_one:
                        return cur.fetchone()
                    if fetch_all:
                        return cur.fetchall()
                    return True
            except OperationalError as e:
                broken = True
                last_error = e
                logger.warning(f"DB operational error (attempt {attempt+1}): {e}")
                if attempt == 2:
                    try:
                        self._connect_with_retry(max_retries=3)
                    except Exception:
                        pass
                time.sleep(min(2 ** attempt, 3))
            except Exception as e:
                last_error = e
                logger.error(f"DB error (attempt {attempt+1}): {e}")
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                time.sleep(1)
            finally:
                if conn:
                    self._put_conn(conn, broken=broken)

        logger.error(f"DB failed after retries: {last_error}")
        return [] if fetch_all else None

    def _setup_schema(self):
        self.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id   BIGINT PRIMARY KEY,
                chat_id   BIGINT NOT NULL,
                username  TEXT,
                joined_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        self.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id           SERIAL PRIMARY KEY,
                user_id      BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                asin         VARCHAR(10) NOT NULL,
                title        TEXT,
                url          TEXT,
                last_status  VARCHAR(20) DEFAULT 'UNKNOWN',
                last_checked TIMESTAMPTZ,
                added_at     TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(user_id, asin)
            );
        """)
        self.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        self.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_messages (
                id         SERIAL PRIMARY KEY,
                text       TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        for col, defn in [
            ("interval_minutes", "INTEGER DEFAULT 10"),
            ("last_sent",        "TIMESTAMPTZ"),
        ]:
            self._add_column_if_missing("broadcast_messages", col, defn)
        for col, defn in [
            ("username",              "TEXT"),
            ("joined_at",             "TIMESTAMPTZ DEFAULT NOW()"),
            ("added_at",              "TIMESTAMPTZ DEFAULT NOW()"),
            ("is_stopped",            "BOOLEAN DEFAULT FALSE"),
            ("instock_alert_count",   "INTEGER DEFAULT 5"),
            ("pricedrop_alert_count", "INTEGER DEFAULT 5"),
            ("alert_gap_seconds",     "INTEGER DEFAULT 1"),
        ]:
            self._add_column_if_missing("users", col, defn)

        for col, defn in [
            ("last_status",     "VARCHAR(20) DEFAULT 'UNKNOWN'"),
            ("last_checked",    "TIMESTAMPTZ"),
            ("added_at",        "TIMESTAMPTZ DEFAULT NOW()"),
            ("tracking_paused", "BOOLEAN DEFAULT FALSE"),
            ("alert_instock",   "BOOLEAN DEFAULT TRUE"),
            ("alert_pricedrop", "BOOLEAN DEFAULT TRUE"),
        ]:
            self._add_column_if_missing("products", col, defn)

        try:
            self.execute("ALTER TABLE products ALTER COLUMN last_price TYPE TEXT USING last_price::TEXT;")
        except Exception:
            pass
        self._add_column_if_missing("products", "last_price", "TEXT")
        self.execute("UPDATE users SET instock_alert_count=5 WHERE instock_alert_count=10;")
        self.execute("UPDATE users SET pricedrop_alert_count=5 WHERE pricedrop_alert_count=10;")
        logger.info("✅ Schema ready")

    def _add_column_if_missing(self, table, col, defn):
        exists = self.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s",
            (table, col), fetch_one=True
        )
        if not exists:
            self.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {defn};")
            logger.info(f"  ➕ Added {table}.{col}")

    def upsert_user(self, user_id, chat_id, username=None):
        self.execute("""
            INSERT INTO users (user_id, chat_id, username)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET chat_id  = EXCLUDED.chat_id,
                          username = COALESCE(EXCLUDED.username, users.username)
        """, (user_id, chat_id, username))

    def add_product(self, user_id, asin, title, url):
        return self.execute("""
            INSERT INTO products (user_id, asin, title, url, last_status)
            VALUES (%s, %s, %s, %s, 'UNKNOWN')
            ON CONFLICT (user_id, asin)
            DO UPDATE SET title = EXCLUDED.title, url = EXCLUDED.url
            RETURNING id
        """, (user_id, asin, title, url), fetch_one=True)

    def count_products(self, user_id):
        row = self.execute("SELECT COUNT(*) FROM products WHERE user_id=%s", (user_id,), fetch_one=True)
        return row[0] if row else 0

    def get_products(self, user_id):
        return self.execute("SELECT * FROM products WHERE user_id=%s ORDER BY id", (user_id,), fetch_all=True) or []

    def get_all_products_flat(self):
        return self.execute("SELECT * FROM products ORDER BY id", fetch_all=True) or []

    def get_setting(self, key, default=None):
        row = self.execute("SELECT value FROM settings WHERE key=%s", (key,), fetch_one=True)
        return row["value"] if row else default

    def set_setting(self, key, value):
        self.execute("""
            INSERT INTO settings (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (key, value))

    def add_broadcast_message(self, text, interval_minutes=10):
        return self.execute(
            "INSERT INTO broadcast_messages (text, interval_minutes) VALUES (%s, %s) RETURNING id",
            (text, interval_minutes), fetch_one=True
        )

    def get_broadcast_messages(self):
        return self.execute("SELECT * FROM broadcast_messages ORDER BY id", fetch_all=True) or []

    def get_due_broadcast_messages(self):
        return self.execute("""
            SELECT * FROM broadcast_messages
            WHERE last_sent IS NULL
               OR last_sent <= NOW() - (interval_minutes || ' minutes')::INTERVAL
            ORDER BY id
        """, fetch_all=True) or []

    def mark_broadcast_sent(self, msg_id):
        self.execute("UPDATE broadcast_messages SET last_sent = NOW() WHERE id=%s", (msg_id,))

    def set_broadcast_interval(self, msg_id, minutes):
        return self.execute(
            "UPDATE broadcast_messages SET interval_minutes=%s WHERE id=%s RETURNING id",
            (minutes, msg_id), fetch_one=True
        )

    def remove_broadcast_message(self, msg_id):
        return self.execute("DELETE FROM broadcast_messages WHERE id=%s", (msg_id,))

    def get_all_products_with_users(self):
        return self.execute("""
            SELECT p.*, u.chat_id, u.user_id
            FROM products p
            JOIN users u ON u.user_id = p.user_id
            WHERE u.is_stopped = FALSE AND p.tracking_paused = FALSE
            ORDER BY p.id
        """, fetch_all=True) or []

    def update_status(self, product_id, status, price=None):
        # Bug fix: RETURNING subquery updated row return karta hai, purani nahi
        # Isliye pehle old values read karo, phir update karo
        old_row = self.execute(
            "SELECT last_status, last_price FROM products WHERE id = %s",
            (product_id,), fetch_one=True
        )
        self.execute("""
            UPDATE products
            SET last_status  = %s,
                last_checked = NOW(),
                last_price   = COALESCE(%s, last_price)
            WHERE id = %s
        """, (status, price, product_id))
        # old_row mein last_status aur last_price hain UPDATE se PEHLE ke
        return old_row

    def remove_product(self, product_id, user_id):
        return self.execute("DELETE FROM products WHERE id=%s AND user_id=%s", (product_id, user_id))

    def remove_all_products(self, user_id):
        return self.execute("DELETE FROM products WHERE user_id=%s", (user_id,))

    def update_title(self, product_id, title):
        return self.execute("UPDATE products SET title=%s WHERE id=%s", (title, product_id))

    def set_user_stopped(self, user_id, stopped: bool):
        self.execute("UPDATE users SET is_stopped=%s WHERE user_id=%s", (stopped, user_id))

    def is_user_stopped(self, user_id):
        row = self.execute("SELECT is_stopped FROM users WHERE user_id=%s", (user_id,), fetch_one=True)
        return bool(row["is_stopped"]) if row else False

    def toggle_product_pause(self, product_id, user_id):
        row = self.execute(
            "SELECT tracking_paused FROM products WHERE id=%s AND user_id=%s",
            (product_id, user_id), fetch_one=True
        )
        if not row:
            return None
        new_state = not row["tracking_paused"]
        self.execute("UPDATE products SET tracking_paused=%s WHERE id=%s AND user_id=%s", (new_state, product_id, user_id))
        return new_state


    def get_alert_settings(self, user_id):
        row = self.execute(
            "SELECT instock_alert_count, pricedrop_alert_count, alert_gap_seconds FROM users WHERE user_id=%s",
            (user_id,), fetch_one=True
        )
        if not row:
            return {"instock_alert_count": 5, "pricedrop_alert_count": 5, "alert_gap_seconds": 2}
        return {
            "instock_alert_count":   row["instock_alert_count"]  or 5,
            "pricedrop_alert_count": row["pricedrop_alert_count"] or 5,
            "alert_gap_seconds":     row["alert_gap_seconds"] if row["alert_gap_seconds"] is not None else 2,
        }



# ═══════════════════════════════════════════════
#  AMAZON SCRAPER  (httpx — guaranteed timeouts)
# ═══════════════════════════════════════════════

class AmazonScraper:

    USER_AGENTS = [
        # Chrome Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        # Chrome Mac
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        # Chrome Linux
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        # Firefox Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        # Firefox Mac
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0",
        # Firefox Linux
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
        # Safari Mac
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
        # Edge
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.2210.133",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
        # Opera
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/108.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/108.0.0.0",
    ]

    MOBILE_USER_AGENTS = [
        # iPhone Safari
        "Mozilla/5.0 (iPhone; CPU iPhone OS 15_8 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
        # iPhone Chrome
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/122.0.6261.89 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/120.0.6099.119 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/119.0.6045.169 Mobile/15E148 Safari/604.1",
        # iPhone Firefox
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/122.0 Mobile/15E148 Safari/605.1.15",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/123.0 Mobile/15E148 Safari/605.1.15",
        # Android Chrome
        "Mozilla/5.0 (Linux; Android 11; Redmi Note 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.134 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 12; Redmi 10C) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.178 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 12; POCO X4 Pro 5G) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.90 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; Redmi Note 12) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.90 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; SM-A546B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.90 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.90 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; Pixel 6a) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.90 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; vivo V27) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.178 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.90 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.90 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; OnePlus 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.90 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; SM-S711B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.90 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; motorola edge 40) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.90 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; RMX3630) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.230 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 12; V2111) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.66 Mobile Safari/537.36",
        # Android Firefox
        "Mozilla/5.0 (Android 12; Mobile; rv:120.0) Gecko/120.0 Firefox/120.0",
        "Mozilla/5.0 (Android 13; Mobile; rv:121.0) Gecko/121.0 Firefox/121.0",
        "Mozilla/5.0 (Android 14; Mobile; rv:122.0) Gecko/122.0 Firefox/122.0",
        "Mozilla/5.0 (Android 14; Mobile; rv:124.0) Gecko/124.0 Firefox/124.0",
        # iPad
        "Mozilla/5.0 (iPad; CPU OS 15_8 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPad; CPU OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/122.0.6261.89 Mobile/15E148 Safari/604.1",
    ]

    ASIN_PATTERNS = [
        r"/dp/([A-Z0-9]{10})",
        r"/gp/product/([A-Z0-9]{10})",
        r"(?:^|/)([A-Z0-9]{10})(?:[/?]|$)",
    ]

    @staticmethod
    def _resolve_short_url(url: str) -> str:
        """amzn.to / amzn.in short links ko actual Amazon URL mein resolve karo."""
        short_domains = ("amzn.to", "amzn.in", "amzn.eu")
        if not any(d in url for d in short_domains):
            return url
        try:
            timeout = httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                resp = client.get(url, headers={"User-Agent": random.choice(AmazonScraper.USER_AGENTS)})
                return str(resp.url)
        except Exception as e:
            logger.warning(f"Short URL resolve failed: {e}")
            return url

    @staticmethod
    def extract_asin(text: str) -> str | None:
        # Short link hai toh pehle resolve karo
        text = text.strip()
        if any(d in text for d in ("amzn.to", "amzn.in", "amzn.eu")):
            text = AmazonScraper._resolve_short_url(text)
            logger.info(f"Short URL resolved to: {text}")

        text_upper = text.upper()
        for pat in AmazonScraper.ASIN_PATTERNS:
            m = re.search(pat, text_upper)
            if m:
                return m.group(1)
        return None

    @staticmethod
    def _build_headers(mobile=False):
        ua_pool = AmazonScraper.MOBILE_USER_AGENTS if mobile else AmazonScraper.USER_AGENTS
        headers = {
            "User-Agent":              random.choice(ua_pool),
            "Accept":                  "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language":         "en-US,en;q=0.9",
            "Accept-Encoding":         "gzip, deflate, br",
            "DNT":                     "1",
            "Connection":              "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control":           "no-cache",
            "Pragma":                  "no-cache",
            "Sec-Fetch-Dest":          "document",
            "Sec-Fetch-Mode":          "navigate",
            "Sec-Fetch-Site":          "none",
            "Sec-Fetch-User":          "?1",
            "Service-Worker-Navigation-Preload": "true",
            "sec-ch-ua":               '"Not)A;Brand";v="24", "Chromium";v="116"',
            "sec-ch-ua-mobile":        "?1" if mobile else "?0",
            "sec-ch-ua-platform":      '"Android"' if mobile else '"Windows"',
            "device-memory":           "8",
            "sec-ch-device-memory":    "8",
            "dpr":                     "2.75" if mobile else "1",
            "sec-ch-dpr":              "2.75" if mobile else "1",
            "viewport-width":          "980" if mobile else "1920",
            "sec-ch-viewport-width":   "980" if mobile else "1920",
            "rtt":                     "100",
            "downlink":                "2.4",
            "ect":                     "4g",
        }
        if mobile:
            headers["X-Requested-With"] = "XMLHttpRequest"
        return headers

    @staticmethod
    def _mobile_url(url: str) -> str:
        return url.replace("www.amazon.in", "m.amazon.in")

    # ── Shared, cookie-persistent client ────────────────────────────
    # Real browser session ke jaisa: cookies (session-id, ubid-acbin,
    # sso-*) requests ke beech carry hote hain, har call pe fresh
    # client nahi banta — Amazon ke liye ye "returning session" jaisa
    # dikhta hai instead of a cold anonymous hit har baar.
    _session_client = None
    _session_lock = Lock()

    # ── Akamai Bot Manager cookies (ak_bmsc, bm_sv) ──────────────────
    # Ye cookies plain HTTP se kabhi nahi milte — Amazon ke JS sensor
    # script (Akamai) browser mein chalke generate karta hai. Bina
    # inke AOD ajax endpoint 503 deta hai chahe headers kitne bhi
    # perfect ho. Headless Playwright se periodically ek baar "warm-up"
    # karke ye cookies nikalte hain aur httpx session mein daal dete hain.
    _cookie_refresh_lock = Lock()
    _last_cookie_refresh  = 0.0
    _COOKIE_REFRESH_TTL   = 2.5 * 3600  # 2.5 ghante — background thread isi TTL par proactively refresh karta hai

    @staticmethod
    def _refresh_akamai_cookies(asin: str = None) -> bool:
        """
        Headless Chromium se amazon.in ka dp page kholte hain, JS
        (Akamai sensor) chalne dete hain, phir jo cookies generate
        hote hain unhe shared httpx session mein copy kar dete hain.
        Return: True agar refresh successful, False agar fail/unavailable.
        """
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("Playwright not installed — Akamai cookie refresh skip ho raha hai.")
            return False

        with AmazonScraper._cookie_refresh_lock:
            # Double-check: kisi doosre thread ne abhi-abhi refresh to nahi kar diya
            if time.time() - AmazonScraper._last_cookie_refresh < AmazonScraper._COOKIE_REFRESH_TTL:
                return True

            target_asin = asin or "B0DGJ8QKSF"
            url = f"https://www.amazon.in/dp/{target_asin}?aod=1"
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
                    context = browser.new_context(
                        user_agent=random.choice(AmazonScraper.MOBILE_USER_AGENTS),
                        viewport={"width": 412, "height": 915},
                        locale="en-IN",
                    )
                    page = context.new_page()
                    page.goto(url, timeout=25000, wait_until="domcontentloaded")
                    # Akamai sensor ko chalne ka time do
                    page.wait_for_timeout(4000)

                    cookies = context.cookies()
                    client = AmazonScraper._get_session_client()
                    copied = 0
                    for c in cookies:
                        if "amazon.in" not in c.get("domain", ""):
                            continue
                        client.cookies.set(c["name"], c["value"], domain=".amazon.in")
                        copied += 1

                    browser.close()
                    AmazonScraper._last_cookie_refresh = time.time()
                    logger.info(f"🍪 Akamai cookie refresh done — {copied} cookies copied.")
                    return copied > 0
            except Exception as e:
                logger.error(f"Akamai cookie refresh failed: {e}")
                return False

    @staticmethod
    def _get_session_client():
        if AmazonScraper._session_client is None:
            with AmazonScraper._session_lock:
                if AmazonScraper._session_client is None:
                    timeout = httpx.Timeout(connect=4.0, read=8.0, write=4.0, pool=4.0)
                    AmazonScraper._session_client = httpx.Client(
                        timeout=timeout, follow_redirects=True
                    )
        return AmazonScraper._session_client

    @staticmethod
    def _store_proxy_cookies(resp, client):
        """
        Proxy(worker) ke response mein Amazon ke Set-Cookie headers aate
        hain lekin response ka origin worker-domain hota hai, isliye httpx
        unhe khud-ba-khud amazon.in domain ke against store nahi karega.
        Manually extract karke client cookie jar mein daalte hain.
        """
        if client is None:
            return
        try:
            for name, value in resp.cookies.items():
                client.cookies.set(name, value, domain=".amazon.in")
        except Exception:
            pass

    @staticmethod
    def _proxied_request(target_url: str, headers: dict):
        """
        Agar CF_PROXY_URL configured hai to request Cloudflare Worker ke
        through jaati hai (Worker Amazon.in ko apni edge IP se hit karta
        hai), warna direct Amazon.in ko hit karte hain (fallback).
        NOTE: proxy ka domain Amazon.in se alag hai, isliye httpx apne
        aap Amazon cookies (ak_bmsc, session-id, etc.) attach nahi karega —
        yahan explicitly Cookie header banake bhejte hain taaki worker
        unhe aage Amazon tak forward kar sake.
        Returns: (actual_request_url, headers_to_send)
        """
        if CF_PROXY_URL:
            proxy_headers = dict(headers)
            if CF_PROXY_KEY:
                proxy_headers["X-Proxy-Key"] = CF_PROXY_KEY
            client = AmazonScraper._session_client
            if client is not None and client.cookies:
                cookie_str = "; ".join(f"{k}={v}" for k, v in client.cookies.items())
                if cookie_str:
                    proxy_headers["Cookie"] = cookie_str
            sep = "&" if "?" in CF_PROXY_URL else "?"
            request_url = f"{CF_PROXY_URL}{sep}url={quote(target_url, safe='')}"
            return request_url, proxy_headers
        return target_url, headers

    @staticmethod
    def fetch_page(url: str, retries=3) -> str | None:
        """
        httpx use karta hai — guaranteed hard timeout at OS level.
        requests.get OS-level hang karta hai, httpx nahi.
        """
        urls_to_try = [url, AmazonScraper._mobile_url(url), url]
        # httpx.Timeout: connect=5s, read=12s, write=5s, pool=5s
        timeout = httpx.Timeout(connect=4.0, read=8.0, write=4.0, pool=4.0)

        for attempt in range(retries):
            use_mobile = (attempt % 2 == 1)
            fetch_url  = urls_to_try[attempt]
            try:
                if attempt > 0:
                    time.sleep(random.uniform(1, 3))

                client = AmazonScraper._get_session_client()
                req_headers = AmazonScraper._build_headers(mobile=use_mobile)
                req_url, req_headers = AmazonScraper._proxied_request(fetch_url, req_headers)
                resp = client.get(req_url, headers=req_headers, timeout=timeout)
                if CF_PROXY_URL:
                    AmazonScraper._store_proxy_cookies(resp, client)

                if resp.status_code == 200:
                    page = resp.text
                    if any(kw in page[:3000].lower() for kw in ["robot check", "automated access", "captcha"]):
                        logger.warning(f"Bot check (attempt {attempt+1}) — switching URL")
                        time.sleep(random.uniform(2, 4))
                        continue
                    return page
                elif resp.status_code in (503, 429):
                    wait = random.uniform(2, 4) * (attempt + 1)
                    logger.warning(f"Rate limited ({resp.status_code}) — waiting {wait:.0f}s")
                    time.sleep(wait)
                else:
                    logger.warning(f"HTTP {resp.status_code} on attempt {attempt+1}")
                    time.sleep(random.uniform(1, 2))

            except httpx.TimeoutException:
                logger.warning(f"httpx Timeout (attempt {attempt+1}): {fetch_url}")
                time.sleep(random.uniform(2, 4))
            except httpx.ConnectError as e:
                logger.warning(f"httpx ConnectError: {e}")
                time.sleep(random.uniform(2, 4))
            except Exception as e:
                logger.error(f"fetch_page unexpected error: {e}")
                time.sleep(1)

        return None

    @staticmethod
    def _parse_price(soup):
        try:
            for pid in ['priceblock_ourprice', 'priceblock_dealprice']:
                tag = soup.find(id=pid)
                if tag:
                    return tag.get_text(strip=True)
            whole = soup.find('span', class_='a-price-whole')
            frac  = soup.find('span', class_='a-price-fraction')
            if whole:
                w = whole.get_text(strip=True).replace(',', '').rstrip('.')
                f = frac.get_text(strip=True) if frac else '00'
                return f'₹{w}.{f}'
            core = soup.find(id='corePriceDisplay_desktop_feature_div')
            if core:
                ps = core.find('span', class_='a-offscreen')
                if ps:
                    return ps.get_text(strip=True)
        except Exception:
            pass
        return None

    # Known colors — title mein mile toh aage laao
    KNOWN_COLORS = [
        'Cosmic Orange', 'Desert Titanium', 'White Titanium', 'Black Titanium',
        'Natural Titanium', 'Blue Titanium', 'Sage Green', 'Cloud White', 'Jet Black',
        'Ultramarine', 'Pebble',
        'Starlight', 'Midnight', 'Natural', 'Titanium',
        'Glacier', 'Burgundy',
        'Black', 'White', 'Blue', 'Red', 'Green', 'Gold', 'Silver',
        'Purple', 'Yellow', 'Pink', 'Orange', 'Teal', 'Coral',
        'Lavender', 'Mint', 'Sage', 'Sky', 'Storm', 'Olive',
    ]

    @staticmethod
    def _reorder_color(title: str) -> str:
        """Title mein color mile toh aage lao, warna as-is return karo."""
        for color in AmazonScraper.KNOWN_COLORS:
            pattern = re.compile(re.escape(color), re.IGNORECASE)
            if pattern.search(title):
                # Color nikaalo title se (aas paas ke separators bhi)
                cleaned = pattern.sub('', title)
                cleaned = re.sub(r'[\s,;:\-|]+$', '', cleaned.strip())
                cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip()
                return f'{color} | {cleaned}'
        return title

    @staticmethod
    def fetch_product_info(asin: str, url: str = None, max_attempts: int = 3) -> dict:
        """
        AOD ajax (aodAjaxMain) hi use karta hai — /dp/ page nahi, kyunki
        multi-variant products (color/size) pe Amazon OOS variant ko
        silently kisi sibling in-stock variant se swap kar deta hai.
        AOD explicitly ?asin= param se us exact variant ko lock karta
        hai, koi swap nahi hota — yahi iski sabse badi wajah hai.
        503 fix session-persistent client + full browser-like headers
        se hua hai, endpoint change se nahi.
        """
        if not url:
            url = f'https://www.amazon.in/dp/{asin}'
        aod_url = f'https://www.amazon.in/gp/product/ajax/aodAjaxMain/ref=auto_load_aod?asin={asin}&pc=dp'

        title  = f'Product {asin}'
        status = 'UNKNOWN'
        price  = None

        client  = AmazonScraper._get_session_client()
        timeout = httpx.Timeout(connect=4.0, read=8.0, write=4.0, pool=4.0)
        ajax_headers = {
            'X-Requested-With':   'XMLHttpRequest',
            'Accept':             'text/html,*/*',
            'Accept-Language':    'en-IN,en;q=0.9',
            'Referer':            f'https://www.amazon.in/dp/{asin}',  # tag-free — sirf display url mein tag hai, fetch mein kahin nahi
            'User-Agent':         random.choice(AmazonScraper.USER_AGENTS),
            'Sec-Fetch-Site':     'same-origin',
            'Sec-Fetch-Mode':     'cors',
            'Sec-Fetch-Dest':     'empty',
            'sec-ch-ua':          '"Not)A;Brand";v="24", "Chromium";v="116"',
            'sec-ch-ua-mobile':   '?1',
            'sec-ch-ua-platform': '"Android"',
        }
        # Cookie refresh ab background daemon thread (_cookie_refresh_thread)
        # proactively har ~2.5hr karta hai — live path se hataya hai taaki
        # fetch fast rahe. Sirf cold-start (bot abhi-abhi start hua, background
        # thread ne pehla refresh nahi kiya) ke liye bootstrap fallback.
        if PLAYWRIGHT_AVAILABLE and AmazonScraper._last_cookie_refresh == 0.0:
            AmazonScraper._refresh_akamai_cookies(asin)

        resp = None
        try:
            for attempt in range(max_attempts):
                ajax_headers['User-Agent'] = random.choice(AmazonScraper.USER_AGENTS)
                req_url, req_headers = AmazonScraper._proxied_request(aod_url, ajax_headers)
                resp = client.get(req_url, headers=req_headers, timeout=timeout)
                if CF_PROXY_URL:
                    AmazonScraper._store_proxy_cookies(resp, client)
                if resp.status_code != 503:
                    break
                wait = random.uniform(2, 4) * (attempt + 1)
                logger.warning(f'503 on {asin} — attempt {attempt+1}/{max_attempts}, retry in {wait:.1f}s')
                time.sleep(wait)

            # Sabhi attempts 503 — is product ko is cycle mein simply skip
            # karo (UNKNOWN rahega, purana status preserve hoga). Cookie
            # refresh yahan se trigger nahi karte — sirf background thread
            # (_cookie_refresh_thread, ~2.5hr proactive) hi refresh karega.
            if resp is not None and resp.status_code == 503:
                logger.warning(f'All attempts 503 on {asin} — skipping this cycle (no cookie refresh triggered).')

            if resp is not None and resp.status_code == 200:
                ajax_html = resp.text
                soup      = BeautifulSoup(ajax_html, 'lxml')
                pt        = soup.get_text(' ', strip=True).lower()

                # ── Title ────────────────────────────────────────────
                title_tag = (
                    soup.find('span', id='aod-asin-title') or
                    soup.find('span', {'class': 'a-size-medium a-color-base a-text-bold'}) or
                    soup.find('h5') or
                    soup.find('h2')
                )
                if title_tag:
                    raw_title = html.unescape(title_tag.get_text(strip=True))
                    if raw_title:
                        title = AmazonScraper._reorder_color(raw_title)
                if title == f'Product {asin}' and soup.title:
                    raw_title = re.sub(r'\s*[-|:]\s*Amazon.*$', '', soup.title.get_text(), flags=re.IGNORECASE)
                    raw_title = html.unescape(raw_title.strip())
                    if raw_title:
                        title = AmazonScraper._reorder_color(raw_title)

                # ── Price — sirf actual price element se ─────────────
                price_tag = soup.find('span', class_='a-offscreen')
                if price_tag:
                    pt_price = price_tag.get_text(strip=True)
                    if '₹' in pt_price:
                        price = pt_price
                if not price:
                    whole = soup.find('span', class_='a-price-whole')
                    frac  = soup.find('span', class_='a-price-fraction')
                    if whole:
                        w = whole.get_text(strip=True).replace(',', '').rstrip('.')
                        f = frac.get_text(strip=True) if frac else '00'
                        price = f'₹{w}.{f}'

                # ── Stock detection ────────────────────────────────────
                has_price  = price is not None
                has_seller = any(s in pt for s in ['sold by', 'ships from', 'seller rating'])
                has_cart   = 'add to cart' in pt

                # ── Delivery-promise check ──────────────────────────────
                # AOD ka DELIVERY_BLOCK slot batata hai ki offer us address
                # pe genuinely deliverable hai ya nahi — checkout tak jaane
                # se pehle hi pata chal jata hai. NO_PROMISE_UPSELL_MESSAGE
                # slot = koi delivery date commit nahi hui (checkout pe
                # "unavailable at your address" milta hai); PRIMARY_DELIVERY_
                # MESSAGE_LARGE/SMALL ke saath data-csa-c-delivery-time =
                # real deliverable date hai.
                has_no_delivery_promise = 'NO_PROMISE_UPSELL_MESSAGE' in ajax_html
                has_delivery_date = bool(re.search(r'data-csa-c-delivery-time="[^"]+"', ajax_html))

                status = 'IN_STOCK' if (has_price or has_seller or has_cart) else 'OUT_OF_STOCK'

                if status == 'IN_STOCK' and has_no_delivery_promise and not has_delivery_date:
                    status = 'OUT_OF_STOCK'

                if status == 'OUT_OF_STOCK':
                    price = None   # OOS pe price bhi drop karo, generic/stale price save mat karo

                logger.info(
                    f'[{asin}] status={status} price={price} '
                    f'has_price={has_price} has_seller={has_seller} has_cart={has_cart} '
                    f'no_delivery_promise={has_no_delivery_promise} has_delivery_date={has_delivery_date}'
                )

            elif resp is not None:
                logger.warning(f'AOD fetch HTTP {resp.status_code} for {asin} (after retries)')

        except Exception as e:
            logger.warning(f'AJAX fetch error {asin}: {e}')

        return {'title': title, 'url': aod_url, 'status': status, 'price': price, 'asin': asin}




def status_emoji(s: str, paused: bool = False) -> str:
    if paused:
        return "⏯️"
    return {"IN_STOCK": "✅", "OUT_OF_STOCK": "❌"}.get(s, "⚪")

def short_title(title: str, n=55) -> str:
    return title[:n] + "…" if len(title) > n else title

def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add",        callback_data="cmd_add"),
         InlineKeyboardButton("📋 My List",    callback_data="cmd_list")],
        [InlineKeyboardButton("⏳ Status",      callback_data="cmd_status"),
         InlineKeyboardButton("🗑 Remove",      callback_data="cmd_remove")],
        [InlineKeyboardButton("⏸ Pause",       callback_data="cmd_pause"),
         InlineKeyboardButton("🛑 Stop",        callback_data="cmd_stop")],
    ])


# ═══════════════════════════════════════════════
#  BOT HANDLERS
# ═══════════════════════════════════════════════

db = DatabaseManager()

# ── Scheduled check lock ──────────────────────────────────────────────
_check_lock     = threading.Lock()
_check_deadline = None   # dynamic per-run
_last_full_check = 0.0   # timestamp of last actual Amazon fetch

# ── Failed-product tail queue ────────────────────────────────────────────
# Jo product 503/UNKNOWN de, use agle 5 cycles tak list ke SABSE LAST
# mein check karo (priority nahi, penalty). Naya fail hua product sabse
# last pe jayega, purane fails ek-ek position peeche khisak jayenge.
# dict: asin -> cycles_remaining
_tail_penalty_asins = {}
_TAIL_PENALTY_CYCLES = 5

# ── 503 circuit breaker ────────────────────────────────────────────────
# Agar ek cycle mein zyada 503 aaye, Amazon abhi block-mode mein hai —
# lagatar retry karna aur bhi zyada flag karega. Isliye backoff badhao
# aur agla cycle extra der tak skip karo, jab tak 503s kam na hon.
_consecutive_bad_cycles = 0   # kitne cycles mein 503-rate high raha
_extra_cooldown_until   = 0.0  # is timestamp tak koi scheduled check nahi

# ── Manual /status override ───────────────────────────────────────────
# Jab user manually /status chalata hai, auto (scheduled) check ko is
# cycle ke liye skip/abort kar dete hain taaki dono ek saath Amazon ko
# hit na karein (overlap => hang jaisa behaviour, especially many products par)
_manual_status_event = threading.Event()

def _watchdog_thread():
    """Har 5s check: deadline cross hoti hai toh lock force-release."""
    global _check_deadline
    while True:
        time.sleep(5)
        try:
            if _check_deadline is not None and time.time() > _check_deadline:
                logger.error("🚨 Watchdog: stock check hung — force releasing lock!")
                _check_deadline = None
                try:
                    _check_lock.release()
                except RuntimeError:
                    pass
        except Exception as e:
            logger.error(f"Watchdog error: {e}")


def _cookie_refresh_thread():
    """Background daemon — har ~2.5hr (AmazonScraper._COOKIE_REFRESH_TTL)
    proactively Akamai cookies refresh karta hai, taaki live fetch path
    (fetch_product_info) ko yeh wait/launch overhead na uthana pade.
    """
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright not installed — cookie refresh background thread skip.")
        return
    while True:
        try:
            AmazonScraper._refresh_akamai_cookies()
        except Exception as e:
            logger.error(f"Background cookie refresh error: {e}")
        time.sleep(AmazonScraper._COOKIE_REFRESH_TTL)


def id_cmd(update: Update, context: CallbackContext):
    update.message.reply_text(f"Chat ID: `{update.effective_chat.id}`", parse_mode=ParseMode.MARKDOWN)


def start(update: Update, context: CallbackContext):
    user = update.effective_user
    try:
        db.upsert_user(user.id, update.effective_chat.id, user.username)
        db.set_user_stopped(user.id, False)
        update.message.reply_text(
            "Hey\n\n"
            "Track any Amazon.in product and get instant alerts the moment it comes back in stock.\n\n"
            "What would you like to do?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_menu_keyboard()
        )
    except Exception as e:
        logger.error(f"start error: {e}")


def stop_cmd(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    target  = update.message or (update.callback_query and update.callback_query.message)
    if not target:
        return
    try:
        db.set_user_stopped(user_id, True)
        target.reply_text(
            "🛑 *All tracking stopped.*\n\n"
            "No more stock checks or alerts will be sent.\n\n"
            "Send /start anytime to resume tracking.",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"stop_cmd error: {e}")


def pause_cmd(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    target  = update.message or (update.callback_query and update.callback_query.message)
    if not target:
        return
    try:
        products = db.get_products(user_id)
        if not products:
            target.reply_text("🗂️ *No products to pause/resume.*", parse_mode=ParseMode.MARKDOWN)
            return
        context.user_data["pause_list"] = products
        context.user_data.pop("awaiting_link", None)
        keyboard = []
        for i, p in enumerate(products, 1):
            paused = p.get("tracking_paused", False)
            se     = status_emoji(p.get("last_status", "UNKNOWN"), paused=paused)
            label  = f"{i}.  {se}  {short_title(p['title'], 25)}  [{'PAUSED' if paused else 'TRACKING'}]"
            keyboard.append([InlineKeyboardButton(label, callback_data=f"pause_{p['id']}")])
        keyboard.append([InlineKeyboardButton("Cancel", callback_data="rm_cancel")])
        target.reply_text(
            "⏯️ *Tap a product to toggle tracking ON/OFF:*\n\n"
            "⏸ = Paused (no alerts)  |  🟢🔴⚪ = Active",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"pause_cmd error: {e}")




def list_products(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    try:
        products = db.get_products(user_id)
        target   = update.message or update.callback_query.message
        if not products:
            target.reply_text(
                "🗂️ *No products tracked yet.*\n\nSend /add to start tracking!",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=main_menu_keyboard()
            )
            return
        lines = ["📦 *Your Tracked Products:*\n"]
        for i, p in enumerate(products, 1):
            paused = p.get("tracking_paused", False)
            se     = status_emoji(p.get("last_status", "UNKNOWN"), paused=paused)
            lines.append(f"`{i}.` {se} [{short_title(p['title'])}]({p['url']})\n")
        lines.append(f"_Total: {len(products)}/{MAX_PRODUCTS_PER_USER}_")
        target.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
            reply_markup=main_menu_keyboard()
        )
    except Exception as e:
        logger.error(f"list error: {e}")


def status_check(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    target  = update.message or (update.callback_query and update.callback_query.message)

    # Auto (scheduled) check ko is cycle ke liye skip/abort karwa do —
    # taaki manual /status aur background auto-check ek saath Amazon
    # na hit karein (overlap hi hang ki asli wajah thi).
    _manual_status_event.set()
    try:
        products = db.get_products(user_id)
        if not products:
            target.reply_text("🗂️ *No products tracked.*", parse_mode=ParseMode.MARKDOWN)
            return
        wait_msg = target.reply_text(f"⏳ Checking {len(products)} product(s)… please wait.", parse_mode=ParseMode.MARKDOWN)
        results  = []

        def _check(p):
            time.sleep(random.uniform(0, 0.6))   # small fixed jitter, NOT scaled by index
            try:
                info = AmazonScraper.fetch_product_info(p["asin"], p.get("url"))
                db.update_status(p["id"], info["status"], info.get("price"))
                return p, info["status"], info.get("price")
            except Exception as e:
                logger.error(f"Status check error: {e}")
                return p, p.get("last_status", "UNKNOWN"), p.get("last_price")

        with ThreadPoolExecutor(max_workers=min(MANUAL_MAX_WORKERS, len(products))) as ex:
            futures = [ex.submit(_check, p) for p in products]
            for fut in as_completed(futures, timeout=PER_PRODUCT_TIMEOUT + 20):
                results.append(fut.result())

        id_order = {p["id"]: i for i, p in enumerate(products)}
        results.sort(key=lambda x: id_order.get(x[0]["id"], 9999))

        lines = ["🔍 *Live Stock Status:*\n"]
        for p, s, price in results:
            paused     = p.get("tracking_paused", False)
            se         = status_emoji(s, paused=paused)
            price_str  = f"\n💰 {price}" if price and s == "IN_STOCK" and not paused else ""
            pause_note = " _(paused)_" if paused else ""
            lines.append(f"{se} [{short_title(p['title'])}]({p['url']}) — `{s}`{price_str}{pause_note}\n")

        try:
            wait_msg.delete()
        except Exception:
            pass
        target.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
            reply_markup=main_menu_keyboard()
        )
    except Exception as e:
        logger.error(f"status error: {e}")
        target.reply_text("❌ Error checking status.")
    finally:
        # Manual check khatam — auto check wapas normally chal sakta hai
        _manual_status_event.clear()


def add_cmd(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    count   = db.count_products(user_id)
    target  = update.message or update.callback_query.message
    if count >= MAX_PRODUCTS_PER_USER:
        target.reply_text(
            f"⚠️ You're already tracking *{MAX_PRODUCTS_PER_USER}* products (max limit).\n"
            "Please /remove some before adding more.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    context.user_data["awaiting_link"] = True
    context.user_data.pop("remove_list", None)
    target.reply_text(
        "🔗 *Send me the Amazon product link now:*\n\n"
        "_Example: https://www.amazon.in/dp/B09XXXXX_",
        parse_mode=ParseMode.MARKDOWN
    )


def remove_cmd(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    target  = update.message or update.callback_query.message
    try:
        products = db.get_products(user_id)
        if not products:
            target.reply_text("🗂️ *Nothing to remove.*", parse_mode=ParseMode.MARKDOWN)
            return
        context.user_data["remove_list"] = products
        context.user_data.pop("awaiting_link", None)
        keyboard = []
        for p in products:
            s    = p.get("last_status", "UNKNOWN")
            icon = "✅" if s == "IN_STOCK" else "❌" if s == "OUT_OF_STOCK" else "⚪"
            keyboard.append([InlineKeyboardButton(f"{icon}  {short_title(p['title'], 32)}", callback_data=f"rm_{p['id']}")])
        keyboard.append([InlineKeyboardButton("Cancel", callback_data="rm_cancel")])
        target.reply_text(
            "🗑️ *Tap a product to remove:*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"remove error: {e}")


def button_handler(update: Update, context: CallbackContext):
    for attempt in range(2):
        try:
            _button_handler_impl(update, context)
            return
        except (NetworkError, TimedOut) as e:
            logger.warning(f"button_handler timeout (attempt {attempt+1}): {e}")
            time.sleep(1)
    logger.error("button_handler: gave up after retries")


def _button_handler_impl(update: Update, context: CallbackContext):
    query = update.callback_query
    try:
        query.answer()
    except (NetworkError, TimedOut) as e:
        logger.warning(f"query.answer() timeout (non-critical, continuing): {e}")
    data  = query.data
    logger.info(f"🔘 button_handler received: {data}")

    command_map = {
        "cmd_add":        add_cmd,
        "cmd_list":       list_products,
        "cmd_status":     status_check,
        "cmd_remove":     remove_cmd,
        "cmd_pause":      pause_cmd,
        "cmd_stop":       stop_cmd,
    }
    if data in command_map:
        command_map[data](update, context)
        return

    if data.startswith("rm_") and data != "rm_cancel":
        user_id = update.effective_user.id
        pid     = int(data[3:])
        db.remove_product(pid, user_id)
        context.user_data.pop("remove_list", None)
        remaining = db.get_products(user_id)
        if not remaining:
            query.message.reply_text(
                "✅ *Product removed.*\n\n🗂️ Your list is now empty.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=main_menu_keyboard()
            )
            return
        keyboard = []
        for p in remaining:
            s    = p.get("last_status", "UNKNOWN")
            icon = "✅" if s == "IN_STOCK" else "❌" if s == "OUT_OF_STOCK" else "⚪"
            keyboard.append([InlineKeyboardButton(f"{icon}  {short_title(p['title'], 32)}", callback_data=f"rm_{p['id']}")])
        keyboard.append([InlineKeyboardButton("✅ Done", callback_data="rm_cancel")])
        query.message.reply_text(
            f"✅ *Removed!* {len(remaining)} product(s) remaining.\n\n🗑️ *Tap another to remove, or Done:*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("pause_"):
        product_id = int(data[6:])
        user_id    = update.effective_user.id
        new_state  = db.toggle_product_pause(product_id, user_id)
        if new_state is None:
            return
        products = db.get_products(user_id)
        keyboard = []
        for i, p in enumerate(products, 1):
            paused = p.get("tracking_paused", False)
            se     = status_emoji(p.get("last_status", "UNKNOWN"), paused=paused)
            label  = f"{i}.  {se}  {short_title(p['title'], 25)}  [{'PAUSED' if paused else 'TRACKING'}]"
            keyboard.append([InlineKeyboardButton(label, callback_data=f"pause_{p['id']}")])
        keyboard.append([InlineKeyboardButton("✅ Done", callback_data="rm_cancel")])
        try:
            query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception:
            pass
        return

    if data == "noop":
        return

    if data == "bc_new":
        context.user_data["awaiting_broadcast_msg"] = True
        context.user_data.pop("awaiting_link", None)
        query.message.reply_text(
            "📝 *Send your broadcast message:*\n\n_I'll ask for its interval next.*",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if data == "bc_list":
        messages = db.get_broadcast_messages()
        if not messages:
            query.message.reply_text(
                "🗂️ *No broadcast messages saved.*\n\nUse /broadcast → New to add one.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        for m in messages:
            query.message.reply_text(
                f"`#{m['id']}` _(every {m['interval_minutes']} min)_\n{_short_preview(m['text'], 80)}",
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
        return

    if data == "bc_remove":
        messages = db.get_broadcast_messages()
        if not messages:
            query.message.reply_text(
                "🗂️ *No broadcast messages saved.*",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        keyboard = [
            [InlineKeyboardButton(f"❌ {_short_preview(m['text'])}", callback_data=f"bcrm_{m['id']}")]
            for m in messages
        ]
        query.message.reply_text(
            "🗑 *Pick a message to remove:*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("bciv_"):
        msg_id = int(data[5:])
        context.user_data["awaiting_retime_id"] = msg_id
        query.message.reply_text(
            f"⏱️ *New interval (in minutes) for broadcast #{msg_id}?*",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if data.startswith("bcrm_"):
        msg_id = int(data[5:])
        db.remove_broadcast_message(msg_id)
        query.edit_message_text(f"🗑️ *Removed broadcast #{msg_id}.*", parse_mode=ParseMode.MARKDOWN)
        return

    if data == "rm_cancel":
        context.user_data.pop("remove_list", None)
        query.edit_message_text("👍 *Cancelled.* Nothing was removed.", parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_keyboard())
        return

    if data == "rmall_confirm":
        db.remove_all_products(update.effective_user.id)
        context.user_data.clear()
        query.edit_message_text("🗑 *All products removed.*", parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_keyboard())
        return


def handle_message(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    text    = update.message.text.strip() if update.message.text else ""
    try:
        db.upsert_user(user_id, update.effective_chat.id, update.effective_user.username)
        if context.user_data.get("awaiting_broadcast_msg"):
            context.user_data.pop("awaiting_broadcast_msg")
            context.user_data["pending_broadcast_text"] = update.message.text
            context.user_data["awaiting_broadcast_interval"] = True
            update.message.reply_text(
                "⏱️ *How many minutes between sends for this message?*\n\n_Just send a number, e.g. 30_",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        if context.user_data.get("awaiting_broadcast_interval"):
            context.user_data.pop("awaiting_broadcast_interval")
            pending_text = context.user_data.pop("pending_broadcast_text", None)
            if not text.isdigit() or int(text) <= 0 or not pending_text:
                update.message.reply_text(
                    "❌ *Please send a valid number of minutes.*",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            minutes = int(text)
            row = db.add_broadcast_message(pending_text, minutes)
            update.message.reply_text(
                f"✅ *Broadcast #{row['id'] if row else '?'} saved!*\n\nIt'll be sent to the channel every {minutes} minute(s).",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=main_menu_keyboard()
            )
            return
        if context.user_data.get("awaiting_retime_id"):
            msg_id = context.user_data.pop("awaiting_retime_id")
            if not text.isdigit() or int(text) <= 0:
                update.message.reply_text(
                    "❌ *Please send a valid number of minutes.*",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            minutes = int(text)
            db.set_broadcast_interval(msg_id, minutes)
            update.message.reply_text(
                f"✅ *Broadcast #{msg_id} will now be sent every {minutes} minute(s).*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=main_menu_keyboard()
            )
            return
        if context.user_data.get("awaiting_link"):
            context.user_data.pop("awaiting_link")
            asin = AmazonScraper.extract_asin(text)
            if not asin:
                update.message.reply_text("❌ *Invalid Amazon link.*\n\nPlease send a valid amazon.in product URL.", parse_mode=ParseMode.MARKDOWN)
                return
            existing = db.get_products(user_id)
            if any(p["asin"] == asin for p in existing):
                update.message.reply_text("ℹ️ *This product is already in your list!*", parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_keyboard())
                return
            # ?aod=1 — variant lock rehta hai, redirect nahi hota; tag — apna affiliate tag, koi bhi purana tag discard
            canonical_url = f"https://www.amazon.in/dp/{asin}?aod=1&tag={AFFILIATE_TAG}"
            wait = update.message.reply_text(f"🔎 Fetching info for `{asin}`…", parse_mode=ParseMode.MARKDOWN)
            info = AmazonScraper.fetch_product_info(asin, canonical_url)
            db.add_product(user_id, asin, info["title"], canonical_url)
            se         = status_emoji(info["status"])
            price_line = f"💰 Price: *{info['price']}*\n" if info.get("price") else ""
            try:
                wait.delete()
            except Exception:
                pass
            update.message.reply_text(
                f"✅ *Product Added!*\n\n"
                f"📦 *{short_title(info['title'], 80)}*\n\n"
                f"{price_line}"
                f"📊 Status: {se} `{info['status']}`\n\n"
                f"🔔 I'll notify you when the status changes!",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=main_menu_keyboard()
            )
            return
        update.message.reply_text("👇 *Use the menu below or send a command:*", parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_keyboard())
    except Exception as e:
        logger.error(f"handle_message error: {e}")
        update.message.reply_text("❌ Something went wrong. Please try again.")


# ═══════════════════════════════════════════════
#  SCHEDULED STOCK CHECK
# ═══════════════════════════════════════════════

def _refresh_existing_titles():
    """Startup ke waqt ek baar chalta hai — purane saved titles ko naye KNOWN_COLORS
    list ke hisaab se re-order karta hai (koi Amazon fetch nahi, sirf DB text update)."""
    try:
        products = db.get_all_products_flat()
        updated = 0
        for p in products:
            old_title = p.get("title") or ""
            new_title = AmazonScraper._reorder_color(old_title)
            if new_title != old_title:
                db.update_title(p["id"], new_title)
                updated += 1
        if updated:
            logger.info(f"🎨 Refreshed {updated} product title(s) with updated color list.")
    except Exception as e:
        logger.error(f"_refresh_existing_titles error: {e}")


def broadcast_list(context: CallbackContext):
    """Sends the tracked-products list-summary — runs independently of custom broadcast messages."""
    try:
        products = db.get_all_products_flat()
        if not products:
            return
        lines = ["📦 *Tracked Products — Auto Summary (every 10 min):*\n"]
        for i, p in enumerate(products, 1):
            paused = p.get("tracking_paused", False)
            se     = status_emoji(p.get("last_status", "UNKNOWN"), paused=paused)
            lines.append(f"`{i}.` {se} [{short_title(p['title'])}]({p['url']})\n")
        lines.append(f"_Total: {len(products)}_")
        context.bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text="\n".join(lines),
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"broadcast_list error: {e}")


def broadcast_ticker(context: CallbackContext):
    """Runs every minute — sends each saved broadcast message on its own interval."""
    try:
        for m in db.get_due_broadcast_messages():
            try:
                context.bot.send_message(
                    chat_id=GROUP_CHAT_ID,
                    text=m["text"],
                    parse_mode=ParseMode.MARKDOWN,
                    disable_web_page_preview=True
                )
                db.mark_broadcast_sent(m["id"])
            except TelegramError as e:
                logger.error(f"broadcast_ticker send error (msg {m['id']}): {e}")
    except Exception as e:
        logger.error(f"broadcast_ticker error: {e}")


def _short_preview(text, length=40):
    text = text.strip().replace("\n", " ")
    return text[:length] + ("…" if len(text) > length else "")


def broadcast_cmd(update: Update, context: CallbackContext):
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🆕 New",    callback_data="bc_new"),
        InlineKeyboardButton("📋 List",   callback_data="bc_list"),
        InlineKeyboardButton("🗑 Remove", callback_data="bc_remove"),
    ]])
    update.message.reply_text(
        "📢 *Broadcast Messages*\n\n_Choose an action:_",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=keyboard
    )


def interval_cmd(update: Update, context: CallbackContext):
    messages = db.get_broadcast_messages()
    if not messages:
        update.message.reply_text(
            "🗂️ *No broadcast messages saved.*\n\nUse /broadcast → New to add one.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    keyboard = [
        [InlineKeyboardButton(f"{_short_preview(m['text'])}  ⏱ {m['interval_minutes']}m", callback_data=f"bciv_{m['id']}")]
        for m in messages
    ]
    update.message.reply_text(
        "⏱️ *Pick a message to change its interval:*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


def scheduled_stock_check(context: CallbackContext):
    global _check_deadline, _last_full_check, _consecutive_bad_cycles, _extra_cooldown_until, _tail_penalty_asins

    # Step -1: 503 circuit breaker — agar pichle cycles mein zyada 503 aaye,
    # cooldown window ke andar naya cycle skip karo. Amazon ko lagatar
    # retries dikhna hi block ko lamba karta hai.
    if time.time() < _extra_cooldown_until:
        logger.info(f"🧊 Cooldown active ({_extra_cooldown_until - time.time():.0f}s left) — skipping cycle.")
        return

    # Step 0: Agar koi user manually /status chala raha hai, is cycle
    # ko poora skip kar do — dono ek saath Amazon hit nahi karenge
    if _manual_status_event.is_set():
        logger.info("⏭️ Manual /status in progress — skipping auto check this cycle.")
        return

    # Step 1: Quick interval check (lock se pehle, sirf rough guard)
    products = db.get_all_products_with_users() or []
    if not products:
        return

    dynamic_interval = random.uniform(10, 15)
    if time.time() - _last_full_check < dynamic_interval:
        return  # silently skip

    # Step 2: Lock acquire karo
    if not _check_lock.acquire(blocking=False):
        logger.warning("⚠️ Previous check still running — skipping.")
        return

    # Manual /status abhi-abhi shuru hui ho sakti hai (race) — turant chhod do
    if _manual_status_event.is_set():
        logger.info("⏭️ Manual /status started — aborting auto check before it begins.")
        _check_lock.release()
        return

    # Step 3: Lock ke ANDAR double-check karo (race condition guard)
    # Agar do threads dono interval check pass kar gaye, doosra yahan rukega
    if time.time() - _last_full_check < dynamic_interval:
        _check_lock.release()
        return

    # Step 4: Timestamp aur deadline set karo — lock ke andar
    _last_full_check = time.time()
    n_products    = len(products)
    total_timeout = max(30, n_products * 15 + 10)
    _check_deadline = time.time() + total_timeout

    try:
        n_workers = min(MAX_WORKERS, n_products)
        logger.info("🔄 Scheduled stock check starting…")
        logger.info(f"Checking {n_products} product(s) | workers: {n_workers} (batched) | timeout: {total_timeout}s")

        t0 = time.time()
        # Har cycle mein order shuffle karo — fixed order + fixed interval
        # milke ek predictable "bot pattern" banata hai jo detection ko
        # aur aasan bana deta hai.
        tail_asins = [a for a in _tail_penalty_asins.keys()]
        tail_set   = set(tail_asins)
        rest       = [p for p in products if p["asin"] not in tail_set]
        random.shuffle(rest)
        tail_products = [p for a in tail_asins for p in products if p["asin"] == a]
        shuffled = rest + tail_products

        bad_count    = 0
        bad_lock     = threading.Lock()
        failed_asins = set()

        def _check_one(product):
            nonlocal bad_count
            if time.time() > _check_deadline or _manual_status_event.is_set():
                return
            try:
                info = AmazonScraper.fetch_product_info(product["asin"], product.get("url"), max_attempts=2)

                if info["status"] != "UNKNOWN":
                    # Status known hai — DB update karo, purana status row mein milega
                    row       = db.update_status(product["id"], info["status"], info.get("price"))
                    old       = row["last_status"] if row else product.get("last_status", "UNKNOWN")
                    old_price = row["last_price"]  if row else product.get("last_price")
                else:
                    # 503 ya fetch fail — DB update mat karo, purana status preserve karo
                    with bad_lock:
                        bad_count += 1
                        failed_asins.add(product["asin"])
                    row       = db.execute(
                        "SELECT last_status, last_price FROM products WHERE id = %s",
                        (product["id"],), fetch_one=True
                    )
                    old       = row["last_status"] if row else product.get("last_status", "UNKNOWN")
                    old_price = row["last_price"]  if row else product.get("last_price")
                    logger.info(f"[{product['asin']}] 503/UNKNOWN — skipping DB update, keeping status={old}")

                _dispatch_status_change(context, product, old, info["status"], old_price, info.get("price"))
            except Exception as e:
                logger.error(f"Check error {product.get('asin','?')}: {e}")

        # 3-3 product ke batch mein parallel fetch — submit ke beech
        # 100-300ms stagger (bilkul simultaneous nahi), batches ke beech
        # 100-500ms random jitter — agar us cycle mein 503 mile hon to
        # jitter thoda badha do (halka backoff, poora nahi hataya).
        for batch_start in range(0, len(shuffled), n_workers):
            if time.time() > _check_deadline:
                logger.warning("⏰ Deadline reached — stopping early")
                break
            if _manual_status_event.is_set():
                logger.info("⏭️ Manual /status started mid-cycle — stopping auto check early.")
                break

            batch = shuffled[batch_start:batch_start + n_workers]
            with ThreadPoolExecutor(max_workers=n_workers) as ex:
                futures = []
                for i, p in enumerate(batch):
                    if i > 0:
                        time.sleep(random.uniform(0.3, 0.9))
                    futures.append(ex.submit(_check_one, p))
                for fut in as_completed(futures, timeout=PER_PRODUCT_TIMEOUT + 20):
                    pass

            jitter = random.uniform(0.3, 1.5)
            if bad_count > 0:
                jitter += random.uniform(0.3, 0.8) * bad_count
            time.sleep(jitter)

        # Cycle ke baad: agar bahut zyada 503 aaye, circuit breaker trigger
        # karo — agla cycle(s) lambe cooldown ke sath skip karo.
        bad_ratio = bad_count / max(1, len(shuffled))
        if bad_ratio >= 0.5:
            _consecutive_bad_cycles += 1
            cooldown = min(300 * _consecutive_bad_cycles, 1800)  # 5min, 10min... cap 30min
            _extra_cooldown_until = time.time() + cooldown
            logger.warning(f"🧊 High 503 rate ({bad_count}/{len(shuffled)}) — cooldown {cooldown}s, streak={_consecutive_bad_cycles}")
        else:
            _consecutive_bad_cycles = 0

        # Tail penalty queue update: existing entries ka counter ghatao,
        # 0 pe pahunchte hi hata do. Naye fail hue asins ko sabse aakhir
        # mein daalo (agar pehle se the to reset karke phir se last pe).
        for asin in list(_tail_penalty_asins.keys()):
            if asin not in failed_asins:
                _tail_penalty_asins[asin] -= 1
                if _tail_penalty_asins[asin] <= 0:
                    del _tail_penalty_asins[asin]
        for asin in failed_asins:
            _tail_penalty_asins.pop(asin, None)   # move-to-end ke liye pehle hatao
            _tail_penalty_asins[asin] = _TAIL_PENALTY_CYCLES

        elapsed = time.time() - _last_full_check
        logger.info(f"✅ Stock check done in {elapsed:.1f}s")

    except Exception as e:
        logger.error(f"scheduled_stock_check error: {e}")
    finally:
        _check_deadline = None
        try:
            _check_lock.release()
        except RuntimeError:
            pass


def _keepalive_ping(context: CallbackContext):
    try:
        db.execute("SELECT 1", fetch_one=True)
        logger.debug("🏓 DB keepalive OK")
    except Exception as e:
        logger.warning(f"Keepalive ping failed: {e}")


def _parse_price_value(price_str):
    if not price_str:
        return None
    try:
        cleaned = price_str.replace("₹", "").replace(",", "").replace(" ", "").strip()
        return float(cleaned.split(".")[0])
    except Exception:
        return None


# ── Alert dispatch: dedicated thread, scraping workers se decoupled ─────
# send_message koi Amazon call nahi hai — ise scraping ThreadPoolExecutor
# ke andar inline chalane ki zaroorat nahi. Har status-change apne khud
# ke daemon thread mein fire hota hai, taaki:
#  1) scraping worker turant free ho jaye (18s tak block na ho)
#  2) multiple products ek saath IN_STOCK hon to unke alert-bursts bhi
#     genuinely parallel chalein (koi lock/serialization nahi)
def _dispatch_status_change(context, product, old, new, old_price=None, new_price=None):
    threading.Thread(
        target=_handle_status_change_locked,
        args=(context, product, old, new, old_price, new_price),
        daemon=True,
        name=f"alert-{product.get('asin','?')}"
    ).start()


# ── Alert serialization lock ─────────────────────────────────────────────
# Scraping 3 workers mein parallel chalti hai (isse touch nahi karna),
# lekin alert-sending (_handle_status_change) ek time pe sirf ek hi
# product ke liye chalni chahiye — sequential-bot jaisa guarantee.
# Dispatch thread (scraper se already decoupled) yahan lock lena hi
# sabse pehla kaam karta hai, taaki scraper kabhi wait na kare, sirf
# dusre products ke alert-bursts ek doosre ka wait karein.
_alert_send_lock = threading.Lock()


def _handle_status_change_locked(context, product, old, new, old_price=None, new_price=None):
    with _alert_send_lock:
        _handle_status_change(context, product, old, new, old_price, new_price)


def _handle_status_change(context, product, old, new, old_price=None, new_price=None):
    chat_id = GROUP_CHAT_ID
    title   = product["title"]
    url     = product["url"]

    if old in ("UNKNOWN", None) and new == "UNKNOWN":
        return

    if old == new and old != "UNKNOWN":
        if old == "IN_STOCK" and old_price and new_price and product.get("alert_pricedrop", True):
            old_val = _parse_price_value(old_price)
            new_val = _parse_price_value(new_price)
            if old_val and new_val and new_val < old_val:
                drop_pct = round((old_val - new_val) / old_val * 100, 1)
                if drop_pct >= 1:
                    pd_settings = db.get_alert_settings(product["user_id"])
                    pd_count    = pd_settings["pricedrop_alert_count"]
                    pd_gap      = pd_settings["alert_gap_seconds"]
                    logger.info(f"💸 PRICE DROP {product['asin']}: {old_price}→{new_price} ({drop_pct}%)")
                    pd_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🛒 Buy Now", url=url)]])
                    for i in range(1, pd_count + 1):
                        t_send = time.time()
                        try:
                            context.bot.send_message(
                                chat_id=chat_id,
                                text=(
                                    f"💰 *Price Drop Alert!* ({i}/{pd_count})\n\n"
                                    f"📦 *{short_title(title, 80)}*\n\n"
                                    f"{old_price} → *{new_price}* ({drop_pct}% off)\n\n"
                                    f"🔗 [View on Amazon]({url})"
                                ),
                                parse_mode=ParseMode.MARKDOWN,
                                reply_markup=pd_kb,
                                disable_web_page_preview=True
                            )
                            logger.info(f"📨 Price-drop alert {i}/{pd_count} sent [{product['asin']}] t={t_send:.3f}")
                        except TelegramError as e:
                            logger.error(f"Price drop alert {i} error: {e}")
                        if i < pd_count and pd_gap > 0:
                            time.sleep(pd_gap)
        return

    if new == "IN_STOCK" and not product.get("alert_instock", True):
        return

    if new == "IN_STOCK":
        settings    = db.get_alert_settings(product["user_id"])
        alert_count = settings["instock_alert_count"]
        gap         = 2  # fixed — user setting ab ignore hoti hai
        logger.info(f"🔥 IN STOCK ({old}→IN_STOCK): {product['asin']} — {alert_count} alerts")
        keyboard   = InlineKeyboardMarkup([[InlineKeyboardButton("🛒 Buy Now on Amazon", url=url)]])
        price_line = f"💰 *Price: {new_price}*\n\n" if new_price else ""
        for i in range(1, alert_count + 1):
            t_send = time.time()
            try:
                context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🚨 *IN STOCK!* ({i}/{alert_count})\n\n"
                        f"📦 *{short_title(title, 80)}*\n\n"
                        f"{price_line}"
                        f"🛒 [Buy Now on Amazon]({url})\n\n"
                        "_Hurry before it sells out again!_ 🏃"
                    ),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard,
                    disable_web_page_preview=True
                )
                logger.info(f"📨 Alert {i}/{alert_count} sent [{product['asin']}] t={t_send:.3f}")
            except TelegramError as e:
                logger.error(f"Alert {i}/{alert_count} error: {e}")
            if i < alert_count and gap > 0:
                time.sleep(gap)

    elif old == "IN_STOCK" and new == "OUT_OF_STOCK":
        logger.info(f"🔴 OUT OF STOCK: {product['asin']}")
        try:
            context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"📴 *Out of Stock*\n\n"
                    f"📦 *{short_title(title, 80)}*\n\n"
                    f"I'll notify you when it's back."
                ),
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
        except TelegramError as e:
            logger.error(f"Out of stock alert error: {e}")


# ═══════════════════════════════════════════════
#  ERROR HANDLER
# ═══════════════════════════════════════════════

def error_handler(update: Update, context: CallbackContext):
    try:
        raise context.error
    except Conflict:
        logger.warning("⚠️ Conflict — old instance shutting down")
    except (NetworkError, TimedOut):
        logger.warning("⚠️ Network/timeout error — sleeping 10s")
        time.sleep(10)
    except TelegramError as e:
        logger.error(f"TelegramError: {e}")
    except Exception as e:
        logger.error(f"Unhandled error: {e}", exc_info=True)


# ═══════════════════════════════════════════════
#  HEALTH SERVER
# ═══════════════════════════════════════════════

health_app = Flask(__name__)

@health_app.route("/")
def home():
    return "🟢 Bot is running!", 200

@health_app.route("/health")
def health():
    return "OK", 200

def _run_health_server():
    health_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

def _self_ping():
    """
    Har 4 minute mein ping karo — Render ko sleep nahi aane deta.
    RENDER_EXTERNAL_URL set hai → external ping (actually works on Render!)
    Nahi set → internal fallback (local dev ke liye)
    """
    time.sleep(60)
    while True:
        try:
            if RENDER_EXTERNAL_URL:
                ping_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/health"
                with httpx.Client(timeout=httpx.Timeout(15.0)) as client:
                    resp = client.get(ping_url)
                logger.debug(f"🏓 External self-ping OK → {resp.status_code}")
            else:
                with httpx.Client(timeout=httpx.Timeout(10.0)) as client:
                    client.get(f"http://127.0.0.1:{PORT}/health")
                logger.debug("🏓 Internal ping OK (set RENDER_EXTERNAL_URL for Render)")
        except Exception as e:
            logger.warning(f"Self-ping failed: {e}")
        time.sleep(240)


# ═══════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════

def main():
    logger.info("=" * 60)
    logger.info("🚀 Amazon Stock Tracker starting…")
    if CF_PROXY_URL:
        logger.info(f"🌐 Cloudflare proxy ENABLED — routing Amazon requests via {CF_PROXY_URL}")
    else:
        logger.info("🌐 Cloudflare proxy NOT configured — hitting Amazon.in directly (set CF_PROXY_URL/CF_PROXY_KEY to enable)")
    logger.info("=" * 60)

    _refresh_existing_titles()

    threading.Thread(target=_run_health_server, daemon=True).start()
    logger.info(f"✅ Health server on port {PORT}")

    threading.Thread(target=_watchdog_thread, daemon=True, name="watchdog").start()
    threading.Thread(target=_cookie_refresh_thread, daemon=True, name="cookie-refresh").start()
    logger.info("✅ Watchdog started")

    threading.Thread(target=_self_ping, daemon=True, name="self-ping").start()
    logger.info("✅ Self-ping started (every 4 min)")

    updater = Updater(token=BOT_TOKEN, use_context=True,
                      request_kwargs={"connect_timeout": 30, "read_timeout": 30})

    try:
        updater.bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook cleared")
    except Exception:
        pass

    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start",       start))
    dp.add_handler(CommandHandler("id",          id_cmd))
    dp.add_handler(CommandHandler("broadcast",   broadcast_cmd))
    dp.add_handler(CommandHandler("interval",    interval_cmd))
    dp.add_handler(CommandHandler("status",      status_check))
    dp.add_handler(CallbackQueryHandler(button_handler))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    dp.add_error_handler(error_handler)

    updater.job_queue.run_repeating(
        scheduled_stock_check,
        interval=5, first=15,
        job_kwargs={"max_instances": 1, "coalesce": True, "misfire_grace_time": 5}
    )
    logger.info("✅ Stock checker: every 5s tick, gated by 5-10s random jitter")

    updater.job_queue.run_repeating(_keepalive_ping, interval=60, first=10)
    logger.info("✅ DB keepalive registered (every 60s)")

    updater.job_queue.run_repeating(broadcast_list, interval=600, first=60, name="broadcast_list_job")
    logger.info("✅ Channel broadcast registered (every 10 min, fixed)")

    updater.job_queue.run_repeating(broadcast_ticker, interval=60, first=30, name="broadcast_ticker_job")
    logger.info("✅ Per-message broadcast ticker registered (checks every 1 min)")

    updater.start_polling(drop_pending_updates=True, poll_interval=1.0, timeout=20)
    logger.info("✅ Bot is live! Send /start to begin.")
    updater.idle()


if __name__ == "__main__":
    main()
