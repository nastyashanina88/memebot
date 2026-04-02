"""
Meme Bot — парсит каналы, присылает мемы тебе в личку на одобрение,
публикует одобренные в канал по расписанию.
"""

import asyncio
import hashlib
import logging
import os
import random
import re
import re as _re
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Optional

import aiohttp
import imagehash
import pytz
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from PIL import Image
from telegram import InputMediaPhoto, InputMediaVideo, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ─────────────────────────────────────────────────────────────────────
#  НАСТРОЙКИ
# ─────────────────────────────────────────────────────────────────────

SOURCE_CHANNELS = [
    "nedovolnij", "membeeeers", "vsratessa", "meme_division", "mynameismem",
    "stolencatsbyolga", "memo4ek", "memnaya_LR", "Leomemesmda", "rus_mem",
    "cherdakmemov", "Katzen_und_Politik", "smilemilf", "vsratyikontent",
    "thresomewhitout", "impirat", "pleasedickann", "monologue3", "dobriememes",
    "russkiememy", "female_memes", "drugzahodi", "axaxanakanecta", "cats_mems",
    "memesfs", "grustnie_memi", "tasamaDama", "plohoy_vladlen", "sammychoret",
]

POSTS_PER_DAY_MIN  = 7
POSTS_PER_DAY_MAX  = 10
POST_START_HOUR    = 9
POST_END_HOUR      = 22
MAX_CAPTION_LEN    = 150
FETCH_INTERVAL     = 3600
FETCH_HOURS_BACK   = 24
MAX_SEND_PER_FETCH = 25
SHOWQUEUE_LIMIT    = 10
PHASH_THRESHOLD    = 10
MIN_VIEWS_PER_HOUR = 150

# ─────────────────────────────────────────────────────────────────────
#  КОНФИГ
# ─────────────────────────────────────────────────────────────────────

BOT_TOKEN     = os.getenv("BOT_TOKEN", "").strip()
MY_CHANNEL    = os.getenv("MY_CHANNEL", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
MSK           = pytz.timezone("Europe/Moscow")
_DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# ─────────────────────────────────────────────────────────────────────
#  ФИЛЬТРЫ
# ─────────────────────────────────────────────────────────────────────

AD_WORDS = [
    "реклама", "купить", "заказать", "промокод", "скидк",
    "подписывайся на", "переходи", "прайс", "оплата",
    "доставка", "магазин", "наш канал", "наш бот", "пиши в лс",
]
LINK_RE    = re.compile(r"https?://|t\.me/\+|t\.me/joinchat", re.I)
MENTION_RE = re.compile(r"@[a-zA-Z0-9_]{5,}")
PHONE_RE   = re.compile(r"\+7[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}")

def is_good_post(caption: str) -> bool:
    text = caption or ""
    if len(text) > MAX_CAPTION_LEN:
        return False
    t = text.lower()
    if any(w in t for w in AD_WORDS):
        return False
    if LINK_RE.search(text):
        return False
    if len(MENTION_RE.findall(text)) > 1:
        return False
    if PHONE_RE.search(text):
        return False
    return True

# ─────────────────────────────────────────────────────────────────────
#  ПАРСИНГ t.me/s/{channel}
# ─────────────────────────────────────────────────────────────────────

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

async def fetch_channel(session: aiohttp.ClientSession, channel: str,
                        semaphore: asyncio.Semaphore,
                        hours_back: int = FETCH_HOURS_BACK) -> list:
    async with semaphore:
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with session.get(f"https://t.me/s/{channel}", timeout=timeout) as resp:
                if resp.status != 200:
                    return []
                html = await resp.text()

            soup   = BeautifulSoup(html, "html.parser")
            posts  = []
            cutoff = datetime.now(pytz.utc) - timedelta(hours=hours_back)

            for msg in soup.find_all("div", class_="tgme_widget_message"):
                data_post = msg.get("data-post", "")
                msg_id    = data_post.split("/")[-1] if "/" in data_post else ""
                if not msg_id:
                    continue

                # Фильтр по времени
                post_time = None
                time_el = msg.find("time")
                if time_el and time_el.get("datetime"):
                    try:
                        post_time = datetime.fromisoformat(time_el["datetime"])
                        if post_time.tzinfo is None:
                            post_time = post_time.replace(tzinfo=timezone.utc)
                        if post_time < cutoff:
                            continue
                    except Exception:
                        pass

                # Просмотры — фильтр по скорости набора (просмотры/час)
                views_el = msg.find("span", class_="tgme_widget_message_views")
                if views_el:
                    views_text = views_el.get_text(strip=True).upper().replace("\u00A0", "")
                    try:
                        if "K" in views_text:
                            views = int(float(views_text.replace("K", "")) * 1000)
                        elif "M" in views_text:
                            views = int(float(views_text.replace("M", "")) * 1_000_000)
                        else:
                            views = int(views_text)
                    except ValueError:
                        views = 0
                    if post_time:
                        age_hours = max((datetime.now(timezone.utc) - post_time).total_seconds() / 3600, 0.25)
                    else:
                        age_hours = 1.0
                    if views / age_hours < MIN_VIEWS_PER_HOUR:
                        continue

                # Подпись
                text_el = msg.find("div", class_="tgme_widget_message_text")
                caption = text_el.get_text(separator=" ").strip() if text_el else ""
                if not is_good_post(caption):
                    continue

                # Все фото в сообщении (поддержка альбомов)
                photo_urls = []
                for wrap in msg.find_all("a", class_="tgme_widget_message_photo_wrap"):
                    m = re.search(r"url\('(.+?)'\)", wrap.get("style", ""))
                    if m:
                        photo_urls.append(m.group(1))

                if photo_urls:
                    posts.append({
                        "channel":    channel,
                        "msg_id":     msg_id,
                        "media_url":  photo_urls[0],
                        "media_urls": photo_urls,      # все фото альбома
                        "caption":    caption,
                        "media_type": "photo",
                        "is_album":   len(photo_urls) > 1,
                    })
                    continue

                # Видео / анимация (одиночные — альбомы из видео редки)
                video_el = msg.find("video", class_="tgme_widget_message_video")
                if video_el and video_el.get("src"):
                    media_type = "animation" if video_el.has_attr("loop") else "video"
                    posts.append({
                        "channel":    channel,
                        "msg_id":     msg_id,
                        "media_url":  video_el["src"],
                        "media_urls": [video_el["src"]],
                        "caption":    caption,
                        "media_type": media_type,
                        "is_album":   False,
                    })

            return posts

        except Exception as e:
            logging.error(f"Ошибка парсинга {channel}: {e}")
            return []


async def download_media(session: aiohttp.ClientSession, url: str) -> Optional[bytes]:
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with session.get(url, timeout=timeout) as r:
            return await r.read() if r.status == 200 else None
    except Exception as e:
        logging.error(f"Ошибка скачивания медиа: {e}")
        return None


async def refetch_media(session: aiohttp.ClientSession, channel: str,
                        msg_id: str, media_type: str = "photo") -> Optional[bytes]:
    """Рефетч первого медиа поста при истёкшем CDN-URL."""
    try:
        url = f"https://t.me/s/{channel}?before={int(msg_id) + 1}"
        timeout = aiohttp.ClientTimeout(total=10)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            html = await resp.text()
        soup = BeautifulSoup(html, "html.parser")
        for msg in soup.find_all("div", class_="tgme_widget_message"):
            if msg.get("data-post", "") != f"{channel}/{msg_id}":
                continue
            if media_type == "photo":
                wrap = msg.find("a", class_="tgme_widget_message_photo_wrap")
                if wrap:
                    m = re.search(r"url\('(.+?)'\)", wrap.get("style", ""))
                    if m:
                        return await download_media(session, m.group(1))
            else:
                video_el = msg.find("video", class_="tgme_widget_message_video")
                if video_el and video_el.get("src"):
                    return await download_media(session, video_el["src"])
        return None
    except Exception as e:
        logging.error(f"refetch_media {channel}/{msg_id}: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────
#  БАЗА ДАННЫХ
# ─────────────────────────────────────────────────────────────────────

_default_db_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(_default_db_dir, exist_ok=True)
DB = os.path.join(os.getenv("DATA_DIR", _default_db_dir), "memes.db")

# ── PostgreSQL адаптер ────────────────────────────────────────────────

def _pg_sql(sql: str) -> str:
    """Конвертирует SQLite SQL в PostgreSQL."""
    is_insert_ignore = bool(_re.search(r'\bINSERT\s+OR\s+IGNORE\b', sql, _re.I))
    sql = sql.replace('?', '%s')
    sql = _re.sub(r'\bINSERT\s+OR\s+(IGNORE|REPLACE)\b', 'INSERT', sql, flags=_re.I)
    sql = _re.sub(
        r"datetime\('now',\s*'(-?\d+)\s+(days?|hours?)'\)",
        lambda m: f"to_char((NOW() + INTERVAL '{m.group(1)} {m.group(2)}') AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')",
        sql
    )
    sql = _re.sub(r"datetime\('now'\)", "to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')", sql)
    sql = _re.sub(r'\bAUTOINCREMENT\b', '', sql, flags=_re.I)
    sql = _re.sub(r'INTEGER\s+PRIMARY\s+KEY', 'BIGSERIAL PRIMARY KEY', sql, flags=_re.I)
    sql = _re.sub(r'\bBLOB\b', 'BYTEA', sql, flags=_re.I)
    sql = _re.sub(r'^\s*VACUUM\s*;?\s*$', 'SELECT 1', sql, flags=_re.I | _re.M)
    sql = _re.sub(r'\bADD\s+COLUMN\b(?!\s+IF\s+NOT\s+EXISTS\b)', 'ADD COLUMN IF NOT EXISTS', sql, flags=_re.I)
    if is_insert_ignore and 'ON CONFLICT' not in sql.upper():
        sql = sql.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
    return sql


class _PGResult:
    """Имитирует sqlite3 курсор для совместимости."""
    def __init__(self, cur, rowcount):
        self._cur = cur
        self.rowcount = rowcount
        self.lastrowid = None

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()


class _PGConn:
    """Обёртка psycopg2 с интерфейсом sqlite3."""
    def __init__(self, conn):
        self._conn = conn
        self._cur = conn.cursor()

    def execute(self, sql, params=()):
        self._cur.execute(_pg_sql(sql), params if params else None)
        return _PGResult(self._cur, self._cur.rowcount)

    def executemany(self, sql, seq):
        self._cur.executemany(_pg_sql(sql), seq)
        return _PGResult(self._cur, self._cur.rowcount)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._cur.close()
        self._conn.close()


if _DATABASE_URL:
    import psycopg2

    _PG_DSN = _DATABASE_URL if "connect_timeout" in _DATABASE_URL else _DATABASE_URL + "&connect_timeout=10"
    _pg_conn = None

    def _get_conn():
        global _pg_conn
        if _pg_conn is None or _pg_conn.closed:
            _pg_conn = psycopg2.connect(_PG_DSN)
        return _pg_conn

    @contextmanager
    def db_open():
        global _pg_conn
        conn = _get_conn()
        try:
            conn.autocommit = False
            yield _PGConn(conn)
            conn.commit()
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            try:
                _pg_conn.close()
            except Exception:
                pass
            _pg_conn = None
            raise
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise

else:
    @contextmanager
    def db_open():
        """Открывает соединение с БД с journal_mode=MEMORY — безопасно на полном диске."""
        conn = sqlite3.connect(DB)
        conn.execute("PRAGMA journal_mode=MEMORY")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def emergency_cleanup_db():
    """Очистка img_data при старте — работает даже когда диск забит (journal in RAM)."""
    try:
        with db_open() as db:
            # Очищаем img_data у ВСЕХ постов — больше не храним бинарные данные
            freed = db.execute(
                "UPDATE posts SET img_data=NULL WHERE img_data IS NOT NULL"
            ).rowcount
            freed_album = db.execute(
                "UPDATE album_media SET img_data=NULL WHERE img_data IS NOT NULL"
            ).rowcount
            deleted = db.execute(
                "DELETE FROM posts WHERE status IN ('posted','skipped','error') "
                "AND added_at < datetime('now', '-14 days')"
            ).rowcount
            db.commit()
        if freed or freed_album or deleted:
            logging.info(f"Стартовая очистка: posts={freed}, album_media={freed_album}, удалено={deleted}")
    except Exception as e:
        logging.warning(f"Стартовая очистка не удалась: {e}")


def init_db():
    with db_open() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                channel      TEXT NOT NULL,
                msg_id       TEXT NOT NULL,
                img_url      TEXT,
                caption      TEXT,
                user_caption TEXT,
                status       TEXT DEFAULT 'new',
                added_at     TEXT DEFAULT (datetime('now')),
                posted_at    TEXT,
                UNIQUE(channel, msg_id)
            )
        """)
        for col_def in [
            "ALTER TABLE posts ADD COLUMN user_caption TEXT",
            "ALTER TABLE posts ADD COLUMN img_data BLOB",
            "ALTER TABLE posts ADD COLUMN file_id TEXT",
            "ALTER TABLE posts ADD COLUMN img_hash TEXT",
            "ALTER TABLE posts ADD COLUMN tg_msg_id INTEGER",
            "ALTER TABLE posts ADD COLUMN media_type TEXT DEFAULT 'photo'",
            "ALTER TABLE posts ADD COLUMN phash TEXT",
            "ALTER TABLE posts ADD COLUMN is_album INTEGER DEFAULT 0",
        ]:
            try:
                db.execute(col_def)
            except Exception:
                pass
        try:
            db.execute("CREATE INDEX IF NOT EXISTS idx_img_hash ON posts(img_hash)")
        except Exception:
            pass
        db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                username TEXT PRIMARY KEY,
                trusted  INTEGER DEFAULT 0
            )
        """)
        # Таблица для хранения отдельных медиа в альбомах
        db.execute("""
            CREATE TABLE IF NOT EXISTS album_media (
                post_id  INTEGER NOT NULL,
                idx      INTEGER NOT NULL,
                img_url  TEXT,
                img_data BLOB,
                file_id  TEXT,
                PRIMARY KEY (post_id, idx)
            )
        """)
        if db.execute("SELECT COUNT(*) FROM channels").fetchone()[0] == 0:
            db.executemany(
                "INSERT OR IGNORE INTO channels (username, trusted) VALUES (?, 0)",
                [(ch,) for ch in SOURCE_CHANNELS]
            )
        db.commit()


def db_get(key: str) -> Optional[str]:
    if key == "admin_chat_id" and ADMIN_CHAT_ID:
        return ADMIN_CHAT_ID
    with db_open() as db:
        r = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r[0] if r else None


def db_set(key: str, value: str):
    with db_open() as db:
        if _DATABASE_URL:
            db.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, value)
            )
        else:
            db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)", (key, value))
        db.commit()


def db_hash_exists(img_hash: str) -> bool:
    with db_open() as db:
        return db.execute(
            "SELECT 1 FROM posts WHERE img_hash=? LIMIT 1", (img_hash,)
        ).fetchone() is not None


def compute_phash(img_data: bytes) -> Optional[str]:
    try:
        img = Image.open(BytesIO(img_data))
        return str(imagehash.dhash(img))
    except Exception:
        return None


def db_phash_is_duplicate(phash_str: str) -> bool:
    try:
        new_hash = imagehash.hex_to_hash(phash_str)
        with db_open() as db:
            rows = db.execute(
                "SELECT phash FROM posts WHERE phash IS NOT NULL "
                "AND added_at > datetime('now', '-30 days')"
            ).fetchall()
        for (existing,) in rows:
            try:
                if new_hash - imagehash.hex_to_hash(existing) <= PHASH_THRESHOLD:
                    return True
            except Exception:
                pass
    except Exception:
        pass
    return False


def db_save_post(channel, msg_id, img_url, caption,
                 img_hash: Optional[str] = None,
                 media_type: str = "photo",
                 phash: Optional[str] = None,
                 is_album: bool = False) -> Optional[int]:
    try:
        with db_open() as db:
            if _DATABASE_URL:
                cur = db.execute(
                    "INSERT INTO posts "
                    "(channel, msg_id, img_url, caption, img_hash, media_type, phash, is_album) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (channel, msg_id) DO NOTHING RETURNING id",
                    (channel, msg_id, img_url, caption,
                     img_hash, media_type, phash, int(is_album)),
                )
                row = cur.fetchone()
                db.commit()
                return row[0] if row else None
            else:
                cur = db.execute(
                    "INSERT OR IGNORE INTO posts "
                    "(channel, msg_id, img_url, caption, img_hash, media_type, phash, is_album) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (channel, msg_id, img_url, caption,
                     img_hash, media_type, phash, int(is_album)),
                )
                db.commit()
                return cur.lastrowid if cur.lastrowid else None
    except Exception as e:
        logging.error(f"db_save_post: {e}")
    return None


def db_save_album_media(post_id: int, urls: list):
    """urls = [img_url, ...] — binary data is never stored, downloaded on demand."""
    with db_open() as db:
        db.executemany(
            "INSERT OR IGNORE INTO album_media (post_id, idx, img_url) VALUES (?,?,?)",
            [(post_id, i, url) for i, url in enumerate(urls)]
        )
        db.commit()


def db_get_album_media(post_id: int) -> list:
    """Возвращает [(idx, img_url, img_data, file_id), ...]"""
    with db_open() as db:
        return db.execute(
            "SELECT idx, img_url, img_data, file_id FROM album_media WHERE post_id=? ORDER BY idx",
            (post_id,)
        ).fetchall()


def db_save_album_file_ids(post_id: int, file_ids: list):
    with db_open() as db:
        for i, fid in enumerate(file_ids):
            if fid:
                db.execute(
                    "UPDATE album_media SET file_id=? WHERE post_id=? AND idx=?",
                    (fid, post_id, i)
                )
        db.commit()


def db_update(post_id: int, status: str) -> bool:
    with db_open() as db:
        cur = db.execute("UPDATE posts SET status=? WHERE id=?", (status, post_id))
        if status in ("posted", "skipped", "error"):
            db.execute("UPDATE posts SET img_data=NULL, file_id=NULL WHERE id=?", (post_id,))
        db.commit()
        return cur.rowcount > 0


def db_update_caption(post_id: int, caption: str):
    with db_open() as db:
        db.execute("UPDATE posts SET user_caption=? WHERE id=?", (caption, post_id))
        db.commit()


def db_save_img_data(post_id: int, img_data: bytes):
    with db_open() as db:
        db.execute("UPDATE posts SET img_data=? WHERE id=?", (img_data, post_id))
        db.commit()


def db_save_file_id(post_id: int, file_id: str):
    with db_open() as db:
        db.execute("UPDATE posts SET file_id=? WHERE id=?", (file_id, post_id))
        db.commit()


def db_save_msg_id(post_id: int, msg_id: int):
    with db_open() as db:
        db.execute("UPDATE posts SET tg_msg_id=? WHERE id=?", (msg_id, post_id))
        db.commit()


def db_queue_size() -> int:
    with db_open() as db:
        return db.execute("SELECT COUNT(*) FROM posts WHERE status='approved'").fetchone()[0]


def db_get_new_posts() -> list:
    with db_open() as db:
        return db.execute(
            "SELECT id, channel, msg_id, img_url, caption, media_type, is_album "
            "FROM posts WHERE status='new' AND added_at > datetime('now', '-48 hours') "
            "ORDER BY added_at ASC LIMIT ?",
            (MAX_SEND_PER_FETCH,)
        ).fetchall()


# ── Каналы ────────────────────────────────────────────────────────────

def db_get_channels() -> list:
    with db_open() as db:
        return db.execute(
            "SELECT username, trusted FROM channels ORDER BY username"
        ).fetchall()


def db_add_channel(username: str, trusted: bool = False) -> bool:
    try:
        with db_open() as db:
            db.execute(
                "INSERT OR IGNORE INTO channels (username, trusted) VALUES (?, ?)",
                (username, int(trusted))
            )
            db.commit()
        return True
    except Exception as e:
        logging.error(f"db_add_channel: {e}")
        return False


def db_remove_channel(username: str) -> bool:
    with db_open() as db:
        cur = db.execute("DELETE FROM channels WHERE username=?", (username,))
        db.commit()
        return cur.rowcount > 0


def db_set_trusted(username: str, trusted: bool) -> bool:
    with db_open() as db:
        cur = db.execute(
            "UPDATE channels SET trusted=? WHERE username=?", (int(trusted), username)
        )
        db.commit()
        return cur.rowcount > 0


def db_is_trusted(username: str) -> bool:
    with db_open() as db:
        row = db.execute(
            "SELECT trusted FROM channels WHERE username=?", (username,)
        ).fetchone()
        return bool(row and row[0])

# ─────────────────────────────────────────────────────────────────────
#  ХЕЛПЕРЫ МЕДИА
# ─────────────────────────────────────────────────────────────────────

async def ensure_img_data(session: aiohttp.ClientSession, post_id: int):
    """No-op: img_data is no longer stored in the database."""
    pass


async def send_media(bot, chat_id, media_type: str, data,
                     caption: Optional[str], reply_markup=None):
    """Отправить одиночное фото / видео / анимацию."""
    kwargs = dict(chat_id=chat_id, caption=caption or None, reply_markup=reply_markup)
    if media_type == "video":
        return await bot.send_video(video=data, **kwargs)
    elif media_type == "animation":
        return await bot.send_animation(animation=data, **kwargs)
    else:
        return await bot.send_photo(photo=data, **kwargs)


def get_file_id(msg, media_type: str) -> Optional[str]:
    if media_type == "video":
        return msg.video.file_id if msg.video else None
    elif media_type == "animation":
        return msg.animation.file_id if msg.animation else None
    else:
        return msg.photo[-1].file_id if msg.photo else None


async def build_input_media(session, post_id: int,
                             caption: Optional[str]) -> list:
    """Собрать список InputMedia для send_media_group (альбомы)."""
    items = db_get_album_media(post_id)
    if not items:
        return []
    with db_open() as db:
        row = db.execute("SELECT media_type FROM posts WHERE id=?", (post_id,)).fetchone()
    media_type = (row[0] if row else None) or "photo"

    result = []
    for i, (idx, img_url, img_data, file_id) in enumerate(items):
        if file_id:
            data = file_id
        else:
            raw = await download_media(session, img_url)
            if not raw:
                continue
            data = BytesIO(raw)
        item_caption = caption if i == 0 else None
        if media_type == "video":
            result.append(InputMediaVideo(media=data, caption=item_caption))
        else:
            result.append(InputMediaPhoto(media=data, caption=item_caption))
    return result


async def send_album_to_chat(bot, session, chat_id: int, post_id: int,
                              caption: Optional[str], reply_markup=None):
    """Отправить альбом в чат. Возвращает (список msg, msg с клавиатурой или None)."""
    media_list = await build_input_media(session, post_id, caption)
    if not media_list:
        return [], None
    if len(media_list) == 1:
        # Telegram не принимает media_group из 1 элемента
        msg = await send_media(bot, chat_id, "photo", media_list[0].media,
                               caption, reply_markup)
        return [msg], msg

    sent_msgs = await bot.send_media_group(chat_id=chat_id, media=media_list)
    keyboard_msg = None
    if reply_markup:
        keyboard_msg = await bot.send_message(
            chat_id=chat_id,
            text=f"⬆️ Альбом: {len(sent_msgs)} фото",
            reply_markup=reply_markup
        )
    return sent_msgs, keyboard_msg

# ─────────────────────────────────────────────────────────────────────
#  РАСПИСАНИЕ
# ─────────────────────────────────────────────────────────────────────

def make_schedule() -> list:
    now   = datetime.now(MSK)
    n     = random.randint(POSTS_PER_DAY_MIN, POSTS_PER_DAY_MAX)
    start = now.replace(hour=POST_START_HOUR, minute=0, second=0, microsecond=0)
    end   = now.replace(hour=POST_END_HOUR,   minute=0, second=0, microsecond=0)
    if now >= end:
        start += timedelta(days=1)
        end   += timedelta(days=1)
    total = int((end - start).total_seconds())
    times = sorted(random.sample(range(0, total + 1), min(n, total + 1)))
    return [t for t in [start + timedelta(seconds=s) for s in times] if t > now]

# ─────────────────────────────────────────────────────────────────────
#  БОТ
# ─────────────────────────────────────────────────────────────────────

class MemeBot:
    def __init__(self):
        self.app: Optional[Application] = None
        self.schedule             = []
        self.last_fetch           = None
        self.current_day          = None
        self.pending_caption      = None
        self.pending_edit_caption = None
        self.session: Optional[aiohttp.ClientSession] = None
        self._post_lock: Optional[asyncio.Lock] = None
        self._fetch_semaphore: Optional[asyncio.Semaphore] = None
        pass  # DB init moved to run() after health server starts

    def _setup_app(self):
        self.app = Application.builder().token(BOT_TOKEN).build()

        self.app.add_handler(CommandHandler("start",         self.cmd_start))
        self.app.add_handler(CommandHandler("help",          self.cmd_help))
        self.app.add_handler(CommandHandler("queue",         self.cmd_queue))
        self.app.add_handler(CommandHandler("post",          self.cmd_post))
        self.app.add_handler(CommandHandler("fetch",         self.cmd_fetch))
        self.app.add_handler(CommandHandler("skip",          self.cmd_skip_caption))
        self.app.add_handler(CommandHandler("status",        self.cmd_status))
        self.app.add_handler(CommandHandler("schedule",      self.cmd_schedule))
        self.app.add_handler(CommandHandler("clearqueue",    self.cmd_clearqueue))
        self.app.add_handler(CommandHandler("clearsent",     self.cmd_clearsent))
        self.app.add_handler(CommandHandler("showqueue",     self.cmd_showqueue))
        self.app.add_handler(CommandHandler("addchannel",    self.cmd_addchannel))
        self.app.add_handler(CommandHandler("removechannel", self.cmd_removechannel))
        self.app.add_handler(CommandHandler("listchannels",  self.cmd_listchannels))
        self.app.add_handler(CommandHandler("trustchannel",  self.cmd_trustchannel))
        self.app.add_handler(CommandHandler("vacuum",         self.cmd_vacuum))
        self.app.add_handler(CallbackQueryHandler(self.on_button))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.on_text))

    # ── Команды ──────────────────────────────────────────────────────

    async def cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = str(update.effective_chat.id)
        db_set("admin_chat_id", chat_id)
        await update.message.reply_text(
            "Привет! Я буду присылать сюда мемы для одобрения.\n\n"
            "✅ — одобрить  |  🚀 — опубликовать сразу  |  ✍️ — с подписью  |  ❌ — пропустить\n\n"
            "Напиши /help чтобы увидеть все команды.\n\n"
            f"Твой Telegram ID: `{chat_id}`",
            parse_mode="Markdown"
        )

    async def cmd_help(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "*Кнопки при одобрении:*\n"
            "✅ — добавить в очередь без подписи\n"
            "🚀 — опубликовать прямо сейчас\n"
            "✍️ — добавить в очередь с подписью\n"
            "❌ — пропустить мем\n\n"
            "*Основные команды:*\n"
            "/fetch — проверить все каналы и прислать новые мемы\n"
            "/post — опубликовать следующий мем из очереди вручную\n"
            "/queue — сколько мемов ждут публикации\n"
            "/showqueue — показать очередь (публикация, подпись, удаление)\n"
            "/schedule — расписание публикаций на сегодня\n"
            "/status — статистика базы\n\n"
            "*Управление каналами:*\n"
            "/listchannels — список каналов-источников\n"
            "/addchannel @username — добавить канал\n"
            "/removechannel @username — удалить канал\n"
            "/trustchannel @username — авто-одобрение постов из этого канала\n\n"
            "*Служебные:*\n"
            "/skip — одобрить/убрать подпись (когда бот ждёт текст)\n"
            "/clearsent — сбросить все непросмотренные мемы\n"
            "/clearqueue — удалить из очереди битые посты\n"
            "/start — зарегистрировать этот чат как admin",
            parse_mode="Markdown"
        )

    async def cmd_queue(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        n     = db_queue_size()
        times = ", ".join(t.strftime("%H:%M") for t in self.schedule) or "нет"
        await update.message.reply_text(
            f"В очереди: {n} мемов\nРасписание на сегодня: {times}"
        )

    async def cmd_schedule(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        now = datetime.now(MSK)
        if not self.schedule:
            await update.message.reply_text("На сегодня слоты закончились. Новое расписание — завтра в 09:00.")
            return
        lines = []
        for t in self.schedule:
            delta = t - now
            mins  = int(delta.total_seconds() // 60)
            until = f"через {mins} мин" if mins < 60 else f"через {mins // 60} ч {mins % 60} мин"
            lines.append(f"  {t.strftime('%H:%M')} ({until})")
        queue_n = db_queue_size()
        text = (
            f"Расписание на сегодня ({len(self.schedule)} слотов):\n"
            + "\n".join(lines)
            + f"\n\nВ очереди: {queue_n} мемов"
        )
        if queue_n < len(self.schedule):
            text += f"\n⚠️ Мемов меньше чем слотов — одобри ещё {len(self.schedule) - queue_n}"
        await update.message.reply_text(text)

    async def cmd_fetch(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Проверяю каналы, подожди...")
        try:
            await self.fetch_and_notify()
            await self.resend_pending()
        except Exception as e:
            logging.error(f"cmd_fetch error: {e}", exc_info=True)
            await update.message.reply_text(f"⚠️ Ошибка при фетче: {e}")
            return
        await update.message.reply_text(f"Готово! В очереди одобрено: {db_queue_size()}")

    async def cmd_post(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if db_queue_size() == 0:
            await update.message.reply_text("Очередь пуста — одобри мемы кнопкой ✅")
            return
        if self._post_lock.locked():
            await update.message.reply_text("Публикация уже идёт, подожди секунду.")
            return
        await update.message.reply_text("Публикую...")
        async with self._post_lock:
            ok, published, err = await self.post_next()
        if not ok:
            await update.message.reply_text(f"❌ Ошибка публикации: {err}")
        elif published:
            await update.message.reply_text(f"Опубликовано! Осталось в очереди: {db_queue_size()}")
        else:
            await update.message.reply_text("Не удалось опубликовать — у всех постов пропало медиа. Попробуй /fetch.")

    async def cmd_clearqueue(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        with db_open() as db:
            # Одиночные посты без URL и без file_id
            n1 = db.execute(
                "SELECT COUNT(*) FROM posts WHERE status='approved' AND is_album=0 "
                "AND file_id IS NULL AND img_url IS NULL"
            ).fetchone()[0]
            db.execute(
                "UPDATE posts SET status='skipped' WHERE status='approved' AND is_album=0 "
                "AND file_id IS NULL AND img_url IS NULL"
            )
            # Альбомы без записей в album_media
            album_ids = [r[0] for r in db.execute(
                "SELECT id FROM posts WHERE status='approved' AND is_album=1"
            ).fetchall()]
            n2 = 0
            for aid in album_ids:
                has_data = db.execute(
                    "SELECT 1 FROM album_media WHERE post_id=? AND (img_url IS NOT NULL OR file_id IS NOT NULL) LIMIT 1",
                    (aid,)
                ).fetchone()
                if not has_data:
                    db.execute("UPDATE posts SET status='skipped' WHERE id=?", (aid,))
                    n2 += 1
            db.commit()
        await update.message.reply_text(
            f"Убрано битых постов: {n1 + n2}\n"
            f"В очереди осталось: {db_queue_size()}\n\n"
            f"Теперь напиши /fetch — одобри новые мемы и они запостятся."
        )

    async def cmd_showqueue(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        with db_open() as db:
            total = db.execute("SELECT COUNT(*) FROM posts WHERE status='approved'").fetchone()[0]
            rows  = db.execute(
                "SELECT id, user_caption, file_id, img_url, channel, msg_id, media_type, is_album "
                "FROM posts WHERE status='approved' ORDER BY added_at ASC LIMIT ?",
                (SHOWQUEUE_LIMIT,)
            ).fetchall()
        if not rows:
            await update.message.reply_text("Очередь пуста.")
            return
        header = f"В очереди {total} мемов"
        if total > SHOWQUEUE_LIMIT:
            header += f" (показываю первые {SHOWQUEUE_LIMIT}):"
        else:
            header += ":"
        await update.message.reply_text(header)

        for post_id, caption, file_id, img_url, channel, msg_id, media_type, is_album in rows:
            media_type = media_type or "photo"
            keyboard   = InlineKeyboardMarkup([[
                InlineKeyboardButton("🚀 Опубликовать", callback_data=f"now:{post_id}"),
                InlineKeyboardButton("✏️ Подпись",      callback_data=f"editcap:{post_id}"),
                InlineKeyboardButton("❌ Убрать",        callback_data=f"unqueue:{post_id}"),
            ]])
            text = caption or f"@{channel}"

            try:
                if is_album:
                    _, km = await send_album_to_chat(
                        self.app.bot, self.session,
                        update.effective_chat.id, post_id, text, keyboard
                    )
                    if km is None:
                        await update.message.reply_text(f"#{post_id} {text} — медиа недоступно",
                                                        reply_markup=keyboard)
                else:
                    sources = []
                    if file_id:
                        sources.append(file_id)
                    sent = False
                    for src in sources:
                        try:
                            await send_media(self.app.bot, update.effective_chat.id,
                                             media_type, src, text, keyboard)
                            sent = True
                            break
                        except Exception as e:
                            logging.warning(f"cmd_showqueue пост {post_id}: {e}")
                    if not sent:
                        raw = (await download_media(self.session, img_url) or
                               await refetch_media(self.session, channel, msg_id, media_type))
                        if raw:
                            await send_media(self.app.bot, update.effective_chat.id,
                                             media_type, BytesIO(raw), text, keyboard)
                        else:
                            await update.message.reply_text(
                                f"#{post_id} {text} — медиа недоступно", reply_markup=keyboard
                            )
            except Exception as e:
                logging.error(f"cmd_showqueue: {e}")
            await asyncio.sleep(0.3)

        if total > SHOWQUEUE_LIMIT:
            await update.message.reply_text(f"...ещё {total - SHOWQUEUE_LIMIT} мемов.")

    async def cmd_clearsent(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        with db_open() as db:
            n = db.execute(
                "SELECT COUNT(*) FROM posts WHERE status IN ('new', 'sent')"
            ).fetchone()[0]
            db.execute("UPDATE posts SET status='skipped' WHERE status IN ('new', 'sent')")
            db.commit()
        await update.message.reply_text(
            f"Сброшено {n} постов.\nНапиши /fetch чтобы загрузить свежие."
        )

    async def cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        with db_open() as db:
            new_cnt      = db.execute("SELECT COUNT(*) FROM posts WHERE status='new'").fetchone()[0]
            approved_cnt = db.execute("SELECT COUNT(*) FROM posts WHERE status='approved'").fetchone()[0]
            skipped_cnt  = db.execute("SELECT COUNT(*) FROM posts WHERE status='skipped'").fetchone()[0]
            posted_cnt   = db.execute("SELECT COUNT(*) FROM posts WHERE status='posted'").fetchone()[0]
        channels    = db_get_channels()
        trusted_cnt = sum(1 for _, t in channels if t)
        await update.message.reply_text(
            f"📊 Статистика:\n"
            f"🆕 Новых (не просмотрено): {new_cnt}\n"
            f"✅ В очереди (одобрено): {approved_cnt}\n"
            f"❌ Пропущено: {skipped_cnt}\n"
            f"📤 Опубликовано: {posted_cnt}\n\n"
            f"📡 Каналов-источников: {len(channels)} (авто: {trusted_cnt})"
        )

    async def cmd_vacuum(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Чищу базу...")
        with db_open() as db:
            freed = db.execute(
                "UPDATE posts SET img_data=NULL, file_id=NULL "
                "WHERE status IN ('posted','skipped','error') AND img_data IS NOT NULL"
            ).rowcount
            deleted = db.execute(
                "DELETE FROM posts WHERE status IN ('posted','skipped','error') "
                "AND added_at < datetime('now', '-7 days')"
            ).rowcount
            db.execute("VACUUM")
            db.commit()
        await update.message.reply_text(
            f"✅ Готово:\n"
            f"  • Очищено img_data: {freed} постов\n"
            f"  • Удалено старых записей: {deleted}"
        )

    # ── Управление каналами ───────────────────────────────────────────

    @staticmethod
    def _parse_channel_arg(args: list) -> Optional[str]:
        if not args:
            return None
        return args[0].lstrip("@").strip() or None

    async def cmd_addchannel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        username = self._parse_channel_arg(ctx.args)
        if not username:
            await update.message.reply_text(
                "Использование: /addchannel @username\nПример: /addchannel meduzaio"
            )
            return
        if db_add_channel(username):
            await update.message.reply_text(
                f"✅ Канал @{username} добавлен.\n"
                f"Для авто-одобрения: /trustchannel {username}"
            )
        else:
            await update.message.reply_text(f"❌ Не удалось добавить @{username}")

    async def cmd_removechannel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        username = self._parse_channel_arg(ctx.args)
        if not username:
            await update.message.reply_text("Использование: /removechannel @username")
            return
        if db_remove_channel(username):
            await update.message.reply_text(f"✅ Канал @{username} удалён.")
        else:
            await update.message.reply_text(f"Канал @{username} не найден.")

    async def cmd_listchannels(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        channels = db_get_channels()
        if not channels:
            await update.message.reply_text("Список каналов пуст. Добавь через /addchannel")
            return
        lines = [f"@{u}{'  🤖 авто' if t else ''}" for u, t in sorted(channels)]
        await update.message.reply_text(
            f"📡 Каналов-источников: {len(channels)}\n\n" + "\n".join(lines)
        )

    async def cmd_trustchannel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        username = self._parse_channel_arg(ctx.args)
        if not username:
            await update.message.reply_text(
                "Использование: /trustchannel @username\n"
                "Чтобы отключить: /trustchannel @username off"
            )
            return
        enable = not (len(ctx.args) > 1 and ctx.args[1].lower() in ("off", "0", "нет"))
        if db_set_trusted(username, enable):
            if enable:
                await update.message.reply_text(
                    f"🤖 @{username} помечен доверенным — посты будут авто-одобряться."
                )
            else:
                await update.message.reply_text(f"✅ Авто-одобрение для @{username} отключено.")
        else:
            await update.message.reply_text(
                f"Канал @{username} не найден. Сначала добавь через /addchannel."
            )

    # ── Кнопки одобрения ─────────────────────────────────────────────

    async def on_button(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        try:
            await query.answer()
        except Exception:
            pass

        if query.data == "noop":
            return

        parts = query.data.split(":", 1)
        if len(parts) != 2:
            return
        action, post_id = parts
        try:
            post_id = int(post_id)
        except ValueError:
            return

        if action == "approve":
            with db_open() as _db:
                row = _db.execute("SELECT status FROM posts WHERE id=?", (post_id,)).fetchone()
            if not row or row[0] == "posted":
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("📤 Уже опубликован", callback_data="noop")]])
                )
                return
            if row[0] == "approved":
                await query.answer("Уже в очереди!", show_alert=False)
                return
            db_update(post_id, "approved")
            await ensure_img_data(self.session, post_id)
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[InlineKeyboardButton("✅ Одобрен", callback_data="noop")]])
            )
            await query.message.reply_text(f"✅ Добавлено в очередь! В очереди: {db_queue_size()}")

        elif action == "caption":
            with db_open() as _db:
                row = _db.execute("SELECT status FROM posts WHERE id=?", (post_id,)).fetchone()
            if not row or row[0] == "posted":
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("📤 Уже опубликован", callback_data="noop")]])
                )
                return
            if self.pending_caption and self.pending_caption != post_id:
                db_update(self.pending_caption, "approved")
            self.pending_caption = post_id
            db_set("pending_caption", str(post_id))
            await ensure_img_data(self.session, post_id)
            if self.pending_caption != post_id:
                return
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[InlineKeyboardButton("✏️ Жду подпись...", callback_data="noop")]])
            )
            await query.message.reply_text("Напиши подпись для мема (или /skip чтобы без подписи):")

        elif action == "editcap":
            if self.pending_edit_caption and self.pending_edit_caption != post_id:
                self.pending_edit_caption = None
            self.pending_edit_caption = post_id
            with db_open() as _db:
                row = _db.execute("SELECT user_caption FROM posts WHERE id=?", (post_id,)).fetchone()
            current = (row[0] if row and row[0] else "нет") if row else "нет"
            await query.message.reply_text(
                f"Текущая подпись: _{current}_\n\nНапиши новую подпись, или /skip чтобы убрать её:",
                parse_mode="Markdown"
            )

        elif action == "now":
            with db_open() as _db:
                row = _db.execute("SELECT status FROM posts WHERE id=?", (post_id,)).fetchone()
            if not row or row[0] == "posted":
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("📤 Уже опубликован", callback_data="noop")]])
                )
                return
            db_update(post_id, "approved")
            await ensure_img_data(self.session, post_id)
            if self._post_lock.locked():
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("✅ В очереди (публикация идёт)", callback_data="noop")]])
                )
                await query.message.reply_text("Публикация уже идёт — мем добавлен в очередь и выйдет следующим.")
                return
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Публикую...", callback_data="noop")]])
            )
            async with self._post_lock:
                ok, published, err = await self.post_next(priority_id=post_id)
            if ok and published:
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Опубликован!", callback_data="noop")]])
                )
            elif ok and published is None:
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("📤 Уже опубликован", callback_data="noop")]])
                )
            elif not ok:
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("❌ Ошибка", callback_data="noop")]])
                )
                await query.message.reply_text(f"❌ Ошибка публикации: {err}")
            else:
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("⚠️ Медиа пропало", callback_data="noop")]])
                )
                await query.message.reply_text("Медиа пропало — пост не опубликован. Попробуй /fetch.")

        elif action == "unqueue":
            db_update(post_id, "skipped")
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[InlineKeyboardButton("❌ Убран из очереди", callback_data="noop")]])
            )
            await query.message.reply_text(f"Убрано. В очереди осталось: {db_queue_size()}")

        elif action == "skip":
            db_update(post_id, "skipped")
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[InlineKeyboardButton("❌ Пропущен", callback_data="noop")]])
            )

    async def on_text(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if self.pending_edit_caption is not None:
            post_id = self.pending_edit_caption
            self.pending_edit_caption = None
            caption = update.message.text.strip()
            db_update_caption(post_id, caption)
            await update.message.reply_text(
                f"✅ Подпись обновлена:\n_{caption}_", parse_mode="Markdown"
            )
            return

        post_id = self.pending_caption
        if post_id is None:
            return
        self.pending_caption = None
        db_set("pending_caption", "")
        caption = update.message.text.strip()
        db_update_caption(post_id, caption)
        db_update(post_id, "approved")
        await ensure_img_data(self.session, post_id)
        await update.message.reply_text(
            f"✅ Добавлено в очередь с подписью:\n_{caption}_\n\nВ очереди: {db_queue_size()}",
            parse_mode="Markdown"
        )

    async def cmd_skip_caption(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if self.pending_edit_caption is not None:
            post_id = self.pending_edit_caption
            self.pending_edit_caption = None
            db_update_caption(post_id, "")
            await update.message.reply_text("✅ Подпись убрана.")
            return
        post_id = self.pending_caption
        if post_id is None:
            await update.message.reply_text("Нет мема ожидающего подпись.")
            return
        self.pending_caption = None
        db_set("pending_caption", "")
        db_update(post_id, "approved")
        await ensure_img_data(self.session, post_id)
        await update.message.reply_text(f"✅ Добавлено в очередь без подписи. В очереди: {db_queue_size()}")

    # ── Утилиты ──────────────────────────────────────────────────────

    async def _try_update_admin_markup(self, post_id: int, button_text: str):
        try:
            with db_open() as _db:
                row = _db.execute("SELECT tg_msg_id FROM posts WHERE id=?", (post_id,)).fetchone()
            if not row or not row[0]:
                return
            admin_id = db_get("admin_chat_id")
            if not admin_id:
                return
            await self.app.bot.edit_message_reply_markup(
                chat_id=admin_id,
                message_id=row[0],
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(button_text, callback_data="noop")
                ]]),
            )
        except Exception:
            pass

    async def resend_pending(self):
        admin_id = db_get("admin_chat_id")
        if not admin_id:
            return
        rows = db_get_new_posts()
        for post_id, channel, msg_id, img_url, caption, media_type, is_album in rows:
            media_type = media_type or "photo"
            label      = f"📌 @{channel}"
            text       = f"{caption}\n\n{label}" if caption else label
            keyboard   = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅", callback_data=f"approve:{post_id}"),
                InlineKeyboardButton("🚀", callback_data=f"now:{post_id}"),
                InlineKeyboardButton("✍️", callback_data=f"caption:{post_id}"),
                InlineKeyboardButton("❌", callback_data=f"skip:{post_id}"),
            ]])
            try:
                if is_album:
                    _, km = await send_album_to_chat(
                        self.app.bot, self.session, int(admin_id), post_id, text, keyboard
                    )
                    if km:
                        db_save_msg_id(post_id, km.message_id)
                    else:
                        logging.warning(f"resend_pending: альбом {post_id} не удалось отправить")
                        continue
                else:
                    img = (await download_media(self.session, img_url) or
                           await refetch_media(self.session, channel, msg_id, media_type))
                    if not img:
                        logging.warning(f"resend_pending: пост {post_id} медиа недоступно")
                        continue
                    sent_msg = await send_media(self.app.bot, admin_id, media_type,
                                                BytesIO(img), text, keyboard)
                    fid = get_file_id(sent_msg, media_type)
                    if fid:
                        db_save_file_id(post_id, fid)
                    db_save_msg_id(post_id, sent_msg.message_id)
                db_update(post_id, "sent")
                await asyncio.sleep(0.5)
            except Exception as e:
                logging.error(f"resend_pending: {e}")

    # ── Сбор и отправка мемов ────────────────────────────────────────

    async def fetch_and_notify(self):
        admin_id = db_get("admin_chat_id")
        if not admin_id:
            logging.warning("Нет admin_chat_id — напиши /start боту в личку")
            return

        channels = db_get_channels()
        if not channels:
            logging.warning("Список каналов пуст — добавь через /addchannel")
            return

        channel_names = [ch for ch, _ in channels]
        logging.info(f"Проверяю {len(channel_names)} каналов параллельно...")

        results = await asyncio.gather(
            *[fetch_channel(self.session, ch, self._fetch_semaphore) for ch in channel_names],
            return_exceptions=True,
        )

        sent = 0
        skipped_limit = 0
        for channel, posts in zip(channel_names, results):
            if isinstance(posts, Exception):
                logging.error(f"Ошибка парсинга {channel}: {posts}")
                continue
            is_trusted = db_is_trusted(channel)
            for post in posts:
                # Скачиваем первое медиа для дедупликации
                first_img = await download_media(self.session, post["media_url"])
                if not first_img:
                    continue

                img_hash = hashlib.md5(first_img).hexdigest()
                if db_hash_exists(img_hash):
                    logging.debug(f"Дубликат (MD5) из @{post['channel']}, пропускаю")
                    continue

                phash_str = None
                if post["media_type"] == "photo":
                    loop = asyncio.get_event_loop()
                    phash_str = await loop.run_in_executor(None, compute_phash, first_img)
                    if phash_str and db_phash_is_duplicate(phash_str):
                        logging.debug(f"Дубликат (pHash) из @{post['channel']}, пропускаю")
                        continue

                post_id = db_save_post(
                    post["channel"], post["msg_id"],
                    post["media_url"], post["caption"],
                    img_hash, post["media_type"], phash_str,
                    is_album=post["is_album"],
                )
                if not post_id:
                    continue

                # Для альбомов — сохраняем только URL-адреса
                if post["is_album"]:
                    db_save_album_media(post_id, post["media_urls"])

                if is_trusted:
                    db_update(post_id, "approved")
                    logging.info(f"Авто-одобрен пост {post_id} из @{channel}")
                    continue

                if sent >= MAX_SEND_PER_FETCH:
                    skipped_limit += 1
                    continue

                try:
                    label    = f"📌 @{channel}"
                    text     = f"{post['caption']}\n\n{label}" if post["caption"] else label
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("✅", callback_data=f"approve:{post_id}"),
                        InlineKeyboardButton("🚀", callback_data=f"now:{post_id}"),
                        InlineKeyboardButton("✍️", callback_data=f"caption:{post_id}"),
                        InlineKeyboardButton("❌", callback_data=f"skip:{post_id}"),
                    ]])

                    if post["is_album"]:
                        _, km = await send_album_to_chat(
                            self.app.bot, self.session, int(admin_id), post_id, text, keyboard
                        )
                        if km:
                            db_save_msg_id(post_id, km.message_id)
                    else:
                        sent_msg = await send_media(self.app.bot, admin_id, post["media_type"],
                                                    BytesIO(first_img), text, keyboard)
                        fid = get_file_id(sent_msg, post["media_type"])
                        if fid:
                            db_save_file_id(post_id, fid)
                        db_save_msg_id(post_id, sent_msg.message_id)

                    db_update(post_id, "sent")
                    sent += 1
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logging.error(f"Ошибка отправки в личку: {e}")

        logging.info(
            f"Отправлено на проверку: {sent}"
            + (f", отложено (лимит): {skipped_limit}" if skipped_limit else "")
        )
        if skipped_limit:
            try:
                await self.app.bot.send_message(
                    chat_id=admin_id,
                    text=f"Показано {sent} из {sent + skipped_limit} новых мемов. "
                         f"Одобри их и нажми /fetch чтобы получить остальные {skipped_limit}.",
                )
            except Exception:
                pass
        self.last_fetch = datetime.now(MSK)

    # ── Публикация в канал ───────────────────────────────────────────

    async def post_next(self, priority_id: Optional[int] = None):
        """Возвращает (ok, published, err)."""
        with db_open() as _db:
            if priority_id is not None:
                rows = _db.execute(
                    "SELECT id, channel, msg_id, img_url, user_caption, file_id, media_type, is_album "
                    "FROM posts WHERE status='approved' AND id=?",
                    (priority_id,)
                ).fetchall()
                if not rows:
                    already = _db.execute(
                        "SELECT status FROM posts WHERE id=?", (priority_id,)
                    ).fetchone()
                    if already and already[0] == "posted":
                        return True, None, None
            else:
                rows = _db.execute(
                    "SELECT id, channel, msg_id, img_url, user_caption, file_id, media_type, is_album "
                    "FROM posts WHERE status='approved' ORDER BY added_at ASC"
                ).fetchall()

        if not rows:
            logging.warning("Очередь пуста, пропускаю слот")
            return True, False, None

        for post_id, channel, msg_id, img_url, caption, file_id, media_type, is_album in rows:
            media_type = media_type or "photo"
            try:
                if is_album:
                    media_list = await build_input_media(self.session, post_id, caption)
                    if not media_list:
                        logging.warning(f"Альбом {post_id}: нет медиа, пропускаю")
                        db_update(post_id, "skipped")
                        await self._try_update_admin_markup(post_id, "⚠️ Медиа пропало")
                        continue
                    if len(media_list) == 1:
                        await send_media(self.app.bot, MY_CHANNEL, media_type,
                                         media_list[0].media, caption)
                    else:
                        sent_msgs = await self.app.bot.send_media_group(
                            chat_id=MY_CHANNEL, media=media_list
                        )
                        # Сохраняем file_ids для будущих публикаций
                        fids = []
                        for sm in sent_msgs:
                            fids.append(
                                sm.photo[-1].file_id if sm.photo else
                                (sm.video.file_id if sm.video else None)
                            )
                        db_save_album_file_ids(post_id, fids)
                else:
                    sources = []
                    if file_id:
                        sources.append(file_id)
                    if not sources:
                        raw = (await download_media(self.session, img_url) or
                               await refetch_media(self.session, channel, msg_id, media_type))
                        if not raw:
                            logging.warning(f"Пост {post_id}: медиа недоступно, пропускаю")
                            db_update(post_id, "skipped")
                            await self._try_update_admin_markup(post_id, "⚠️ Медиа пропало")
                            continue
                        sources.append(BytesIO(raw))

                    published = False
                    for src in sources:
                        try:
                            await send_media(self.app.bot, MY_CHANNEL, media_type, src, caption)
                            published = True
                            break
                        except Exception as e:
                            logging.warning(f"Пост {post_id}: источник не сработал: {e}")
                    if not published:
                        db_update(post_id, "skipped")
                        await self._try_update_admin_markup(post_id, "⚠️ Медиа пропало")
                        continue

                with db_open() as _db:
                    _db.execute(
                        "UPDATE posts SET status='posted', posted_at=datetime('now'), img_data=NULL WHERE id=?",
                        (post_id,)
                    )
                    _db.commit()
                logging.info("Мем опубликован в канале")
                await self._try_update_admin_markup(post_id, "📤 Опубликован!")
                return True, True, None

            except Exception as e:
                logging.error(f"Ошибка публикации поста {post_id}: {e}")
                return False, False, str(e)

        logging.warning("Все одобренные посты были битые")
        return True, False, None

    # ── Главный цикл ─────────────────────────────────────────────────

    async def main_loop(self):
        self.current_day = datetime.now(MSK).date()
        self.schedule    = make_schedule()
        logging.info(
            f"Расписание ({len(self.schedule)} постов): "
            + ", ".join(t.strftime("%H:%M") for t in self.schedule)
        )
        self.last_fetch  = datetime.now(MSK)
        self._last_ping  = datetime.now(MSK)

        while True:
            now = datetime.now(MSK)

            if now.date() != self.current_day:
                self.current_day = now.date()
                self.schedule    = make_schedule()
                logging.info("Новый день! Расписание: "
                             + ", ".join(t.strftime("%H:%M") for t in self.schedule))
                try:
                    with db_open() as _db:
                        deleted = _db.execute(
                            "DELETE FROM posts WHERE status IN ('posted','skipped','error') "
                            "AND added_at < datetime('now', '-30 days')"
                        ).rowcount
                        freed = _db.execute(
                            "UPDATE posts SET img_data=NULL, file_id=NULL "
                            "WHERE status IN ('posted','skipped','error') AND img_data IS NOT NULL"
                        ).rowcount
                        expired = _db.execute(
                            "UPDATE posts SET status='skipped' WHERE status IN ('new','sent') "
                            "AND added_at < datetime('now', '-48 hours')"
                        ).rowcount
                        _db.execute("VACUUM")
                        _db.commit()
                    if deleted or freed or expired:
                        logging.info(f"Очистка базы: удалено {deleted}, img_data очищено у {freed}, просрочено {expired}")
                except Exception as e:
                    logging.error(f"Ошибка ежедневной очистки: {e}")

            if self.last_fetch is None or (now - self.last_fetch).total_seconds() >= FETCH_INTERVAL:
                await self.fetch_and_notify()

            if self.schedule and now >= self.schedule[0]:
                self.schedule.pop(0)
                try:
                    async with self._post_lock:
                        ok, published, err = await self.post_next()
                    admin_id = db_get("admin_chat_id")
                    if not ok:
                        logging.error(f"Плановая публикация не удалась: {err}")
                        if admin_id:
                            try:
                                await self.app.bot.send_message(
                                    chat_id=admin_id, text=f"❌ Плановая публикация не удалась: {err}"
                                )
                            except Exception:
                                pass
                    elif published:
                        logging.info("Плановая публикация прошла успешно")
                        if admin_id:
                            next_times = ", ".join(t.strftime("%H:%M") for t in self.schedule) or "больше нет"
                            try:
                                await self.app.bot.send_message(
                                    chat_id=admin_id,
                                    text=f"📤 Опубликовано по расписанию\n"
                                         f"Осталось в очереди: {db_queue_size()}\n"
                                         f"Следующие слоты: {next_times}",
                                )
                            except Exception:
                                pass
                    else:
                        logging.warning("Плановая публикация: все посты битые")
                        if admin_id:
                            try:
                                await self.app.bot.send_message(
                                    chat_id=admin_id,
                                    text="⚠️ Плановая публикация: нет постов с рабочим медиа. "
                                         "Одобри новые через /fetch."
                                )
                            except Exception:
                                pass
                except Exception as e:
                    logging.error(f"Ошибка плановой публикации: {e}", exc_info=True)

            # Self-ping каждые 12 минут чтобы Render не засыпал
            if (now - self._last_ping).total_seconds() >= 720:
                self._last_ping = now
                try:
                    url = f"https://memebot-8tqa.onrender.com/health"
                    async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as _:
                        pass
                except Exception:
                    pass

            await asyncio.sleep(30)

    async def run(self):
        self._post_lock       = asyncio.Lock()
        self._fetch_semaphore = asyncio.Semaphore(8)

        # HTTP health endpoint (нужен для Render free tier)
        async def _health_handler(reader, writer):
            await reader.read(4096)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nOK")
            await writer.drain()
            writer.close()
        _port = int(os.getenv("PORT", 10000))
        _srv = await asyncio.start_server(_health_handler, "0.0.0.0", _port)
        asyncio.get_event_loop().create_task(_srv.serve_forever())
        logging.info(f"Health server on port {_port}")

        emergency_cleanup_db()
        init_db()

        saved = db_get("pending_caption")
        if saved:
            pid = int(saved)
            with db_open() as _db:
                row = _db.execute("SELECT status FROM posts WHERE id=?", (pid,)).fetchone()
            if row and row[0] not in ("posted", "skipped", "error"):
                self.pending_caption = pid
                logging.info(f"Восстановлен pending_caption={pid}")
            else:
                db_set("pending_caption", "")

        self._setup_app()
        connector    = aiohttp.TCPConnector(limit=20)
        self.session = aiohttp.ClientSession(headers=HEADERS, connector=connector)
        await self.app.initialize()
        await self.app.start()
        try:
            try:
                await self.app.bot.get_updates(offset=-1, timeout=0, read_timeout=5)
            except Exception:
                pass
            await asyncio.sleep(2)
            await self.app.updater.start_polling(drop_pending_updates=True, poll_interval=2.0)
            logging.info(
                f"Бот запущен! MY_CHANNEL={MY_CHANNEL!r}  "
                f"BOT_TOKEN={'OK' if BOT_TOKEN else 'ПУСТОЙ'}  DB={DB}"
            )
            await self.main_loop()
        finally:
            logging.info("Завершение...")
            await self.session.close()
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    PID_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.pid")
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE) as _f:
                old_pid = int(_f.read().strip())
            os.kill(old_pid, 0)
            logging.critical(f"Бот уже запущен (PID {old_pid}). Завершаю.")
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            pass

    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    try:
        errors = []
        if not BOT_TOKEN:
            errors.append("BOT_TOKEN не задан в .env")
        if not MY_CHANNEL:
            errors.append("MY_CHANNEL не задан в .env")
        if errors:
            for e in errors:
                logging.critical(f"Ошибка конфига: {e}")
            sys.exit(1)

        asyncio.run(MemeBot().run())
    finally:
        try:
            os.remove(PID_FILE)
        except OSError:
            pass
