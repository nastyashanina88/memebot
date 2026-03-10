"""
Meme Bot — парсит каналы, присылает мемы тебе в личку на одобрение,
публикует одобренные в @yslovnay по расписанию.
"""

import asyncio
import logging
import os
import random
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from io import BytesIO
from typing import Optional

import aiohttp
import pytz
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ─────────────────────────────────────────────────────────────────────
#  НАСТРОЙКИ
# ─────────────────────────────────────────────────────────────────────

SOURCE_CHANNELS = [
    "nedovolnij",
    "membeeeers",
    "vsratessa",
    "meme_division",
    "mynameismem",
    "stolencatsbyolga",
    "memo4ek",
    "memnaya_LR",
    "Leomemesmda",
    "rus_mem",
    "cherdakmemov",
    "Katzen_und_Politik",
    "smilemilf",
    "vsratyikontent",
    "thresomewhitout",
    "impirat",
    "pleasedickann",
    "monologue3",
    "dobriememes",
    "russkiememy",
    "female_memes",
    "drugzahodi",
    "axaxanakanecta",
    "cats_mems",
    "memesfs",
    "grustnie_memi",
    "tasamaDama",
    "plohoy_vladlen",
    "sammychoret",
]

POSTS_PER_DAY_MIN = 7
POSTS_PER_DAY_MAX = 10
POST_START_HOUR   = 9
POST_END_HOUR     = 22
MAX_CAPTION_LEN   = 150
FETCH_INTERVAL    = 3600  # секунды между проверками каналов
FETCH_HOURS_BACK  = 72    # брать посты за последние N часов
MAX_SEND_PER_FETCH = 25   # максимум мемов на одобрение за один /fetch

# ─────────────────────────────────────────────────────────────────────
#  КОНФИГ
# ─────────────────────────────────────────────────────────────────────

BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
MY_CHANNEL     = os.getenv("MY_CHANNEL", "")
ADMIN_CHAT_ID  = os.getenv("ADMIN_CHAT_ID", "")  # задаётся в Railway Variables
MSK            = pytz.timezone("Europe/Moscow")

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

async def fetch_channel(session: aiohttp.ClientSession, channel: str, semaphore: asyncio.Semaphore, hours_back: int = FETCH_HOURS_BACK) -> list:
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
                time_el = msg.find("time")
                if time_el and time_el.get("datetime"):
                    from datetime import timezone
                    try:
                        post_time = datetime.fromisoformat(time_el["datetime"])
                        if post_time.tzinfo is None:
                            post_time = post_time.replace(tzinfo=timezone.utc)
                        if post_time < cutoff:
                            continue
                    except Exception:
                        pass

                # Ищем картинку
                img_url = None
                wrap    = msg.find("a", class_="tgme_widget_message_photo_wrap")
                if wrap:
                    m = re.search(r"url\('(.+?)'\)", wrap.get("style", ""))
                    if m:
                        img_url = m.group(1)

                if not img_url:
                    continue

                # Подпись
                text_el = msg.find("div", class_="tgme_widget_message_text")
                caption = text_el.get_text(separator=" ").strip() if text_el else ""

                if not is_good_post(caption):
                    continue

                posts.append({"channel": channel, "msg_id": msg_id,
                              "img_url": img_url, "caption": caption})

            return posts

        except Exception as e:
            logging.error(f"Ошибка парсинга {channel}: {e}")
            return []

async def download_image(session: aiohttp.ClientSession, url: str) -> Optional[bytes]:
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with session.get(url, timeout=timeout) as r:
            return await r.read() if r.status == 200 else None
    except Exception as e:
        logging.error(f"Ошибка скачивания изображения: {e}")
        return None

async def refetch_image(session: aiohttp.ClientSession, channel: str, msg_id: str) -> Optional[bytes]:
    """Заново достаёт свежий CDN-URL поста и скачивает картинку."""
    try:
        url = f"https://t.me/s/{channel}?before={int(msg_id) + 1}"
        timeout = aiohttp.ClientTimeout(total=10)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            html = await resp.text()
        soup = BeautifulSoup(html, "html.parser")
        for msg in soup.find_all("div", class_="tgme_widget_message"):
            data_post = msg.get("data-post", "")
            if data_post != f"{channel}/{msg_id}":
                continue
            wrap = msg.find("a", class_="tgme_widget_message_photo_wrap")
            if wrap:
                m = re.search(r"url\('(.+?)'\)", wrap.get("style", ""))
                if m:
                    return await download_image(session, m.group(1))
        return None
    except Exception as e:
        logging.error(f"refetch_image {channel}/{msg_id}: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────
#  БАЗА ДАННЫХ
# ─────────────────────────────────────────────────────────────────────

_default_db_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(_default_db_dir, exist_ok=True)
DB = os.path.join(os.getenv("DATA_DIR", _default_db_dir), "memes.db")

def init_db():
    with sqlite3.connect(DB) as db:
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
        # Добавляем колонки если их ещё нет (для старых БД)
        try:
            db.execute("ALTER TABLE posts ADD COLUMN user_caption TEXT")
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE posts ADD COLUMN img_data BLOB")
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE posts ADD COLUMN file_id TEXT")
        except Exception:
            pass
        db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        db.commit()

def db_get(key: str) -> Optional[str]:
    # Для admin_chat_id сначала проверяем переменную окружения
    if key == "admin_chat_id" and ADMIN_CHAT_ID:
        return ADMIN_CHAT_ID
    with sqlite3.connect(DB) as db:
        r = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r[0] if r else None

def db_set(key: str, value: str):
    with sqlite3.connect(DB) as db:
        db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)", (key, value))
        db.commit()

def db_save_post(channel, msg_id, img_url, caption, img_data: Optional[bytes] = None) -> Optional[int]:
    try:
        with sqlite3.connect(DB) as db:
            cur = db.execute(
                "INSERT OR IGNORE INTO posts (channel, msg_id, img_url, caption, img_data) VALUES (?,?,?,?,?)",
                (channel, msg_id, img_url, caption, img_data),
            )
            db.commit()
            if cur.lastrowid:
                return cur.lastrowid
    except Exception as e:
        logging.error(f"db_save_post: {e}")
    return None

def db_update(post_id: int, status: str) -> bool:
    """Возвращает True если строка реально обновилась."""
    with sqlite3.connect(DB) as db:
        cur = db.execute("UPDATE posts SET status=? WHERE id=?", (status, post_id))
        db.commit()
        return cur.rowcount > 0

def db_update_caption(post_id: int, caption: str):
    with sqlite3.connect(DB) as db:
        db.execute("UPDATE posts SET user_caption=? WHERE id=?", (caption, post_id))
        db.commit()

def db_save_img_data(post_id: int, img_data: bytes):
    with sqlite3.connect(DB) as db:
        db.execute("UPDATE posts SET img_data=? WHERE id=?", (img_data, post_id))
        db.commit()

def db_save_file_id(post_id: int, file_id: str):
    with sqlite3.connect(DB) as db:
        db.execute("UPDATE posts SET file_id=? WHERE id=?", (file_id, post_id))
        db.commit()

async def ensure_img_data(session: aiohttp.ClientSession, post_id: int):
    """Гарантирует что байты картинки сохранены — вызывать при одобрении поста."""
    with sqlite3.connect(DB) as db:
        row = db.execute(
            "SELECT channel, msg_id, img_url, img_data FROM posts WHERE id=?", (post_id,)
        ).fetchone()
    if not row:
        return
    channel, msg_id, img_url, img_data = row
    if img_data:
        return  # уже есть
    img = await download_image(session, img_url) or await refetch_image(session, channel, msg_id)
    if img:
        db_save_img_data(post_id, img)
        logging.info(f"Пост {post_id}: байты картинки сохранены при одобрении")

def db_get_approved() -> Optional[tuple]:
    with sqlite3.connect(DB) as db:
        return db.execute(
            "SELECT id, channel, msg_id, img_url, user_caption, img_data, file_id "
            "FROM posts WHERE status='approved' ORDER BY added_at ASC LIMIT 1"
        ).fetchone()


def db_queue_size() -> int:
    with sqlite3.connect(DB) as db:
        return db.execute("SELECT COUNT(*) FROM posts WHERE status='approved'").fetchone()[0]

def db_get_new_posts() -> list:
    """Посты которые в базе но ещё не просмотрены."""
    with sqlite3.connect(DB) as db:
        return db.execute(
            "SELECT id, channel, msg_id, img_url, caption, img_data FROM posts WHERE status='new' ORDER BY added_at ASC LIMIT 30"
        ).fetchall()

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
    times = sorted(random.sample(range(0, total), min(n, total)))
    return [t for t in [start + timedelta(seconds=s) for s in times] if t > now]

# ─────────────────────────────────────────────────────────────────────
#  БОТ
# ─────────────────────────────────────────────────────────────────────

class MemeBot:
    def __init__(self):
        self.app: Optional[Application] = None
        self.schedule         = []
        self.last_fetch       = None
        self.current_day      = None
        self.pending_caption  = None  # post_id ожидающий подписи
        self.session: Optional[aiohttp.ClientSession] = None
        self._post_lock: Optional[asyncio.Lock] = None
        self._fetch_semaphore: Optional[asyncio.Semaphore] = None
        init_db()

    def _setup_app(self):
        """Создаём Application и регистрируем хэндлеры — внутри event loop."""
        self.app = Application.builder().token(BOT_TOKEN).build()
        self.app.add_handler(CommandHandler("start",      self.cmd_start))
        self.app.add_handler(CommandHandler("help",       self.cmd_help))
        self.app.add_handler(CommandHandler("queue",      self.cmd_queue))
        self.app.add_handler(CommandHandler("post",       self.cmd_post))
        self.app.add_handler(CommandHandler("fetch",      self.cmd_fetch))
        self.app.add_handler(CommandHandler("skip",       self.cmd_skip_caption))
        self.app.add_handler(CommandHandler("status",     self.cmd_status))
        self.app.add_handler(CommandHandler("schedule",   self.cmd_schedule))
        self.app.add_handler(CommandHandler("clearqueue", self.cmd_clearqueue))
        self.app.add_handler(CommandHandler("clearsent",  self.cmd_clearsent))
        self.app.add_handler(CommandHandler("showqueue",  self.cmd_showqueue))
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
            "/showqueue — показать очередь с возможностью убрать или опубликовать сразу\n"
            "/schedule — расписание публикаций на сегодня с обратным отсчётом\n"
            "/status — статистика базы (новые, одобренные, пропущенные, опубликованные)\n\n"
            "*Служебные команды:*\n"
            "/skip — одобрить мем без подписи (когда бот ждёт текст подписи)\n"
            "/clearsent — сбросить все непросмотренные мемы (чистый лист перед /fetch)\n"
            "/clearqueue — удалить из очереди битые посты без картинки\n"
            "/start — зарегистрировать этот чат как admin (обычно нужен только один раз)",
            parse_mode="Markdown"
        )

    async def cmd_queue(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        n = db_queue_size()
        times = ", ".join(t.strftime("%H:%M") for t in self.schedule) or "нет"
        await update.message.reply_text(
            f"В очереди: {n} мемов\n"
            f"Расписание на сегодня: {times}"
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
            if mins < 60:
                until = f"через {mins} мин"
            else:
                until = f"через {mins // 60} ч {mins % 60} мин"
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
        """Вручную запустить проверку каналов прямо сейчас."""
        await update.message.reply_text("Проверяю каналы, подожди...")
        await self.fetch_and_notify()
        # Также показать посты которые уже в базе но ещё не просмотрены
        await self.resend_pending()
        n = db_queue_size()
        await update.message.reply_text(f"Готово! В очереди одобрено: {n}")

    async def resend_pending(self):
        """Показать посты которые уже в базе но ещё не просмотрены."""
        admin_id = db_get("admin_chat_id")
        if not admin_id:
            return
        rows = db_get_new_posts()
        for post_id, channel, msg_id, img_url, caption, img_data in rows:
            img = img_data or await download_image(self.session, img_url) or await refetch_image(self.session, channel, msg_id)
            if not img:
                db_update(post_id, "error")
                continue
            try:
                label = f"📌 @{channel}"
                text  = f"{caption}\n\n{label}" if caption else label
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅", callback_data=f"approve:{post_id}"),
                    InlineKeyboardButton("🚀", callback_data=f"now:{post_id}"),
                    InlineKeyboardButton("✍️", callback_data=f"caption:{post_id}"),
                    InlineKeyboardButton("❌", callback_data=f"skip:{post_id}"),
                ]])
                sent_msg = await self.app.bot.send_photo(
                    chat_id=admin_id,
                    photo=BytesIO(img),
                    caption=text,
                    reply_markup=keyboard,
                )
                fid = sent_msg.photo[-1].file_id
                db_save_file_id(post_id, fid)
                db_save_img_data(post_id, img)
                db_update(post_id, "sent")
                await asyncio.sleep(0.5)
            except Exception as e:
                logging.error(f"resend_pending: {e}")

    async def cmd_post(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Вручную опубликовать следующий мем из очереди."""
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
            await update.message.reply_text("Не удалось опубликовать — у всех постов пропала картинка. Попробуй /fetch и одобри новые.")

    async def cmd_clearqueue(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Сбросить все одобренные посты без file_id (битые)."""
        with sqlite3.connect(DB) as db:
            n = db.execute(
                "SELECT COUNT(*) FROM posts WHERE status='approved' AND file_id IS NULL"
            ).fetchone()[0]
            db.execute(
                "UPDATE posts SET status='skipped' WHERE status='approved' AND file_id IS NULL"
            )
            db.commit()
        await update.message.reply_text(
            f"Убрано битых постов: {n}\n"
            f"В очереди осталось: {db_queue_size()}\n\n"
            f"Теперь напиши /fetch — одобри новые мемы и они запостятся."
        )

    async def cmd_showqueue(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Показать все мемы в очереди с кнопкой убрать."""
        with sqlite3.connect(DB) as db:
            rows = db.execute(
                "SELECT id, user_caption, file_id, img_data, img_url, channel, msg_id "
                "FROM posts WHERE status='approved' ORDER BY added_at ASC"
            ).fetchall()
        if not rows:
            await update.message.reply_text("Очередь пуста.")
            return
        await update.message.reply_text(f"В очереди {len(rows)} мемов:")
        for post_id, caption, file_id, img_data, img_url, channel, msg_id in rows:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🚀 Опубликовать сейчас", callback_data=f"now:{post_id}"),
                InlineKeyboardButton("❌ Убрать из очереди", callback_data=f"unqueue:{post_id}"),
            ]])
            text = caption or f"@{channel}"
            try:
                if file_id:
                    await update.message.reply_photo(photo=file_id, caption=text, reply_markup=keyboard)
                elif img_data:
                    await update.message.reply_photo(photo=BytesIO(img_data), caption=text, reply_markup=keyboard)
                else:
                    raw = await download_image(self.session, img_url) or await refetch_image(self.session, channel, msg_id)
                    if raw:
                        await update.message.reply_photo(photo=BytesIO(raw), caption=text, reply_markup=keyboard)
                    else:
                        await update.message.reply_text(f"#{post_id} {text} — картинка недоступна", reply_markup=keyboard)
                await asyncio.sleep(0.3)
            except Exception as e:
                logging.error(f"cmd_showqueue: {e}")

    async def cmd_clearsent(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Сбросить все посты которые уже показаны или ещё не показаны — чистый лист."""
        with sqlite3.connect(DB) as db:
            n = db.execute(
                "SELECT COUNT(*) FROM posts WHERE status IN ('new', 'sent')"
            ).fetchone()[0]
            db.execute("UPDATE posts SET status='skipped' WHERE status IN ('new', 'sent')")
            db.commit()
        await update.message.reply_text(f"Сброшено {n} постов. Бот больше их не пришлёт.\nНапиши /fetch чтобы загрузить свежие.")

    async def cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Показать статистику по базе данных."""
        with sqlite3.connect(DB) as db:
            new_cnt      = db.execute("SELECT COUNT(*) FROM posts WHERE status='new'").fetchone()[0]
            approved_cnt = db.execute("SELECT COUNT(*) FROM posts WHERE status='approved'").fetchone()[0]
            skipped_cnt  = db.execute("SELECT COUNT(*) FROM posts WHERE status='skipped'").fetchone()[0]
            posted_cnt   = db.execute("SELECT COUNT(*) FROM posts WHERE status='posted'").fetchone()[0]
        await update.message.reply_text(
            f"📊 Статистика:\n"
            f"🆕 Новых (не просмотрено): {new_cnt}\n"
            f"✅ В очереди (одобрено): {approved_cnt}\n"
            f"❌ Пропущено: {skipped_cnt}\n"
            f"📤 Опубликовано: {posted_cnt}"
        )

    # ── Кнопки одобрения ─────────────────────────────────────────────

    async def on_button(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        if query.data == "noop":
            return

        action, post_id = query.data.split(":")
        post_id = int(post_id)

        if action == "approve":
            with sqlite3.connect(DB) as _db:
                row = _db.execute("SELECT status FROM posts WHERE id=?", (post_id,)).fetchone()
            if not row or row[0] in ("posted", "skipped", "error"):
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("🗑 Устарел", callback_data="noop")]])
                )
                return
            db_update(post_id, "approved")
            await ensure_img_data(self.session, post_id)
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Одобрен", callback_data="noop")
                ]])
            )
            await query.message.reply_text(
                f"✅ Добавлено в очередь! В очереди: {db_queue_size()}"
            )
        elif action == "caption":
            with sqlite3.connect(DB) as _db:
                row = _db.execute("SELECT status FROM posts WHERE id=?", (post_id,)).fetchone()
            if not row or row[0] in ("posted", "skipped", "error"):
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("🗑 Устарел", callback_data="noop")]])
                )
                return
            self.pending_caption = post_id
            await ensure_img_data(self.session, post_id)  # сохраняем байты пока URL свежий
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("✏️ Жду подпись...", callback_data="noop")
                ]])
            )
            await query.message.reply_text(
                "Напиши подпись для мема (или /skip чтобы без подписи):"
            )
        elif action == "now":
            with sqlite3.connect(DB) as _db:
                row = _db.execute("SELECT status FROM posts WHERE id=?", (post_id,)).fetchone()
            if not row or row[0] in ("posted", "skipped", "error"):
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("🗑 Устарел", callback_data="noop")]])
                )
                return
            db_update(post_id, "approved")
            await ensure_img_data(self.session, post_id)
            if self._post_lock.locked():
                await query.message.reply_text("Публикация уже идёт, подожди секунду.")
                return
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("🚀 Публикую...", callback_data="noop")
                ]])
            )
            async with self._post_lock:
                ok, published, err = await self.post_next(priority_id=post_id)
            if ok and published:
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[
                        InlineKeyboardButton("🚀 Опубликован!", callback_data="noop")
                    ]])
                )
            elif not ok:
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("❌ Ошибка", callback_data="noop")]])
                )
                await query.message.reply_text(f"❌ Ошибка публикации: {err}")
            else:
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[InlineKeyboardButton("⚠️ Картинка пропала", callback_data="noop")]])
                )
                await query.message.reply_text("Картинка пропала — пост не опубликован. Попробуй /fetch.")
        elif action == "unqueue":
            db_update(post_id, "skipped")
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ Убран из очереди", callback_data="noop")
                ]])
            )
            await query.message.reply_text(f"Убрано. В очереди осталось: {db_queue_size()}")
        elif action == "skip":
            db_update(post_id, "skipped")
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ Пропущен", callback_data="noop")
                ]])
            )

    async def on_text(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Получаем подпись от пользователя после одобрения мема с подписью."""
        if self.pending_caption is None:
            return
        caption = update.message.text.strip()
        db_update_caption(self.pending_caption, caption)
        db_update(self.pending_caption, "approved")
        await ensure_img_data(self.session, self.pending_caption)
        self.pending_caption = None
        await update.message.reply_text(f"✅ Добавлено в очередь с подписью:\n_{caption}_\n\nВ очереди: {db_queue_size()}", parse_mode="Markdown")

    async def cmd_skip_caption(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Одобрить мем без подписи (во время ввода подписи)."""
        if self.pending_caption is None:
            await update.message.reply_text("Нет мема ожидающего подпись.")
            return
        db_update(self.pending_caption, "approved")
        await ensure_img_data(self.session, self.pending_caption)
        self.pending_caption = None
        await update.message.reply_text(f"✅ Добавлено в очередь без подписи. В очереди: {db_queue_size()}")

    # ── Сбор и отправка мемов ────────────────────────────────────────

    async def fetch_and_notify(self):
        admin_id = db_get("admin_chat_id")
        if not admin_id:
            logging.warning("Нет admin_chat_id — напиши /start боту в личку")
            return

        logging.info("Проверяю каналы параллельно...")
        results = await asyncio.gather(
            *[fetch_channel(self.session, ch, self._fetch_semaphore) for ch in SOURCE_CHANNELS],
            return_exceptions=True,
        )

        sent = 0
        skipped_limit = 0
        for channel, posts in zip(SOURCE_CHANNELS, results):
            if isinstance(posts, Exception):
                logging.error(f"Ошибка парсинга {channel}: {posts}")
                continue
            for post in posts:
                img = await download_image(self.session, post["img_url"])
                if not img:
                    continue

                post_id = db_save_post(
                    post["channel"], post["msg_id"],
                    post["img_url"],  post["caption"], img,
                )
                if not post_id:
                    # Пост уже в базе — обновляем img_data если его нет
                    with sqlite3.connect(DB) as _db:
                        row = _db.execute(
                            "SELECT id FROM posts WHERE channel=? AND msg_id=? AND img_data IS NULL",
                            (post["channel"], post["msg_id"])
                        ).fetchone()
                        if row:
                            _db.execute("UPDATE posts SET img_data=? WHERE id=?", (img, row[0]))
                            _db.commit()
                    continue  # уже видели, не показываем снова

                # Лимит на количество отправок за один fetch
                if sent >= MAX_SEND_PER_FETCH:
                    skipped_limit += 1
                    continue  # пост сохранён в базе, покажем через resend_pending в следующий раз

                try:
                    caption = post["caption"]
                    label   = f"📌 @{channel}"
                    text    = f"{caption}\n\n{label}" if caption else label

                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("✅", callback_data=f"approve:{post_id}"),
                        InlineKeyboardButton("🚀", callback_data=f"now:{post_id}"),
                        InlineKeyboardButton("✍️", callback_data=f"caption:{post_id}"),
                        InlineKeyboardButton("❌", callback_data=f"skip:{post_id}"),
                    ]])

                    sent_msg = await self.app.bot.send_photo(
                        chat_id=admin_id,
                        photo=BytesIO(img),
                        caption=text,
                        reply_markup=keyboard,
                    )
                    # Сохраняем file_id — он постоянный, не истекает
                    fid = sent_msg.photo[-1].file_id
                    db_save_file_id(post_id, fid)
                    db_update(post_id, "sent")
                    sent += 1
                    await asyncio.sleep(0.5)

                except Exception as e:
                    logging.error(f"Ошибка отправки в личку: {e}")

        logging.info(f"Отправлено на проверку: {sent}" + (f", отложено (лимит): {skipped_limit}" if skipped_limit else ""))
        if skipped_limit:
            try:
                await self.app.bot.send_message(
                    chat_id=admin_id,
                    text=f"Показано {sent} из {sent + skipped_limit} новых мемов. Одобри их и нажми /fetch чтобы получить остальные {skipped_limit}.",
                )
            except Exception:
                pass
        self.last_fetch = datetime.now(MSK)

    # ── Публикация в канал ───────────────────────────────────────────

    async def post_next(self, priority_id: Optional[int] = None):
        """Возвращает (ok, published, err).
        ok=True published=True  — мем успешно опубликован
        ok=True published=False — очередь пуста или все посты битые
        ok=False published=False — ошибка Telegram API
        """
        with sqlite3.connect(DB) as _db:
            if priority_id is not None:
                rows = _db.execute(
                    "SELECT id, channel, msg_id, img_url, user_caption, img_data, file_id FROM posts "
                    "WHERE status='approved' AND id=?",
                    (priority_id,)
                ).fetchall()
            else:
                rows = _db.execute(
                    "SELECT id, channel, msg_id, img_url, user_caption, img_data, file_id FROM posts "
                    "WHERE status='approved' ORDER BY added_at ASC"
                ).fetchall()

        if not rows:
            logging.warning("Очередь пуста, пропускаю слот")
            return True, False, None

        for post_id, channel, msg_id, img_url, caption, img_data, file_id in rows:
            # file_id — постоянный, не истекает никогда; img_data — байты; остальное — запасные варианты
            if file_id:
                photo = file_id
            elif img_data:
                photo = BytesIO(img_data)
            else:
                raw = await download_image(self.session, img_url) or await refetch_image(self.session, channel, msg_id)
                if not raw:
                    logging.warning(f"Пост {post_id}: картинка недоступна, пропускаю")
                    db_update(post_id, "skipped")
                    continue
                photo = BytesIO(raw)

            try:
                await self.app.bot.send_photo(
                    chat_id=MY_CHANNEL,
                    photo=photo,
                    caption=caption if caption else None,
                )
                db_update(post_id, "posted")
                with sqlite3.connect(DB) as _db:
                    _db.execute("UPDATE posts SET img_data=NULL WHERE id=?", (post_id,))
                    _db.commit()
                logging.info("Мем опубликован в канале")
                return True, True, None
            except Exception as e:
                logging.error(f"Ошибка публикации: {e}")
                return False, False, str(e)

        logging.warning("Все одобренные посты были битые, очередь очищена")
        return True, False, None

    # ── Главный цикл ─────────────────────────────────────────────────

    async def main_loop(self):
        self.current_day = datetime.now(MSK).date()
        self.schedule    = make_schedule()
        logging.info(
            f"Расписание ({len(self.schedule)} постов): "
            + ", ".join(t.strftime("%H:%M") for t in self.schedule)
        )

        # Не делаем авто-фетч при старте — только по расписанию или /fetch вручную
        self.last_fetch = datetime.now(MSK)

        while True:
            now = datetime.now(MSK)

            if now.date() != self.current_day:
                self.current_day = now.date()
                self.schedule    = make_schedule()
                logging.info("Новый день! Расписание: "
                             + ", ".join(t.strftime("%H:%M") for t in self.schedule))
                # Чистим старые записи раз в день
                with sqlite3.connect(DB) as _db:
                    deleted = _db.execute(
                        "DELETE FROM posts WHERE status IN ('posted','skipped','error') "
                        "AND added_at < datetime('now', '-30 days')"
                    ).rowcount
                    _db.commit()
                if deleted:
                    logging.info(f"Очистка базы: удалено {deleted} старых записей")

            if self.last_fetch is None or (now - self.last_fetch).total_seconds() >= FETCH_INTERVAL:
                await self.fetch_and_notify()

            if self.schedule and now >= self.schedule[0]:
                self.schedule.pop(0)
                async with self._post_lock:
                    ok, published, err = await self.post_next()
                admin_id = db_get("admin_chat_id")
                if not ok:
                    logging.error(f"Плановая публикация не удалась: {err}")
                    if admin_id:
                        try:
                            await self.app.bot.send_message(chat_id=admin_id, text=f"❌ Плановая публикация не удалась: {err}")
                        except Exception:
                            pass
                elif published:
                    logging.info("Плановая публикация прошла успешно")
                    if admin_id:
                        next_times = ", ".join(t.strftime("%H:%M") for t in self.schedule) or "больше нет"
                        try:
                            await self.app.bot.send_message(
                                chat_id=admin_id,
                                text=f"📤 Опубликовано по расписанию\nОсталось в очереди: {db_queue_size()}\nСледующие слоты: {next_times}",
                            )
                        except Exception:
                            pass
                else:
                    logging.warning("Плановая публикация: все посты битые")
                    if admin_id:
                        try:
                            await self.app.bot.send_message(chat_id=admin_id, text="⚠️ Плановая публикация: в очереди нет постов с рабочей картинкой. Одобри новые через /fetch.")
                        except Exception:
                            pass

            await asyncio.sleep(30)

    async def run(self):
        from telegram.error import Conflict
        # Создаём все asyncio-примитивы внутри event loop (Python 3.9 требует это)
        self._post_lock = asyncio.Lock()
        self._fetch_semaphore = asyncio.Semaphore(8)
        self._setup_app()
        connector = aiohttp.TCPConnector(limit=20)
        self.session = aiohttp.ClientSession(headers=HEADERS, connector=connector)
        await self.app.initialize()
        await self.app.start()
        try:
            await self.app.updater.start_polling(drop_pending_updates=True)
        except Conflict as e:
            logging.critical(f"Конфликт: уже запущен другой экземпляр бота. Выхожу. ({e})")
            sys.exit(1)
        logging.info(f"Бот запущен! MY_CHANNEL={MY_CHANNEL!r}  BOT_TOKEN={'OK' if BOT_TOKEN else 'ПУСТОЙ'}  DB={DB}")
        try:
            await self.main_loop()
        except Conflict as e:
            logging.critical(f"Конфликт во время работы: {e}. Выхожу.")
            sys.exit(1)
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
