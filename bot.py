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

import pytz
import requests
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
]

POSTS_PER_DAY_MIN = 7
POSTS_PER_DAY_MAX = 10
POST_START_HOUR   = 9
POST_END_HOUR     = 22
MAX_CAPTION_LEN   = 150
FETCH_INTERVAL    = 3600  # секунды между проверками каналов
FETCH_HOURS_BACK  = 72    # брать посты за последние N часов

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

def fetch_channel(channel: str, hours_back: int = FETCH_HOURS_BACK) -> list:
    try:
        resp = requests.get(f"https://t.me/s/{channel}", headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return []

        soup     = BeautifulSoup(resp.text, "html.parser")
        posts    = []
        cutoff   = datetime.now(pytz.utc) - timedelta(hours=hours_back)

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

def download_image(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        return r.content if r.status_code == 200 else None
    except Exception as e:
        logging.error(f"Ошибка скачивания изображения: {e}")
        return None

def refetch_image(channel: str, msg_id: str) -> Optional[bytes]:
    """Заново достаёт свежий CDN-URL поста и скачивает картинку."""
    try:
        url = f"https://t.me/s/{channel}?before={int(msg_id) + 1}"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        for msg in soup.find_all("div", class_="tgme_widget_message"):
            data_post = msg.get("data-post", "")
            if data_post != f"{channel}/{msg_id}":
                continue
            wrap = msg.find("a", class_="tgme_widget_message_photo_wrap")
            if wrap:
                m = re.search(r"url\('(.+?)'\)", wrap.get("style", ""))
                if m:
                    return download_image(m.group(1))
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

def db_update(post_id: int, status: str):
    with sqlite3.connect(DB) as db:
        db.execute("UPDATE posts SET status=? WHERE id=?", (status, post_id))
        db.commit()

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

def ensure_img_data(post_id: int):
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
    img = download_image(img_url) or refetch_image(channel, msg_id)
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
            "SELECT id, channel, img_url, caption FROM posts WHERE status='new' ORDER BY added_at ASC LIMIT 30"
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
        self.app              = Application.builder().token(BOT_TOKEN).build()
        self.schedule         = []
        self.last_fetch       = None
        self.current_day      = None
        self.pending_caption  = None  # post_id ожидающий подписи
        init_db()

        self.app.add_handler(CommandHandler("start",      self.cmd_start))
        self.app.add_handler(CommandHandler("queue",      self.cmd_queue))
        self.app.add_handler(CommandHandler("post",       self.cmd_post))
        self.app.add_handler(CommandHandler("fetch",      self.cmd_fetch))
        self.app.add_handler(CommandHandler("skip",       self.cmd_skip_caption))
        self.app.add_handler(CommandHandler("status",     self.cmd_status))
        self.app.add_handler(CommandHandler("clearqueue", self.cmd_clearqueue))
        self.app.add_handler(CallbackQueryHandler(self.on_button))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.on_text))

    # ── Команды ──────────────────────────────────────────────────────

    async def cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = str(update.effective_chat.id)
        db_set("admin_chat_id", chat_id)
        await update.message.reply_text(
            "Привет! Я буду присылать сюда мемы для одобрения.\n\n"
            "✅ — одобрить (без подписи)\n"
            "✍️ — одобрить с подписью\n"
            "❌ — пропустить\n\n"
            "/queue — сколько мемов в очереди\n"
            "/fetch — проверить каналы прямо сейчас\n"
            "/post — опубликовать мем вручную\n\n"
            f"Твой Telegram ID: `{chat_id}`",
            parse_mode="Markdown"
        )

    async def cmd_queue(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        n = db_queue_size()
        times = ", ".join(t.strftime("%H:%M") for t in self.schedule) or "нет"
        await update.message.reply_text(
            f"В очереди: {n} мемов\n"
            f"Расписание на сегодня: {times}"
        )

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
        for post_id, channel, img_url, caption in rows:
            img = download_image(img_url)
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
        await update.message.reply_text("Публикую...")
        ok, err = await self.post_next()
        if ok:
            await update.message.reply_text(f"Готово! Осталось в очереди: {db_queue_size()}")
        else:
            await update.message.reply_text(f"❌ Ошибка публикации: {err}")

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
            db_update(post_id, "approved")
            ensure_img_data(post_id)  # сохраняем байты пока URL свежий
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Одобрен", callback_data="noop")
                ]])
            )
            await query.message.reply_text(
                f"✅ Добавлено в очередь! В очереди: {db_queue_size()}"
            )
        elif action == "caption":
            # Одобряем с подписью — ждём текст; байты сохраним когда придёт подпись
            self.pending_caption = post_id
            ensure_img_data(post_id)  # сохраняем байты пока URL свежий
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("✏️ Жду подпись...", callback_data="noop")
                ]])
            )
            await query.message.reply_text(
                "Напиши подпись для мема (или /skip чтобы без подписи):"
            )
        elif action == "now":
            db_update(post_id, "approved")
            ensure_img_data(post_id)  # сохраняем байты пока URL свежий
            await query.edit_message_reply_markup(
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("🚀 Публикую...", callback_data="noop")
                ]])
            )
            ok, err = await self.post_next()
            if ok:
                await query.edit_message_reply_markup(
                    InlineKeyboardMarkup([[
                        InlineKeyboardButton("🚀 Опубликован!", callback_data="noop")
                    ]])
                )
            else:
                await query.message.reply_text(f"❌ Ошибка публикации: {err}")
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
        ensure_img_data(self.pending_caption)
        self.pending_caption = None
        await update.message.reply_text(f"✅ Добавлено в очередь с подписью:\n_{caption}_\n\nВ очереди: {db_queue_size()}", parse_mode="Markdown")

    async def cmd_skip_caption(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Одобрить мем без подписи (во время ввода подписи)."""
        if self.pending_caption is None:
            await update.message.reply_text("Нет мема ожидающего подпись.")
            return
        db_update(self.pending_caption, "approved")
        ensure_img_data(self.pending_caption)
        self.pending_caption = None
        await update.message.reply_text(f"✅ Добавлено в очередь без подписи. В очереди: {db_queue_size()}")

    # ── Сбор и отправка мемов ────────────────────────────────────────

    async def fetch_and_notify(self):
        admin_id = db_get("admin_chat_id")
        if not admin_id:
            logging.warning("Нет admin_chat_id — напиши /start боту в личку")
            return

        logging.info("Проверяю каналы...")
        sent = 0

        for channel in SOURCE_CHANNELS:
            posts = fetch_channel(channel)
            for post in posts:
                img = download_image(post["img_url"])
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

        logging.info(f"Отправлено на проверку: {sent}")
        self.last_fetch = datetime.now(MSK)

    # ── Публикация в канал ───────────────────────────────────────────

    async def post_next(self):
        # Берём сразу все одобренные посты и перебираем
        with sqlite3.connect(DB) as _db:
            rows = _db.execute(
                "SELECT id, channel, msg_id, img_url, user_caption, img_data, file_id FROM posts "
                "WHERE status='approved' ORDER BY added_at ASC"
            ).fetchall()

        if not rows:
            logging.warning("Очередь пуста, пропускаю слот")
            return True, None

        for post_id, channel, msg_id, img_url, caption, img_data, file_id in rows:
            # file_id — постоянный, не истекает никогда; img_data — байты; остальное — запасные варианты
            if file_id:
                photo = file_id
            elif img_data:
                photo = BytesIO(img_data)
            else:
                raw = download_image(img_url) or refetch_image(channel, msg_id)
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
                # Байты больше не нужны — освобождаем место в базе
                with sqlite3.connect(DB) as _db:
                    _db.execute("UPDATE posts SET img_data=NULL WHERE id=?", (post_id,))
                    _db.commit()
                logging.info("Мем опубликован в канале")
                return True, None
            except Exception as e:
                logging.error(f"Ошибка публикации: {e}")
                return False, str(e)

        # Все посты оказались битые — очередь очищена, не ошибка
        logging.warning("Все одобренные посты были битые, очередь очищена")
        return True, None

    # ── Главный цикл ─────────────────────────────────────────────────

    async def main_loop(self):
        self.current_day = datetime.now(MSK).date()
        self.schedule    = make_schedule()
        logging.info(
            f"Расписание ({len(self.schedule)} постов): "
            + ", ".join(t.strftime("%H:%M") for t in self.schedule)
        )

        await self.fetch_and_notify()

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
                ok, err = await self.post_next()
                if not ok and err:
                    logging.error(f"Плановая публикация не удалась: {err}")

            await asyncio.sleep(30)

    async def run(self):
        from telegram.error import Conflict
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
