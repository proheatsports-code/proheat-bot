import os
import re
import json
import logging
import hashlib
import unicodedata
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from openai import OpenAI
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    filters,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
)

# =========================================================
# CONFIG
# =========================================================

MX_TZ = ZoneInfo("America/Mexico_City")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

# =========================
# ADMINS
# =========================
ADMIN_IDS = {
    7696799656,
    8483530865,
}

USERS_FILE = os.getenv("USERS_FILE", "usuarios.json")
EXCEL_FILE = os.getenv("EXCEL_FILE", "data.xlsx")  # Fallback local si la API no responde

# URL pública del backend/web ProHeat Sports.
# En Railway agrega esta variable si tu dominio cambia:
# PROHEAT_API_BASE=https://proheatsports.com
PROHEAT_API_BASE = os.getenv("PROHEAT_API_BASE", "https://proheatsports.com").strip().rstrip("/")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "").strip()
API_FOOTBALL_BASE = "https://v3.football.api-sports.io"

GNEWS_API_KEY = os.getenv("GNEWS_API_KEY", "").strip()
GNEWS_BASE = "https://gnews.io/api/v4"

OPEN_METEO_GEOCODING_BASE = "https://geocoding-api.open-meteo.com/v1/search"
OPEN_METEO_FORECAST_BASE = "https://api.open-meteo.com/v1/forecast"

SPORT_IA_USAGE_FILE = os.getenv("SPORT_IA_USAGE_FILE", "sport_ia_usage.json")
SPORT_IA_CACHE_FILE = os.getenv("SPORT_IA_CACHE_FILE", "sport_ia_cache.json")

# =========================
# LÍMITE EDITABLE PROHEAT SPORT IA
# =========================
SPORT_IA_DAILY_LIMIT = int(os.getenv("SPORT_IA_DAILY_LIMIT", "10"))

NEWS_LOOKBACK_DAYS = int(os.getenv("NEWS_LOOKBACK_DAYS", "6"))
MAX_NEWS_ITEMS = int(os.getenv("MAX_NEWS_ITEMS", "8"))
GNEWS_MAX_ARTICLES = int(os.getenv("GNEWS_MAX_ARTICLES", "8"))
RECENT_FIXTURES_COUNT = int(os.getenv("RECENT_FIXTURES_COUNT", "5"))
TEAM_MATCH_THRESHOLD = int(os.getenv("TEAM_MATCH_THRESHOLD", "58"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("proheat-bot")

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# =========================================================
# MENÚS
# =========================================================

def build_main_menu_for_user(user_id: int):
    base_menu = [
        ["📘 Guía de Uso"],
        ["🤖 ProHeat Sport IA"],
        ["Partidos del Día"],
        ["🔥 Hot Predicciones"]
    ]

    if is_admin(user_id):
        base_menu.append(["🛠️ Panel de Administradores"])

    return ReplyKeyboardMarkup(base_menu, resize_keyboard=True)

hot_predicciones_menu = InlineKeyboardMarkup([
    [InlineKeyboardButton("Predicciones Premium", callback_data="Hoja2")],
    [InlineKeyboardButton("Manejo de Banca", callback_data="Hoja3")],
    [InlineKeyboardButton("Combinadas IA", callback_data="Hoja4")],
    [InlineKeyboardButton("Marcadores Probables", callback_data="Hoja5")],
    [InlineKeyboardButton("Predicciones Heatop", callback_data="Hoja6")],
    [InlineKeyboardButton("Equipos más confiables hoy", callback_data="Hoja7")],
    [InlineKeyboardButton("Predicciones Simples", callback_data="Hoja8")],
    [InlineKeyboardButton("Combinadas Inferno", callback_data="Hoja9")],
])

# Alias para compatibilidad interna con llamadas antiguas.
picks_menu = hot_predicciones_menu

admin_panel_menu = InlineKeyboardMarkup([
    [InlineKeyboardButton("👥 Ver usuarios", callback_data="admin_view_users")],
    [InlineKeyboardButton("📩 Ver pendientes", callback_data="admin_view_pending")],
    [InlineKeyboardButton("✅ Aprobar usuario", callback_data="admin_prompt_approve")],
    [InlineKeyboardButton("🗑️ Eliminar usuario", callback_data="admin_prompt_delete")],
    [InlineKeyboardButton("⏳ Extensión 15 días", callback_data="admin_prompt_extend_15")],
    [InlineKeyboardButton("⏳ Extensión 30 días", callback_data="admin_prompt_extend_30")],
    [InlineKeyboardButton("📆 Extensión 60 días", callback_data="admin_prompt_extend_60")],
    [InlineKeyboardButton("📅 Extensión 90 días", callback_data="admin_prompt_extend_90")],
    [InlineKeyboardButton("🎁 Prueba Gratuita (7 días)", callback_data="admin_prompt_trial_7")],
])

sheet_titles = {
    "Hoja1": "🔥 PARTIDOS DEL DÍA 🔥",
    "Hoja2": "🔥 PREDICCIONES PREMIUM 🔥",
    "Hoja3": "🔥 MANEJO DE BANCA 🔥",
    "Hoja4": "🔥 COMBINADAS IA 🔥",
    "Hoja5": "🔥 MARCADORES PROBABLES 🔥",
    "Hoja6": "🔥 PREDICCIONES HEATOP 🔥",
    "Hoja7": "🔥 EQUIPOS MÁS CONFIABLES HOY 🔥",
    "Hoja8": "🔥 PREDICCIONES SIMPLES 🔥",
    "Hoja9": "🔥 COMBINADAS INFERNO 🔥",
}

# El admin web genera latest.json y expone estas secciones por API.
# Así el bot y la web premium consumen la misma fuente después de subir Excel.
SHEET_TO_API = {
    # Hoja1 = Partidos del día
    "Hoja1": "/api/data/general",
    # Hoja2 = Predicciones Premium
    "Hoja2": "/api/data/ultra",
    # Hoja3 = Manejo de banca
    "Hoja3": "/api/data/stakes",
    # Hoja4 = Combinadas IA
    "Hoja4": "/api/data/combinadas",
    # Hoja5 = Marcadores probables
    "Hoja5": "/api/data/goles",
    # Hoja6 = Predicciones Heatop
    "Hoja6": "/api/data/top",
    # Hoja7 = Equipos más confiables hoy
    "Hoja7": "/api/data/alta-confianza",
    # Hoja8 = Predicciones Simples
    "Hoja8": "/api/data/public",
    # Hoja9 = Combinadas Inferno
    "Hoja9": "/api/data/inferno",
}

# =========================================================
# HELPERS GENERALES
# =========================================================

TEAM_ALIASES = {
    "real madrid": ["real madrid", "realmadrid", "rmadrid", "real"],
    "atletico de madrid": ["atletico de madrid", "atl madrid", "atletico madrid", "atletico", "atm", "atleti"],
    "barcelona": ["barcelona", "barca", "fc barcelona", "barsa"],
    "manchester city": ["manchester city", "man city", "city"],
    "manchester united": ["manchester united", "man united", "man utd", "united", "man u"],
    "psg": ["psg", "paris saint germain", "paris sg"],
    "bayern munich": ["bayern munich", "bayern", "bayern munchen"],
    "juventus": ["juventus", "juve"],
    "inter": ["inter", "inter milan", "internazionale", "inter de milan"],
    "milan": ["milan", "ac milan"],
    "rb leipzig": ["rb leipzig", "leipzig"],
    "borussia dortmund": ["borussia dortmund", "dortmund", "bvb"],
    "newcastle united": ["newcastle", "newcastle united"],
    "tottenham": ["tottenham", "spurs"],
    "liverpool": ["liverpool"],
    "arsenal": ["arsenal"],
    "chelsea": ["chelsea"],
}

TEAM_SEARCH_REPLACEMENTS = {
    "atleti": "atletico madrid",
    "atletico de madrid": "atletico madrid",
    "athletic de madrid": "atletico madrid",
    "man u": "manchester united",
    "man utd": "manchester united",
    "psg": "paris saint germain",
    "inter de milan": "inter",
    "inter milan": "inter",
    "barca": "barcelona",
    "barsa": "barcelona",
    "juve": "juventus",
}

TEAM_STOPWORDS = {"de", "del", "la", "el", "los", "las", "fc", "cf", "sc", "club", "ac", "as"}

RUMOR_TERMS = [
    "rumor", "rumour", "gossip", "transfer", "fichaje", "mercado",
    "player ratings", "fantasy", "odds", "betting", "highlights", "recap"
]

def now_mx() -> datetime:
    return datetime.now(MX_TZ)

def today_mx() -> str:
    return now_mx().strftime("%Y-%m-%d")

def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default

def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"http\S+", "", str(text))
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def cut_text(text: str, max_len: int = 180) -> str:
    text = normalize_spaces(text)
    if len(text) <= max_len:
        return text
    return text[:max_len - 1].rstrip() + "…"

def limpiar_texto(texto: Any) -> str:
    texto = str(texto).lower().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    texto = re.sub(r"[^a-z0-9\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto

def hash_key(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def atomic_write_json(path: str, data: Any) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    os.replace(tmp, path)

def load_json_file(path: str, default_value: Any) -> Any:
    if not os.path.exists(path):
        atomic_write_json(path, default_value)
        return default_value
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("No se pudo leer %s: %s", path, e)
        return default_value

def save_json_file(path: str, data: Any) -> None:
    atomic_write_json(path, data)

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def format_date_safe(date_str: Optional[str]) -> str:
    if not date_str:
        return "N/A"
    return str(date_str)

# =========================================================
# JSON / USERS / CACHE
# =========================================================

def load_users() -> Dict[str, Any]:
    return load_json_file(USERS_FILE, {})

def save_users(data: Dict[str, Any]) -> None:
    save_json_file(USERS_FILE, data)

def is_user_active(user_data: Dict[str, Any]) -> bool:
    if not user_data:
        return False
    if user_data.get("status") == "expired":
        return False
    expires = user_data.get("expires")
    if not expires:
        return True
    try:
        exp_date = datetime.strptime(expires, "%Y-%m-%d").date()
        return now_mx().date() <= exp_date
    except Exception:
        return False

def is_allowed(user_id: int) -> bool:
    if is_admin(user_id):
        return True
    users = load_users()
    return is_user_active(users.get(str(user_id)))

def load_usage() -> Dict[str, Any]:
    return load_json_file(SPORT_IA_USAGE_FILE, {})

def save_usage(data: Dict[str, Any]) -> None:
    save_json_file(SPORT_IA_USAGE_FILE, data)

def get_user_usage_today(user_id: int) -> int:
    usage = load_usage()
    return safe_int(usage.get(str(user_id), {}).get(today_mx(), 0), 0)

def increment_user_usage(user_id: int) -> None:
    usage = load_usage()
    key = str(user_id)
    today = today_mx()
    usage.setdefault(key, {})
    usage[key][today] = safe_int(usage[key].get(today, 0), 0) + 1
    save_usage(usage)

def remaining_queries_today(user_id: int) -> int:
    return max(SPORT_IA_DAILY_LIMIT - get_user_usage_today(user_id), 0)

def load_cache() -> Dict[str, Any]:
    return load_json_file(SPORT_IA_CACHE_FILE, {})

def save_cache(data: Dict[str, Any]) -> None:
    save_json_file(SPORT_IA_CACHE_FILE, data)

def normalize_cache_key(text: str) -> str:
    return hash_key(limpiar_texto(text))

def get_cached_analysis(match_text: str) -> Optional[Dict[str, Any]]:
    cache = load_cache()
    key = normalize_cache_key(match_text)
    item = cache.get(key)
    if not item:
        return None
    if item.get("date") != today_mx():
        return None
    return item.get("result")

def save_cached_analysis(match_text: str, result: Dict[str, Any]) -> None:
    cache = load_cache()
    key = normalize_cache_key(match_text)
    cache[key] = {
        "date": today_mx(),
        "result": result
    }
    save_cache(cache)

# =========================================================
# USERS HELPERS
# =========================================================

def create_or_update_pending_user(user_id: int) -> None:
    users = load_users()
    key = str(user_id)
    if key not in users:
        users[key] = {
            "status": "pending",
            "requested_at": today_mx(),
            "start_date": None,
            "expires": None,
            "warned_3days_at": None,
            "expired_notified_at": None,
            "is_trial": False,
        }
    else:
        users[key].setdefault("status", "pending")
        users[key].setdefault("requested_at", today_mx())
        users[key].setdefault("start_date", None)
        users[key].setdefault("expires", None)
        users[key].setdefault("warned_3days_at", None)
        users[key].setdefault("expired_notified_at", None)
        users[key].setdefault("is_trial", False)
    save_users(users)

def approve_user_membership(user_id: str, days: int = 30, is_trial: bool = False) -> Dict[str, Any]:
    users = load_users()
    start_date = now_mx().date()
    expiration = start_date + timedelta(days=days)

    users[user_id] = {
        "status": "active",
        "requested_at": users.get(user_id, {}).get("requested_at", today_mx()),
        "start_date": start_date.strftime("%Y-%m-%d"),
        "expires": expiration.strftime("%Y-%m-%d"),
        "warned_3days_at": None,
        "expired_notified_at": None,
        "is_trial": is_trial,
    }
    save_users(users)
    return users[user_id]

def delete_user_membership(user_id: str) -> bool:
    users = load_users()
    if user_id not in users:
        return False
    del users[user_id]
    save_users(users)
    return True

def extend_user_membership(user_id: str, days: int) -> Optional[Dict[str, Any]]:
    users = load_users()
    if user_id not in users:
        return None

    user_data = users[user_id]
    today = now_mx().date()

    expires_str = user_data.get("expires")
    if expires_str:
        try:
            current_exp = datetime.strptime(expires_str, "%Y-%m-%d").date()
        except Exception:
            current_exp = today
    else:
        current_exp = today

    base_date = current_exp if current_exp >= today else today
    new_exp = base_date + timedelta(days=days)

    if not user_data.get("start_date"):
        user_data["start_date"] = today.strftime("%Y-%m-%d")

    user_data["status"] = "active"
    user_data["expires"] = new_exp.strftime("%Y-%m-%d")
    user_data["warned_3days_at"] = None
    user_data["expired_notified_at"] = None
    users[user_id] = user_data
    save_users(users)
    return user_data

def get_pending_users_report() -> str:
    users = load_users()
    pending = []

    for user_id, data in users.items():
        if data.get("status") == "pending":
            pending.append(f"🆔 {user_id} | Solicitud: {data.get('requested_at', 'N/A')}")

    if not pending:
        return "No hay usuarios pendientes."

    return "📩 USUARIOS PENDIENTES\n\n" + "\n".join(pending)

def build_users_report() -> str:
    users = load_users()
    if not users:
        return "No hay usuarios registrados."

    lines = ["👥 USUARIOS PROHEAT SPORTS", ""]
    sorted_items = sorted(users.items(), key=lambda x: x[0])

    for user_id, data in sorted_items:
        status = data.get("status", "active" if is_user_active(data) else "inactive")
        requested_at = format_date_safe(data.get("requested_at"))
        start_date = format_date_safe(data.get("start_date"))
        expires = format_date_safe(data.get("expires"))
        trial_txt = "Sí" if data.get("is_trial") else "No"

        if data.get("expires"):
            active_now = is_user_active(data)
            estado_txt = "Activo" if active_now else "Vencido"
        else:
            estado_txt = status.capitalize()

        lines.append(f"🆔 {user_id}")
        lines.append(f"Estado: {estado_txt}")
        lines.append(f"Solicitud: {requested_at}")
        lines.append(f"Inicio: {start_date}")
        lines.append(f"Fin: {expires}")
        lines.append(f"Prueba gratuita: {trial_txt}")
        lines.append("━━━━━━━━━━━━━━━")

    return "\n".join(lines)

# =========================================================
# RENOVACIONES / EXPIRACIÓN
# =========================================================

async def check_subscriptions(context: ContextTypes.DEFAULT_TYPE):
    users = load_users()
    if not users:
        return

    today = now_mx().date()
    changed = False

    for user_id, data in list(users.items()):
        expires_str = data.get("expires")
        if not expires_str:
            continue

        try:
            expires_date = datetime.strptime(expires_str, "%Y-%m-%d").date()
        except Exception:
            continue

        days_left = (expires_date - today).days

        if days_left == 3 and data.get("warned_3days_at") != today_mx():
            try:
                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        "⏳ Tu suscripción de ProHeat Sports vencerá en 3 días.\n"
                        "Si deseas continuar con el bot, te recomendamos renovar con anticipación."
                    )
                )
            except Exception:
                logger.exception("No se pudo avisar al usuario %s sobre vencimiento próximo", user_id)

            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=(
                            f"📣 Aviso de suscripción próxima a vencer\n"
                            f"Usuario: {user_id}\n"
                            f"Inicio: {data.get('start_date', 'N/A')}\n"
                            f"Fin: {data.get('expires', 'N/A')}\n"
                            "Vence en 3 días."
                        )
                    )
                except Exception:
                    logger.exception("No se pudo avisar al admin %s sobre vencimiento próximo", admin_id)

            data["warned_3days_at"] = today_mx()
            changed = True

        if days_left < 0 and data.get("status") != "expired":
            data["status"] = "expired"
            data["expired_notified_at"] = today_mx()
            changed = True

            try:
                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        "🔒 Tu suscripción de ProHeat Sports ha vencido.\n"
                        "Gracias por usar el bot.\n"
                        "Si deseas continuar, envía tu comprobante para renovar tu acceso."
                    )
                )
            except Exception:
                logger.exception("No se pudo avisar al usuario %s sobre expiración", user_id)

            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=(
                            f"🔒 Suscripción vencida automáticamente\n"
                            f"Usuario: {user_id}\n"
                            f"Inicio: {data.get('start_date', 'N/A')}\n"
                            f"Fin: {data.get('expires', 'N/A')}"
                        )
                    )
                except Exception:
                    logger.exception("No se pudo avisar al admin %s sobre expiración", admin_id)

    if changed:
        save_users(users)

# =========================================================
# GUÍA
# =========================================================

def guia_texto() -> str:
    return (
        "📘 *GUÍA DE USO – PROHEAT SPORTS*\n\n"
        "📊 *GLOSARIO*\n"
        "• L: Local\n"
        "• V: Visitante\n"
        "• ML: Probabilidad de ganador\n"
        "• SoT: Tiros a puerta\n"
        "• Corners: Tiros de esquina\n"
        "• Tarjetas: Total estimado de tarjetas\n"
        "• Doble oportunidad: dos resultados cubiertos en una misma jugada\n\n"
        "📌 *SECCIONES*\n\n"
        "🔥 *Partidos del Día*\n"
        "Análisis completo generado por IA: ganador probable, goles, corners y tarjetas.\n\n"
        "💎 *Predicciones Premium*\n"
        "Selección de picks con mayor probabilidad estadística.\n\n"
        "📊 *Manejo de Banca*\n"
        "Porcentaje recomendado a invertir por pick.\n\n"
        "🔗 *Combinadas IA*\n"
        "Picks agrupados optimizados por IA.\n\n"
        "⚽ *Marcadores Probables*\n"
        "Proyección de goles por partido.\n\n"
        "🏆 *Predicciones Heatop*\n"
        "Los picks con mayor valor estadístico del día.\n\n"
        "🔥🟠 *Picks Inferno*\n"
        "Selección premium basada en análisis completo del día.\n\n"
        "🤖 *ProHeat Sport IA*\n"
        "Análisis bajo demanda de partidos usando datos deportivos, noticias recientes y el motor ProHeat Sports.\n"
        f"Límite: {SPORT_IA_DAILY_LIMIT} consultas por usuario al día.\n\n"
        "📈 *RECOMENDACIÓN*\n"
        "Gestiona tu banca con disciplina.\n"
        "Evita sobreapostar y sigue la estrategia.\n"
    )

# =========================================================
# DATOS WEB / EXCEL FALLBACK
# =========================================================

DISPLAY_LABELS = {
    "hora": "⏰ Hora",
    "time": "⏰ Hora",
    "liga": "⚽ Liga",
    "league": "⚽ Liga",
    "partido": "🏟️ Partido",
    "match": "🏟️ Partido",
    "sem": "🚦 Semáforo",
    "semaforo": "🚦 Semáforo",
    "ml": "📊 ML/DC",
    "dc": "📊 DC",
    "pick": "📌 Pick",
    "bet_recomendado": "✅ Bet recomendado",
    "goles_local": "⚽ Goles Local",
    "goles_visitante": "⚽ Goles Visitante",
    "marcador_global": "📈 Marcador Global",
    "mitades": "⏱️ Mitades",
    "sot": "🎯 SoT",
    "corners": "📐 Corners",
    "tarjetas": "🟨 Tarjetas",
    "stake": "💰 Stake",
    "probabilidad": "📊 Probabilidad",
    "nota": "🧠 Nota",
    "notas": "🧠 Notas",
}

PRIMARY_ORDER = [
    "hora", "time", "liga", "league", "sem", "semaforo", "partido", "match",
    "ml", "dc", "pick", "bet_recomendado", "goles_local", "goles_visitante",
    "marcador_global", "mitades", "sot", "corners", "tarjetas", "stake",
    "probabilidad", "nota", "notas",
]

EMPTY_VALUES = {"", "nan", "none", "null", "n/a", "na", "-"}

def is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    text = str(value).strip()
    return text.lower() in EMPTY_VALUES

def format_cell_value(value: Any) -> str:
    if is_empty_value(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%H:%M")
    return str(value).strip()

def humanize_key(key: str) -> str:
    clean = str(key or "").strip()
    normalized = limpiar_texto(clean).replace(" ", "_")
    if normalized in DISPLAY_LABELS:
        return DISPLAY_LABELS[normalized]
    return clean.replace("_", " ").strip().title()

def normalize_item_key(key: str) -> str:
    return limpiar_texto(str(key or "")).replace(" ", "_")

def fetch_api_items(sheet_name: str) -> Optional[List[Dict[str, Any]]]:
    endpoint = SHEET_TO_API.get(sheet_name)
    if not endpoint:
        return None

    url = f"{PROHEAT_API_BASE}{endpoint}"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        if not isinstance(items, list):
            return []
        return [item for item in items if isinstance(item, dict)]
    except Exception as e:
        logger.warning("No se pudo leer API ProHeat %s: %s", url, e)
        return None

def get_value_by_normalized_key(item: Dict[str, Any], wanted_key: str) -> Any:
    for key, value in item.items():
        if normalize_item_key(key) == wanted_key:
            return value
    return None

def item_has_key(item: Dict[str, Any], wanted_key: str) -> bool:
    for key in item.keys():
        if normalize_item_key(key) == wanted_key:
            return True
    return False

def append_line(lines: List[str], label_key: str, value: Any) -> None:
    if is_empty_value(value):
        return
    label = humanize_key(label_key)
    lines.append(f"{label}: {format_cell_value(value)}")

def format_api_item(item: Dict[str, Any], sheet_name: str) -> str:
    normalized_keys = {normalize_item_key(k): k for k in item.keys()}

    hora = get_value_by_normalized_key(item, "hora") or get_value_by_normalized_key(item, "time")
    liga = get_value_by_normalized_key(item, "liga") or get_value_by_normalized_key(item, "league")
    partido = get_value_by_normalized_key(item, "partido") or get_value_by_normalized_key(item, "match")

    lines: List[str] = []

    if not is_empty_value(liga):
        lines.append(f"⚽ {format_cell_value(liga)}")
    if not is_empty_value(hora):
        lines.append(f"⏰ {format_cell_value(hora)}")
    if not is_empty_value(partido):
        lines.append(f"🏟️ {format_cell_value(partido)}")

    for key in PRIMARY_ORDER:
        if key in {"hora", "time", "liga", "league", "partido", "match"}:
            continue
        if item_has_key(item, key):
            append_line(lines, key, get_value_by_normalized_key(item, key))

    already_used = set(PRIMARY_ORDER)
    for original_key, value in item.items():
        nkey = normalize_item_key(original_key)
        if nkey in already_used:
            continue
        if nkey in {"id", "created_at", "updated_at", "source", "source_file"}:
            continue
        append_line(lines, original_key, value)

    if not lines:
        raw_values = [format_cell_value(v) for v in item.values() if not is_empty_value(v)]
        if raw_values:
            lines.append(" | ".join(raw_values))

    return "\n".join(lines).strip()

def read_sheet_from_api(sheet_name: str) -> Optional[str]:
    items = fetch_api_items(sheet_name)
    if items is None:
        return None

    title = sheet_titles.get(sheet_name, sheet_name)
    if not items:
        return f"{title}\n\n⚠️ Sin datos cargados desde la web."

    parts = [title, ""]
    for item in items:
        block = format_api_item(item, sheet_name)
        if block:
            parts.append(block)
            parts.append("━━━━━━━━━━━━━━━")
            parts.append("")

    return "\n".join(parts).strip()

def read_sheet_from_excel_fallback(sheet_name: str) -> str:
    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name=sheet_name)
        if df.empty:
            return "⚠️ Sin datos"

        title = sheet_titles.get(sheet_name, sheet_name)
        parts = [title, ""]

        for _, row in df.iterrows():
            valores = [format_cell_value(x) for x in row if format_cell_value(x)]

            if not valores:
                continue

            if sheet_name in ["Hoja1", "Hoja2"]:
                if len(valores) < 3:
                    continue

                hora = valores[0]
                liga = valores[1]
                partido = valores[2]

                block = [
                    f"⚽ {liga}",
                    f"⏰ {hora}",
                    f"🏟️ {partido}",
                ]

                if len(valores) > 3:
                    block.append(f"📊 {valores[3]}")
                if len(valores) > 5:
                    block.append(f"⚽ L: {valores[4]} | V: {valores[5]}")
                if len(valores) > 6:
                    block.append(f"🎯 SoT: {valores[6]}")
                if len(valores) > 7:
                    block.append(f"📐 Corners: {valores[7]}")
                if len(valores) > 8:
                    block.append(f"🟨 Tarjetas: {valores[8]}")

                parts.append("\n".join(block))
                parts.append("━━━━━━━━━━━━━━━")
                parts.append("")
            else:
                parts.append("• " + " | ".join(valores))
                parts.append("")

        return "\n".join(parts).strip()

    except Exception as e:
        logger.exception("Error leyendo Excel fallback")
        return f"❌ Error leyendo API y Excel fallback:\n{str(e)}"

def read_sheet(sheet_name: str) -> str:
    # Primero consume la API del backend/web. Si falla, usa el Excel local como respaldo.
    api_text = read_sheet_from_api(sheet_name)
    if api_text is not None:
        return api_text
    return read_sheet_from_excel_fallback(sheet_name)

# =========================================================
# PARSEO PARTIDO / EQUIPOS
# =========================================================

def parse_match_input(text: str) -> Tuple[Optional[str], Optional[str]]:
    parts = re.split(r"\s+vs\s+|\s+v\s+|\s*-\s*", text.strip(), maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return None, None
    home = parts[0].strip()
    away = parts[1].strip()
    if not home or not away:
        return None, None
    return home, away

def expand_team_variants(team_name: str) -> List[str]:
    clean_name = limpiar_texto(team_name)
    variants = [
        team_name,
        clean_name,
        team_name.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    ]

    for canonical, aliases in TEAM_ALIASES.items():
        if clean_name == canonical or clean_name in aliases:
            variants.extend([canonical] + aliases)

    dedup = []
    seen = set()
    for item in variants:
        item = normalize_spaces(item)
        if item and item not in seen:
            seen.add(item)
            dedup.append(item)
    return dedup

def compact_team_query(name: str) -> str:
    txt = limpiar_texto(name)
    for old, new in TEAM_SEARCH_REPLACEMENTS.items():
        txt = txt.replace(old, new)
    return txt.strip()

def remove_team_stopwords(text: str) -> str:
    tokens = limpiar_texto(text).split()
    tokens = [t for t in tokens if t not in TEAM_STOPWORDS]
    return " ".join(tokens).strip()

def generate_search_queries(team_name: str) -> List[str]:
    base_variants = expand_team_variants(team_name)
    queries = []

    for item in base_variants:
        compact = compact_team_query(item)
        nostop = remove_team_stopwords(compact)

        for q in [item, compact, nostop]:
            q = limpiar_texto(q)
            if q and q not in queries:
                queries.append(q)

        tokens = nostop.split()
        if len(tokens) >= 2:
            q2 = " ".join(tokens[:2])
            if q2 and q2 not in queries:
                queries.append(q2)
        if len(tokens) >= 1:
            q1 = tokens[0]
            if q1 and q1 not in queries:
                queries.append(q1)

    return queries[:10]

# =========================================================
# API HELPERS
# =========================================================

def api_football_get(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    if not API_FOOTBALL_KEY:
        return None

    headers = {"x-apisports-key": API_FOOTBALL_KEY}

    try:
        response = requests.get(
            f"{API_FOOTBALL_BASE}/{endpoint}",
            headers=headers,
            params=params or {},
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning("[API_FOOTBALL] %s -> %s", endpoint, e)
        return None

def gnews_get(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    if not GNEWS_API_KEY:
        return None

    params = params or {}
    params["apikey"] = GNEWS_API_KEY

    try:
        response = requests.get(
            f"{GNEWS_BASE}/{endpoint}",
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning("[GNEWS] %s -> %s", endpoint, e)
        return None

# =========================================================
# OPEN METEO
# =========================================================

def geocode_city(city_query: str) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(
            OPEN_METEO_GEOCODING_BASE,
            params={"name": city_query, "count": 1, "language": "es", "format": "json"},
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        return results[0] if results else None
    except Exception:
        return None

def get_weather_context(city: str, country: str = "") -> str:
    if not city:
        return "No pude confirmar el clima en la sede."

    query = f"{city}, {country}" if country else city
    geo = geocode_city(query)
    if not geo:
        return "No pude confirmar el clima en la sede."

    try:
        response = requests.get(
            OPEN_METEO_FORECAST_BASE,
            params={
                "latitude": geo["latitude"],
                "longitude": geo["longitude"],
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_probability_max,windspeed_10m_max",
                "forecast_days": 1,
                "timezone": "auto"
            },
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        daily = data.get("daily", {})

        tmax = daily.get("temperature_2m_max", ["N/D"])[0]
        tmin = daily.get("temperature_2m_min", ["N/D"])[0]
        rain = daily.get("precipitation_probability_max", ["N/D"])[0]
        wind = daily.get("windspeed_10m_max", ["N/D"])[0]

        return f"En {city} se esperan {tmin}°C a {tmax}°C, lluvia de {rain}% y viento de {wind} km/h."
    except Exception:
        return "No pude confirmar el clima en la sede."

# =========================================================
# API FOOTBALL: TEAMS / FIXTURES / H2H / STANDINGS / STATS
# =========================================================

def score_team_candidate(team_name: str, item: Dict[str, Any]) -> int:
    variants = [limpiar_texto(v) for v in expand_team_variants(team_name)]
    team = item.get("team", {})
    venue = item.get("venue", {})

    api_name = limpiar_texto(team.get("name", ""))
    api_country = limpiar_texto(team.get("country", ""))
    api_code = limpiar_texto(team.get("code", ""))
    api_city = limpiar_texto(venue.get("city", ""))

    candidates = [
        api_name,
        f"{api_name} {api_country}".strip(),
        f"{api_name} {api_code}".strip(),
        f"{api_name} {api_city}".strip()
    ]

    best = 0
    for cand in candidates:
        if not cand:
            continue
        for variant in variants:
            if cand == variant:
                score = 100
            elif cand in variant or variant in cand:
                score = 92
            else:
                common = set(cand.split()) & set(variant.split())
                score = max(0, 40 + 15 * len(common))
            best = max(best, score)

    return best

def search_team(team_name: str) -> Optional[Dict[str, Any]]:
    queries = generate_search_queries(team_name)
    candidates_by_id: Dict[int, Dict[str, Any]] = {}

    for query in queries:
        data = api_football_get("teams", {"search": query})
        if data and data.get("response"):
            for item in data["response"]:
                team_id = item.get("team", {}).get("id")
                if team_id:
                    candidates_by_id[team_id] = item

    if not candidates_by_id:
        return None

    scored = []
    for item in candidates_by_id.values():
        score = score_team_candidate(team_name, item)
        scored.append((score, item))

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best = scored[0]

    if best_score < TEAM_MATCH_THRESHOLD:
        return None

    team = best["team"]
    venue = best.get("venue", {})
    return {
        "id": team["id"],
        "name": team.get("name", team_name),
        "country": team.get("country", ""),
        "code": team.get("code", ""),
        "venue_city": venue.get("city", ""),
        "venue_name": venue.get("name", "")
    }

def get_recent_fixtures(team_id: int, last_n: int = RECENT_FIXTURES_COUNT) -> List[Dict[str, Any]]:
    data = api_football_get("fixtures", {"team": team_id, "last": last_n})
    if not data or "response" not in data:
        return []
    return data["response"]

def get_h2h(home_id: int, away_id: int, last_n: int = 5) -> List[Dict[str, Any]]:
    data = api_football_get("fixtures/headtohead", {"h2h": f"{home_id}-{away_id}", "last": last_n})
    if not data or "response" not in data:
        return []
    return data["response"]

def get_team_injuries(team_id: int) -> List[str]:
    season_candidates = [now_mx().year, now_mx().year - 1]
    injuries = []

    for season in season_candidates:
        data = api_football_get("injuries", {"team": team_id, "season": season})
        if data and data.get("response"):
            for item in data["response"][:10]:
                player = item.get("player", {}).get("name", "")
                reason = (
                    item.get("player", {}).get("reason", "")
                    or item.get("player", {}).get("type", "")
                    or item.get("fixture", {}).get("status", {}).get("long", "")
                )
                if player:
                    injuries.append(f"{player} ({reason})" if reason else player)
            if injuries:
                break

    clean_items = []
    seen = set()
    for x in injuries:
        key = limpiar_texto(x)
        if key and key not in seen:
            seen.add(key)
            clean_items.append(x)
    return clean_items[:5]

def summarize_team_form(team_name: str, fixtures: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not fixtures:
        return {
            "played": 0,
            "wins": 0,
            "draws": 0,
            "losses": 0,
            "gf_avg": 0.0,
            "ga_avg": 0.0
        }

    wins = draws = losses = 0
    gf = ga = 0

    for fx in fixtures:
        home = fx["teams"]["home"]["name"]
        away = fx["teams"]["away"]["name"]
        hg = fx["goals"]["home"] if fx["goals"]["home"] is not None else 0
        ag = fx["goals"]["away"] if fx["goals"]["away"] is not None else 0

        if limpiar_texto(home) == limpiar_texto(team_name):
            gf += hg
            ga += ag
            if hg > ag:
                wins += 1
            elif hg == ag:
                draws += 1
            else:
                losses += 1
        else:
            gf += ag
            ga += hg
            if ag > hg:
                wins += 1
            elif ag == hg:
                draws += 1
            else:
                losses += 1

    played = len(fixtures)
    return {
        "played": played,
        "wins": wins,
        "draws": draws,
        "losses": losses,
        "gf_avg": round(gf / played, 2) if played else 0.0,
        "ga_avg": round(ga / played, 2) if played else 0.0
    }

def get_days_since_last_match(fixtures: List[Dict[str, Any]]) -> Optional[int]:
    if not fixtures:
        return None
    try:
        latest = fixtures[0]["fixture"]["date"]
        dt = datetime.fromisoformat(latest.replace("Z", "+00:00"))
        now_utc = datetime.now(dt.tzinfo)
        return max((now_utc - dt).days, 0)
    except Exception:
        return None

def get_last_fixture_context(team_name: str, fixtures: List[Dict[str, Any]]) -> str:
    if not fixtures:
        return f"{team_name} no trae partidos recientes confirmados en la muestra."

    try:
        fx = fixtures[0]
        home = fx["teams"]["home"]["name"]
        away = fx["teams"]["away"]["name"]
        hg = fx["goals"]["home"] if fx["goals"]["home"] is not None else 0
        ag = fx["goals"]["away"] if fx["goals"]["away"] is not None else 0
        days = get_days_since_last_match([fx])

        if limpiar_texto(home) == limpiar_texto(team_name):
            rival = away
            condicion = "local"
            marcador = f"{hg}-{ag}"
        else:
            rival = home
            condicion = "visitante"
            marcador = f"{ag}-{hg}"

        if days is None:
            return f"{team_name} viene de jugar como {condicion} ante {rival} y quedó {marcador}."
        return f"{team_name} viene de jugar como {condicion} ante {rival}, terminó {marcador} y descansó {days} días."
    except Exception:
        return f"No pude reconstruir con claridad el último partido de {team_name}."

def detect_primary_league_from_fixtures(fixtures: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not fixtures:
        return None

    counter = {}
    latest_by_key = {}

    for fx in fixtures:
        league = fx.get("league", {})
        league_id = league.get("id")
        season = league.get("season")
        if not league_id or not season:
            continue

        key = (league_id, season)
        counter[key] = counter.get(key, 0) + 1

        fx_date = fx.get("fixture", {}).get("date", "")
        if key not in latest_by_key or fx_date > latest_by_key[key].get("fixture", {}).get("date", ""):
            latest_by_key[key] = fx

    if not counter:
        return None

    best_key = sorted(counter.items(), key=lambda x: (-x[1], -x[0][1]))[0][0]
    best_fx = latest_by_key[best_key]
    league = best_fx.get("league", {})

    return {
        "league_id": league.get("id"),
        "season": league.get("season"),
        "league_name": league.get("name", ""),
        "country": league.get("country", "")
    }

def get_team_standing(team_id: int, league_id: int, season: int) -> Optional[Dict[str, Any]]:
    data = api_football_get("standings", {"league": league_id, "season": season})
    if not data or "response" not in data or not data["response"]:
        return None

    try:
        standings_groups = data["response"][0]["league"]["standings"]
        for group in standings_groups:
            for row in group:
                if row.get("team", {}).get("id") == team_id:
                    return {
                        "rank": row.get("rank"),
                        "points": row.get("points"),
                        "goalsDiff": row.get("goalsDiff"),
                        "group": row.get("group"),
                        "form": row.get("form"),
                        "description": row.get("description", "")
                    }
    except Exception:
        return None

    return None

def get_team_statistics(team_id: int, league_id: int, season: int) -> Optional[Dict[str, Any]]:
    data = api_football_get("teams/statistics", {
        "league": league_id,
        "season": season,
        "team": team_id
    })

    if not data or "response" not in data:
        return None

    stats = data["response"]
    return {
        "form": stats.get("form", ""),
        "fixtures": stats.get("fixtures", {}),
        "goals_for": stats.get("goals", {}).get("for", {}),
        "goals_against": stats.get("goals", {}).get("against", {}),
        "clean_sheet": stats.get("clean_sheet", {}),
        "failed_to_score": stats.get("failed_to_score", {}),
        "biggest": stats.get("biggest", {}),
        "league": stats.get("league", {})
    }

def safe_get_avg_goals(stats_block: Dict[str, Any], side: str) -> str:
    try:
        return str(stats_block["average"][side])
    except Exception:
        return "N/D"

def build_team_stats_summary(team_name: str, stats: Optional[Dict[str, Any]], standing: Optional[Dict[str, Any]], home_or_away_label: str) -> str:
    if not stats:
        return f"No pude recuperar estadísticas amplias de temporada para {team_name}."

    gf_for = safe_get_avg_goals(stats.get("goals_for", {}), "total")
    ga_against = safe_get_avg_goals(stats.get("goals_against", {}), "total")
    gf_split = safe_get_avg_goals(stats.get("goals_for", {}), home_or_away_label)
    ga_split = safe_get_avg_goals(stats.get("goals_against", {}), home_or_away_label)

    rank_txt = ""
    if standing:
        rank_txt = f"{team_name} marcha en el lugar {standing.get('rank', 'N/D')} con {standing.get('points', 'N/D')} puntos. "

    lugar = "local" if home_or_away_label == "home" else "visitante"
    return (
        f"{rank_txt}"
        f"Promedia {gf_for} goles a favor y {ga_against} en contra por juego; "
        f"en condición de {lugar} su split está en {gf_split} a favor y {ga_split} en contra."
    )

def compute_h2h_summary(home_name: str, away_name: str, h2h: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not h2h:
        return {
            "count": 0,
            "home_wins": 0,
            "away_wins": 0,
            "draws": 0,
            "avg_goals": 0.0,
            "text": f"No hay H2H reciente confirmado entre {home_name} y {away_name}."
        }

    home_wins = away_wins = draws = 0
    total_goals = 0

    for fx in h2h:
        h = fx["teams"]["home"]["name"]
        a = fx["teams"]["away"]["name"]
        hg = fx["goals"]["home"] if fx["goals"]["home"] is not None else 0
        ag = fx["goals"]["away"] if fx["goals"]["away"] is not None else 0
        total_goals += hg + ag

        if hg == ag:
            draws += 1
        else:
            if limpiar_texto(h) == limpiar_texto(home_name):
                if hg > ag:
                    home_wins += 1
                else:
                    away_wins += 1
            elif limpiar_texto(a) == limpiar_texto(home_name):
                if ag > hg:
                    home_wins += 1
                else:
                    away_wins += 1
            else:
                if hg > ag:
                    home_wins += 1
                else:
                    away_wins += 1

    count = len(h2h)
    avg_goals = round(total_goals / count, 2) if count else 0.0

    return {
        "count": count,
        "home_wins": home_wins,
        "away_wins": away_wins,
        "draws": draws,
        "avg_goals": avg_goals,
        "text": (
            f"En los últimos {count} H2H, {home_name} ganó {home_wins}, "
            f"{away_name} ganó {away_wins} y hubo {draws} empates; "
            f"el promedio conjunto fue de {avg_goals} goles."
        )
    }

# =========================================================
# GNEWS
# =========================================================

def news_is_noise(title: str, description: str) -> bool:
    text = limpiar_texto(f"{title} {description}")
    return any(term in text for term in RUMOR_TERMS)

def dedupe_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    seen = set()

    for art in articles:
        key = limpiar_texto(art.get("title", ""))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(art)

    return out

def gnews_search_recent(query: str, days: int = NEWS_LOOKBACK_DAYS, max_articles: int = 10, lang: str = "es") -> List[Dict[str, Any]]:
    from_date = (now_mx() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    data = gnews_get(
        "search",
        {
            "q": query,
            "lang": lang,
            "max": min(max_articles, GNEWS_MAX_ARTICLES),
            "from": from_date,
            "sortby": "publishedAt"
        }
    )
    if not data or "articles" not in data:
        return []
    return data["articles"]

def collect_team_news(team_name: str) -> List[Dict[str, Any]]:
    queries = [
        f'"{team_name}" lesionado OR baja OR suspendido OR convocatoria',
        f'"{team_name}" entrenador OR dt OR rueda de prensa OR declaraciones',
        f'"{team_name}" rotacion OR descanso OR calendario OR desgaste',
        f'"{team_name}" crisis OR presion OR racha OR clasificacion'
    ]

    all_articles = []
    for q in queries:
        all_articles.extend(gnews_search_recent(q, lang="es"))
        all_articles.extend(gnews_search_recent(q, lang="en"))

    all_articles = dedupe_articles(all_articles)

    filtered = []
    team_variants = [limpiar_texto(x) for x in expand_team_variants(team_name)]
    for art in all_articles:
        title = art.get("title", "")
        desc = art.get("description", "")

        if news_is_noise(title, desc):
            continue

        combined = limpiar_texto(f"{title} {desc}")
        if any(v in combined for v in team_variants):
            filtered.append(art)

    return filtered[:MAX_NEWS_ITEMS]

def collect_match_news(home_name: str, away_name: str) -> List[Dict[str, Any]]:
    queries = [
        f'"{home_name}" "{away_name}" lesion suspension lineup preview coach',
        f'"{home_name}" "{away_name}" rotation travel weather',
        f'"{home_name}" "{away_name}" clasificacion forma previa'
    ]

    all_articles = []
    for q in queries:
        all_articles.extend(gnews_search_recent(q, lang="es"))
        all_articles.extend(gnews_search_recent(q, lang="en"))

    all_articles = dedupe_articles(all_articles)

    filtered = []
    home_clean = limpiar_texto(home_name)
    away_clean = limpiar_texto(away_name)

    for art in all_articles:
        title = art.get("title", "")
        desc = art.get("description", "")
        if news_is_noise(title, desc):
            continue

        combined = limpiar_texto(f"{title} {desc}")
        if home_clean in combined or away_clean in combined:
            filtered.append(art)

    return filtered[:MAX_NEWS_ITEMS]

def classify_news_angle(title: str, desc: str) -> str:
    txt = limpiar_texto(f"{title} {desc}")

    if any(x in txt for x in ["lesion", "lesionado", "baja", "suspendido", "injury", "injured", "absence"]):
        return "bajas"
    if any(x in txt for x in ["dt", "entrenador", "coach", "rueda de prensa", "declaraciones", "manager"]):
        return "dt"
    if any(x in txt for x in ["rotacion", "descanso", "fatiga", "calendario", "viaje", "travel"]):
        return "desgaste"
    if any(x in txt for x in ["racha", "crisis", "presion", "clasificacion", "tabla", "form"]):
        return "momento"
    return "general"

def summarize_articles_for_prompt(articles: List[Dict[str, Any]], max_items: int = 4) -> str:
    if not articles:
        return "Sin noticias relevantes recientes confirmadas."

    lines = []
    for art in articles[:max_items]:
        title = cut_text(art.get("title", "").strip(), 120)
        desc = cut_text(art.get("description", "").strip(), 150)
        if desc:
            lines.append(f"- {title} — {desc}")
        else:
            lines.append(f"- {title}")
    return "\n".join(lines)

def summarize_news_angles(team_name: str, articles: List[Dict[str, Any]]) -> str:
    if not articles:
        return f"Sin noticias relevantes recientes de {team_name}."

    buckets = {
        "bajas": [],
        "dt": [],
        "desgaste": [],
        "momento": [],
        "general": []
    }

    for art in articles:
        angle = classify_news_angle(art.get("title", ""), art.get("description", ""))
        snippet = clean_text(f"{art.get('title', '')} — {art.get('description', '')}")
        if snippet:
            buckets[angle].append(cut_text(snippet, 170))

    parts = []
    for key in ["bajas", "dt", "desgaste", "momento", "general"]:
        if buckets[key]:
            parts.append(f"{key.upper()}: " + " | ".join(buckets[key][:2]))

    return "\n".join(parts) if parts else f"Sin noticias relevantes recientes de {team_name}."

def extract_news_angles(articles: List[Dict[str, Any]], team_name: str) -> List[str]:
    if not articles:
        return []

    angles = []
    for art in articles[:3]:
        title = normalize_spaces(art.get("title", ""))
        desc = normalize_spaces(art.get("description", ""))
        snippet = title
        if desc:
            snippet += f": {desc}"
        snippet = cut_text(snippet, 155)
        if snippet:
            start = snippet[0].lower() + snippet[1:] if len(snippet) > 1 else snippet.lower()
            angles.append(f"En noticias recientes sobre {team_name} destaca que {start}.")
    return angles

# =========================================================
# PROMPT
# =========================================================

def build_proheat_prompt(
    home_name: str,
    away_name: str,
    home_form: Dict[str, Any],
    away_form: Dict[str, Any],
    h2h: List[Dict[str, Any]],
    h2h_summary: Dict[str, Any],
    home_rest_days: Optional[int],
    away_rest_days: Optional[int],
    home_last_context: str,
    away_last_context: str,
    home_injuries: List[str],
    away_injuries: List[str],
    home_news_summary: str,
    away_news_summary: str,
    match_news_summary: str,
    weather_summary: str,
    home_stats_summary: str,
    away_stats_summary: str,
    home_standing: Optional[Dict[str, Any]],
    away_standing: Optional[Dict[str, Any]]
) -> str:
    h2h_lines = []
    for fx in h2h[:5]:
        try:
            h = fx["teams"]["home"]["name"]
            a = fx["teams"]["away"]["name"]
            hg = fx["goals"]["home"]
            ag = fx["goals"]["away"]
            h2h_lines.append(f"{h} {hg}-{ag} {a}")
        except Exception:
            continue

    h2h_text = "\n".join(h2h_lines) if h2h_lines else "Sin H2H reciente disponible"

    rest_text = (
        f"{home_name}: {home_rest_days if home_rest_days is not None else 'N/D'} días desde su último partido\n"
        f"{away_name}: {away_rest_days if away_rest_days is not None else 'N/D'} días desde su último partido"
    )

    injuries_text = (
        f"{home_name}: {', '.join(home_injuries) if home_injuries else 'Sin bajas claras reportadas'}\n"
        f"{away_name}: {', '.join(away_injuries) if away_injuries else 'Sin bajas claras reportadas'}"
    )

    return f"""
Eres ProHeat Sport IA.

Tu tarea es analizar el partido {home_name} vs {away_name} con tono profesional, concreto y útil.
NO escribas relleno, NO repitas ideas y NO hagas frases vacías.
El análisis debe sentirse específico del partido y apoyarse en los datos entregados.

Usa:
- forma reciente
- promedio de goles a favor y en contra
- H2H reciente
- descanso
- lesiones
- standings / posición de tabla
- estadísticas de temporada
- noticias recientes
- posibles rotaciones o declaraciones si aparecen
- clima
- prudencia si faltan datos

Datos deportivos:
{home_name}: PJ {home_form['played']}, G {home_form['wins']}, E {home_form['draws']}, P {home_form['losses']}, GF {home_form['gf_avg']}, GC {home_form['ga_avg']}
{away_name}: PJ {away_form['played']}, G {away_form['wins']}, E {away_form['draws']}, P {away_form['losses']}, GF {away_form['gf_avg']}, GC {away_form['ga_avg']}

Estadísticas ampliadas de temporada:
{home_name}: {home_stats_summary}
{away_name}: {away_stats_summary}

Tabla / posición:
{home_name}: {home_standing if home_standing else 'Sin posición confirmada'}
{away_name}: {away_standing if away_standing else 'Sin posición confirmada'}

Descanso:
{rest_text}

Último contexto:
{home_name}: {home_last_context}
{away_name}: {away_last_context}

Lesiones / bajas:
{injuries_text}

Resumen H2H:
{h2h_summary['text']}

Detalle H2H:
{h2h_text}

Noticias local:
{home_news_summary}

Noticias visitante:
{away_news_summary}

Noticias del partido:
{match_news_summary}

Clima:
{weather_summary}

Devuelve SOLO JSON válido con esta estructura exacta:
{{
  "pick_principal": "texto",
  "doble_oportunidad": {{"pick": "texto", "probabilidad": "texto"}},
  "marcador_global": {{"linea": "texto", "probabilidad": "texto"}},
  "goles": {{
    "local": {{"valor": "texto", "probabilidad": "texto"}},
    "visitante": {{"valor": "texto", "probabilidad": "texto"}}
  }},
  "sot": {{"linea": "texto", "probabilidad": "texto"}},
  "corners": {{"linea": "texto", "probabilidad": "texto"}},
  "tarjetas": {{"linea": "texto", "probabilidad": "texto"}},
  "analisis": "8 líneas exactas separadas por \\n"
}}

Reglas obligatorias:
- El campo "analisis" debe tener EXACTAMENTE 8 líneas.
- Cada línea debe aportar un ángulo distinto.
- No uses viñetas ni numeración.
- No uses frases como "duelo que debe leerse con cautela" ni otras frases plantilla.
- No repitas información entre líneas.
- Si faltan datos, dilo de forma concreta y sigue con lo que sí se puede inferir.
- Menciona nombres de los equipos de forma natural.
- Mantén cada línea breve, clara y específica.
- En marcador_global usa formato como +1.5 o -3.5.
- En goles devuelve valor y probabilidad por separado.
- No agregues texto fuera del JSON.
""".strip()

# =========================================================
# PARSE JSON
# =========================================================

def parse_json_response(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        raise ValueError("No se pudo parsear JSON válido.")

# =========================================================
# 8 LÍNEAS NO GENÉRICAS
# =========================================================

def line_quality_ok(line: str) -> bool:
    if not line:
        return False
    s = normalize_spaces(line)
    if len(s) < 35:
        return False

    banned = [
        "debe leerse con cautela",
        "partido interesante",
        "puede pasar cualquier cosa",
        "habrá que ver",
        "todo puede suceder",
        "es un duelo parejo",
        "sin duda",
        "sin lugar a dudas",
        "será clave"
    ]
    s_low = limpiar_texto(s)
    return not any(b in s_low for b in banned)

def unique_lines(lines: List[str]) -> List[str]:
    out = []
    seen = set()
    for line in lines:
        s = normalize_spaces(line)
        key = limpiar_texto(s)
        if not s or key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out

def build_specific_fallback_lines(ctx: Dict[str, Any]) -> List[str]:
    home = ctx["home_name"]
    away = ctx["away_name"]
    home_form = ctx["home_form"]
    away_form = ctx["away_form"]
    home_rest = ctx["home_rest_days"]
    away_rest = ctx["away_rest_days"]
    home_last_context = ctx["home_last_context"]
    away_last_context = ctx["away_last_context"]
    home_injuries = ctx["home_injuries"]
    away_injuries = ctx["away_injuries"]
    h2h_summary = ctx["h2h_summary"]
    weather_summary = ctx["weather_summary"]
    home_news = ctx["home_news"]
    away_news = ctx["away_news"]
    match_news = ctx["match_news"]
    home_stats_summary = ctx["home_stats_summary"]
    away_stats_summary = ctx["away_stats_summary"]

    lines = []

    if home_form["played"] and away_form["played"]:
        lines.append(
            f"{home} llega con balance de {home_form['wins']}-{home_form['draws']}-{home_form['losses']} y {home_form['gf_avg']} goles a favor por juego, mientras {away} trae {away_form['wins']}-{away_form['draws']}-{away_form['losses']} y {away_form['gf_avg']} de media ofensiva."
        )
        lines.append(
            f"En defensa, {home} ha permitido {home_form['ga_avg']} goles por partido en su muestra reciente y {away} concede {away_form['ga_avg']}, dato que ayuda a medir si el cruce apunta a un marcador corto o abierto."
        )

    if home_stats_summary:
        lines.append(home_stats_summary)
    if away_stats_summary:
        lines.append(away_stats_summary)

    if h2h_summary["count"] > 0:
        lines.append(h2h_summary["text"])
    else:
        lines.append(f"No hay una muestra reciente de enfrentamientos directos entre {home} y {away}, así que el peso del análisis recae más en forma, tabla, bajas y contexto actual.")

    if home_rest is not None and away_rest is not None:
        if home_rest > away_rest:
            lines.append(f"{home} tiene una ventaja ligera de descanso con {home_rest} días desde su último partido, contra {away} que llega con {away_rest}, algo que puede sentirse más en la segunda mitad.")
        elif away_rest > home_rest:
            lines.append(f"{away} llega con mejor descanso, {away_rest} días frente a {home} que trae {home_rest}, y ese margen puede influir en ritmo, presión y cambios.")
        else:
            lines.append(f"Ambos llegan con una carga similar de descanso, {home_rest} días para cada lado, por lo que no hay una ventaja física clara desde el calendario.")

    lines.append(home_last_context)
    lines.append(away_last_context)

    if home_injuries:
        lines.append(f"En {home} sí hay bajas a vigilar: {', '.join(home_injuries[:3])}, lo que puede tocar rotación, volumen ofensivo o estabilidad defensiva según los nombres implicados.")
    else:
        lines.append(f"No aparecen bajas fuertes claramente confirmadas en {home}, así que en principio su lectura competitiva parte de una base relativamente estable.")

    if away_injuries:
        lines.append(f"{away} también llega con ausencias reportadas: {', '.join(away_injuries[:3])}, un punto importante si faltan piezas de ataque o equilibrio en medio campo.")
    else:
        lines.append(f"En {away} tampoco se detectan ausencias claras de peso en la muestra consultada, lo que reduce el ruido previo al silbatazo.")

    lines.append(weather_summary)

    for item in extract_news_angles(home_news, home):
        lines.append(item)

    for item in extract_news_angles(away_news, away):
        lines.append(item)

    if match_news:
        title = normalize_spaces(match_news[0].get("title", ""))
        desc = normalize_spaces(match_news[0].get("description", ""))
        snippet = cut_text(f"{title}. {desc}".strip(". "), 160)
        if snippet:
            lines.append(f"En la previa del partido también aparece este foco reciente: {snippet}")

    lines.append(
        f"Con la información disponible, el guion más probable depende de si {home} logra imponer su tramo fuerte antes del descanso o si {away} consigue llevar el partido a un ritmo más controlado."
    )

    return unique_lines([cut_text(x, 190) for x in lines if x])

def ensure_8_lines_blog(text: str, ctx: Dict[str, Any]) -> str:
    ai_lines = [normalize_spaces(x) for x in str(text or "").splitlines() if normalize_spaces(x)]
    ai_lines = [x for x in ai_lines if line_quality_ok(x)]
    ai_lines = unique_lines(ai_lines)

    fallback_lines = build_specific_fallback_lines(ctx)

    final_lines = []
    used = set()

    for line in ai_lines + fallback_lines:
        key = limpiar_texto(line)
        if key not in used and line_quality_ok(line):
            used.add(key)
            final_lines.append(cut_text(line, 190))
        if len(final_lines) == 8:
            break

    if len(final_lines) < 8:
        home = ctx["home_name"]
        away = ctx["away_name"]
        extra_templates = [
            f"La comparación reciente entre producción ofensiva de {home} y resistencia defensiva de {away} marca uno de los puntos más sensibles de la previa.",
            f"También habrá que medir si {away} sostiene su plan fuera de casa o si {home} consigue inclinar el partido desde la localía y el primer tramo del juego.",
            f"Más que un pronóstico vacío, este partido pide leer detalle por detalle: forma corta, tabla, bajas, descanso y señales recientes del entorno competitivo."
        ]
        for line in extra_templates:
            key = limpiar_texto(line)
            if key not in used:
                used.add(key)
                final_lines.append(cut_text(line, 190))
            if len(final_lines) == 8:
                break

    final_lines = final_lines[:8]

    if len(final_lines) < 8:
        home = ctx["home_name"]
        away = ctx["away_name"]
        while len(final_lines) < 8:
            idx = len(final_lines) + 1
            final_lines.append(
                cut_text(
                    f"Ángulo {idx}: la lectura de {home} vs {away} sigue apoyándose en datos concretos de forma, tabla, contexto inmediato y disponibilidad del plantel.",
                    190
                )
            )

    return "\n".join(final_lines)

# =========================================================
# FORMATO RESPUESTA
# =========================================================

def normalize_goles_payload(goles_payload: Any) -> Dict[str, Dict[str, str]]:
    default = {
        "local": {"valor": "-", "probabilidad": "-"},
        "visitante": {"valor": "-", "probabilidad": "-"}
    }

    if not isinstance(goles_payload, dict):
        return default

    local = goles_payload.get("local", {})
    visitante = goles_payload.get("visitante", {})

    if isinstance(local, str):
        local = {"valor": local, "probabilidad": "-"}
    if isinstance(visitante, str):
        visitante = {"valor": visitante, "probabilidad": "-"}

    return {
        "local": {
            "valor": str(local.get("valor", "-")),
            "probabilidad": str(local.get("probabilidad", "-")),
        },
        "visitante": {
            "valor": str(visitante.get("valor", "-")),
            "probabilidad": str(visitante.get("probabilidad", "-")),
        }
    }

def format_sport_ia_picks(home_name: str, away_name: str, payload: Dict[str, Any]) -> str:
    doble = payload.get("doble_oportunidad", {}) or {}
    marcador_global = payload.get("marcador_global", {}) or {}
    goles = normalize_goles_payload(payload.get("goles", {}))
    sot = payload.get("sot", {}) or {}
    corners = payload.get("corners", {}) or {}
    tarjetas = payload.get("tarjetas", {}) or {}

    return (
        "🤖 PROHEAT SPORT IA\n\n"
        "⚽ Partido\n"
        "⏰ Por confirmar\n"
        f"🏟️ {home_name} vs {away_name}\n\n"
        f"📊 {payload.get('pick_principal', '-')}\n"
        f"🔒 Doble oportunidad: {doble.get('pick', '-')} ({doble.get('probabilidad', '-')})\n"
        f"📈 Marcador Global: {marcador_global.get('linea', '-')} ({marcador_global.get('probabilidad', '-')})\n"
        f"⚽ Goles: L {goles['local']['valor']} ({goles['local']['probabilidad']}) | V {goles['visitante']['valor']} ({goles['visitante']['probabilidad']})\n"
        f"🎯 SoT: {sot.get('linea', '-')} ({sot.get('probabilidad', '-')})\n"
        f"📐 Corners: {corners.get('linea', '-')} ({corners.get('probabilidad', '-')})\n"
        f"🟨 Tarjetas: {tarjetas.get('linea', '-')} ({tarjetas.get('probabilidad', '-')})"
    )

def format_sport_ia_blog(payload: Dict[str, Any]) -> str:
    return f"🧠 Nota ProHeat\n\n{payload.get('analisis', 'Sin análisis disponible.')}"

# =========================================================
# SPORT IA
# =========================================================

def run_sport_ia_analysis(match_text: str) -> Tuple[Optional[str], str]:
    if not openai_client:
        return None, "❌ Falta configurar OPENAI_API_KEY."
    if not API_FOOTBALL_KEY:
        return None, "❌ Falta configurar API_FOOTBALL_KEY."

    cached = get_cached_analysis(match_text)
    if cached:
        try:
            home_name, away_name = parse_match_input(match_text)
            if not home_name or not away_name:
                return None, "⚠️ Escribe el partido así:\nLiverpool vs Galatasaray"
            return (
                format_sport_ia_picks(home_name, away_name, cached),
                format_sport_ia_blog(cached)
            )
        except Exception:
            logger.warning("Cache inválida para %s", match_text)

    home_name, away_name = parse_match_input(match_text)
    if not home_name or not away_name:
        return None, "⚠️ Escribe el partido así:\nLiverpool vs Galatasaray"

    home_team = search_team(home_name)
    away_team = search_team(away_name)

    if not home_team or not away_team:
        return None, "❌ No pude identificar uno o ambos equipos con suficiente precisión."

    if home_team["id"] == away_team["id"]:
        return None, "❌ Detecté ambos equipos como el mismo club. Escribe el partido con un nombre más específico."

    home_fixtures = get_recent_fixtures(home_team["id"], RECENT_FIXTURES_COUNT)
    away_fixtures = get_recent_fixtures(away_team["id"], RECENT_FIXTURES_COUNT)
    h2h = get_h2h(home_team["id"], away_team["id"], 5)

    home_form = summarize_team_form(home_team["name"], home_fixtures)
    away_form = summarize_team_form(away_team["name"], away_fixtures)

    home_rest_days = get_days_since_last_match(home_fixtures)
    away_rest_days = get_days_since_last_match(away_fixtures)

    home_last_context = get_last_fixture_context(home_team["name"], home_fixtures)
    away_last_context = get_last_fixture_context(away_team["name"], away_fixtures)

    home_injuries = get_team_injuries(home_team["id"])
    away_injuries = get_team_injuries(away_team["id"])

    home_league_info = detect_primary_league_from_fixtures(home_fixtures)
    away_league_info = detect_primary_league_from_fixtures(away_fixtures)

    home_standing = None
    away_standing = None
    home_stats = None
    away_stats = None

    if home_league_info:
        home_standing = get_team_standing(
            home_team["id"],
            home_league_info["league_id"],
            home_league_info["season"]
        )
        home_stats = get_team_statistics(
            home_team["id"],
            home_league_info["league_id"],
            home_league_info["season"]
        )

    if away_league_info:
        away_standing = get_team_standing(
            away_team["id"],
            away_league_info["league_id"],
            away_league_info["season"]
        )
        away_stats = get_team_statistics(
            away_team["id"],
            away_league_info["league_id"],
            away_league_info["season"]
        )

    home_stats_summary = build_team_stats_summary(home_team["name"], home_stats, home_standing, "home")
    away_stats_summary = build_team_stats_summary(away_team["name"], away_stats, away_standing, "away")

    h2h_summary = compute_h2h_summary(home_team["name"], away_team["name"], h2h)

    home_news = collect_team_news(home_team["name"])
    away_news = collect_team_news(away_team["name"])
    match_news = collect_match_news(home_team["name"], away_team["name"])

    home_news_summary = summarize_news_angles(home_team["name"], home_news)
    away_news_summary = summarize_news_angles(away_team["name"], away_news)
    match_news_summary = summarize_articles_for_prompt(match_news)

    weather_summary = get_weather_context(
        home_team.get("venue_city", ""),
        home_team.get("country", "")
    )

    prompt = build_proheat_prompt(
        home_team["name"],
        away_team["name"],
        home_form,
        away_form,
        h2h,
        h2h_summary,
        home_rest_days,
        away_rest_days,
        home_last_context,
        away_last_context,
        home_injuries,
        away_injuries,
        home_news_summary,
        away_news_summary,
        match_news_summary,
        weather_summary,
        home_stats_summary,
        away_stats_summary,
        home_standing,
        away_standing
    )

    ctx_for_lines = {
        "home_name": home_team["name"],
        "away_name": away_team["name"],
        "home_form": home_form,
        "away_form": away_form,
        "h2h_summary": h2h_summary,
        "home_rest_days": home_rest_days,
        "away_rest_days": away_rest_days,
        "home_last_context": home_last_context,
        "away_last_context": away_last_context,
        "home_injuries": home_injuries,
        "away_injuries": away_injuries,
        "weather_summary": weather_summary,
        "home_news": home_news,
        "away_news": away_news,
        "match_news": match_news,
        "home_stats_summary": home_stats_summary,
        "away_stats_summary": away_stats_summary
    }

    try:
        response = openai_client.responses.create(
            model=OPENAI_MODEL,
            input=prompt
        )
        raw = (response.output_text or "").strip()
        payload = parse_json_response(raw)

        if not isinstance(payload, dict):
            raise ValueError("La respuesta del modelo no fue un objeto JSON.")

        payload.setdefault("pick_principal", "-")
        payload.setdefault("doble_oportunidad", {"pick": "-", "probabilidad": "-"})
        payload.setdefault("marcador_global", {"linea": "-", "probabilidad": "-"})
        payload["goles"] = normalize_goles_payload(payload.get("goles", {}))
        payload.setdefault("sot", {"linea": "-", "probabilidad": "-"})
        payload.setdefault("corners", {"linea": "-", "probabilidad": "-"})
        payload.setdefault("tarjetas", {"linea": "-", "probabilidad": "-"})

        payload["analisis"] = ensure_8_lines_blog(payload.get("analisis", ""), ctx_for_lines)

        save_cached_analysis(match_text, payload)

        return (
            format_sport_ia_picks(home_team["name"], away_team["name"], payload),
            format_sport_ia_blog(payload)
        )

    except Exception as e:
        logger.exception("Error en OpenAI")
        return None, f"❌ Error generando el análisis:\n{str(e)}"

# =========================================================
# HANDLERS TELEGRAM
# =========================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user_id = update.message.from_user.id

    if not is_allowed(user_id):
        await update.message.reply_text(
            "💎 PROHEAT SPORTS PREMIUM\n\n"
            "Acceso mensual: $100 MXN\n\n"
            "Envía comprobante en imagen\n\n"
            "Usa /myid"
        )
        return

    context.user_data["sport_ia_mode"] = False
    context.user_data["admin_action"] = None

    await update.message.reply_text(
        "📊 PROHEAT SPORTS\nSelecciona una opción:",
        reply_markup=build_main_menu_for_user(user_id)
    )

async def my_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    await update.message.reply_text(f"🆔 {update.message.from_user.id}")

async def usuarios_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Este comando es solo para administradores.")
        return

    await update.message.reply_text(build_users_report())

async def eliminar_usuario_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Este comando es solo para administradores.")
        return

    if not context.args:
        await update.message.reply_text("Uso: /eliminar_usuario 123456789")
        return

    target_user_id = context.args[0].strip()
    ok = delete_user_membership(target_user_id)

    if not ok:
        await update.message.reply_text("❌ Ese usuario no existe en usuarios.json")
        return

    try:
        await context.bot.send_message(
            chat_id=int(target_user_id),
            text=(
                "🔒 Tu acceso a ProHeat Sports fue removido por administración.\n"
                "Si deseas volver a usar el bot, envía tu comprobante para reactivar tu membresía."
            )
        )
    except Exception:
        logger.exception("No se pudo avisar al usuario eliminado %s", target_user_id)

    await update.message.reply_text(f"✅ Usuario {target_user_id} eliminado manualmente.")

async def handle_receipt_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user_id = update.message.from_user.id

    if is_allowed(user_id):
        await update.message.reply_text("✅ Imagen recibida.")
        return

    create_or_update_pending_user(user_id)

    caption = (
        "📩 NUEVO COMPROBANTE RECIBIDO\n"
        f"Usuario ID: {user_id}\n"
        "Revisa la imagen y valida si el comprobante es real.\n\n"
        "Para aprobar usa el botón de abajo."
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Aprobar", callback_data=f"approve_{user_id}")]
    ])

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_photo(
                chat_id=admin_id,
                photo=update.message.photo[-1].file_id,
                caption=caption,
                reply_markup=keyboard
            )
        except Exception:
            logger.exception("No se pudo reenviar comprobante al admin %s", admin_id)

    await update.message.reply_text("📩 Comprobante recibido. Será revisado por administración.")


async def send_long_message(message, text: str, chunk_size: int = 3500):
    """
    Envía respuestas largas en varios mensajes para evitar el límite de Telegram.
    Divide preferentemente por separadores de bloque ProHeat.
    """
    text = str(text or "").strip()

    if not text:
        await message.reply_text("⚠️ Sin datos para mostrar.")
        return

    if len(text) <= chunk_size:
        await message.reply_text(text)
        return

    parts = []
    current = ""

    blocks = text.split("━━━━━━━━━━━━━━━")
    for block in blocks:
        block = block.strip()
        if not block:
            continue

        candidate = f"{current}\n━━━━━━━━━━━━━━━\n{block}" if current else block

        if len(candidate) > chunk_size:
            if current:
                parts.append(current.strip())
                current = block
            else:
                # Si un solo bloque supera el tamaño, lo corta en partes seguras.
                for i in range(0, len(block), chunk_size):
                    parts.append(block[i:i + chunk_size].strip())
                current = ""
        else:
            current = candidate

    if current:
        parts.append(current.strip())

    total = len(parts)
    for i, part in enumerate(parts, start=1):
        header = f"Parte {i}/{total}\n\n" if total > 1 else ""
        await message.reply_text(header + part)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user_id = update.message.from_user.id
    raw_text = update.message.text or ""
    msg = raw_text.lower().strip()

    if not is_allowed(user_id):
        create_or_update_pending_user(user_id)

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Aprobar", callback_data=f"approve_{user_id}")]
        ])

        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"📩 Solicitud de acceso\nID: {user_id}",
                    reply_markup=keyboard
                )
            except Exception:
                logger.exception("No se pudo avisar al admin %s", admin_id)

        await update.message.reply_text(
            "📩 Espera aprobación.\n\n"
            "Si ya realizaste el pago, envía el comprobante en imagen."
        )
        return

    # =========================
    # PANEL ADMIN - FLUJOS
    # =========================
    if is_admin(user_id):
        admin_action = context.user_data.get("admin_action")

        if admin_action == "approve_user_input":
            target_user_id = raw_text.strip()
            if not target_user_id.isdigit():
                await update.message.reply_text("❌ Escribe un ID numérico válido.")
                return

            user_data = approve_user_membership(target_user_id, days=30)
            context.user_data["admin_action"] = None

            try:
                await context.bot.send_message(chat_id=int(target_user_id), text="✅ Acceso aprobado")
                await context.bot.send_message(
                    chat_id=int(target_user_id),
                    text="Selecciona opción:",
                    reply_markup=build_main_menu_for_user(int(target_user_id))
                )
            except Exception:
                logger.exception("No se pudo notificar al usuario aprobado manualmente")

            await update.message.reply_text(
                f"✅ Usuario {target_user_id} aprobado.\n"
                f"Inicio: {user_data.get('start_date')}\n"
                f"Fin: {user_data.get('expires')}"
            )
            return

        if admin_action == "delete_user_input":
            target_user_id = raw_text.strip()
            if not target_user_id.isdigit():
                await update.message.reply_text("❌ Escribe un ID numérico válido.")
                return

            ok = delete_user_membership(target_user_id)
            context.user_data["admin_action"] = None

            if not ok:
                await update.message.reply_text("❌ Ese usuario no existe en usuarios.json")
                return

            try:
                await context.bot.send_message(
                    chat_id=int(target_user_id),
                    text=(
                        "🔒 Tu acceso a ProHeat Sports fue removido por administración.\n"
                        "Si deseas volver a usar el bot, envía tu comprobante para reactivar tu membresía."
                    )
                )
            except Exception:
                logger.exception("No se pudo avisar al usuario eliminado %s", target_user_id)

            await update.message.reply_text(f"✅ Usuario {target_user_id} eliminado manualmente.")
            return

        if admin_action in {"extend_user_15", "extend_user_30", "extend_user_60", "extend_user_90", "trial_user_7"}:
            target_user_id = raw_text.strip()
            if not target_user_id.isdigit():
                await update.message.reply_text("❌ Escribe un ID numérico válido.")
                return

            if admin_action == "trial_user_7":
                user_data = approve_user_membership(target_user_id, days=7, is_trial=True)
                context.user_data["admin_action"] = None

                try:
                    await context.bot.send_message(
                        chat_id=int(target_user_id),
                        text=(
                            "🎁 Se activó tu prueba gratuita de ProHeat Sports por 7 días.\n"
                            f"Fin: {user_data.get('expires')}"
                        )
                    )
                    await context.bot.send_message(
                        chat_id=int(target_user_id),
                        text="Selecciona opción:",
                        reply_markup=build_main_menu_for_user(int(target_user_id))
                    )
                except Exception:
                    logger.exception("No se pudo avisar al usuario de prueba %s", target_user_id)

                await update.message.reply_text(
                    f"🎁 Prueba gratuita activada para {target_user_id}.\n"
                    f"Inicio: {user_data.get('start_date')}\n"
                    f"Fin: {user_data.get('expires')}"
                )
                return

            days_map = {
                "extend_user_15": 15,
                "extend_user_30": 30,
                "extend_user_60": 60,
                "extend_user_90": 90,
            }
            days = days_map[admin_action]
            user_data = extend_user_membership(target_user_id, days)
            context.user_data["admin_action"] = None

            if not user_data:
                await update.message.reply_text("❌ Ese usuario no existe en usuarios.json")
                return

            try:
                await context.bot.send_message(
                    chat_id=int(target_user_id),
                    text=(
                        f"✅ Tu suscripción de ProHeat Sports fue extendida {days} días.\n"
                        f"Nuevo vencimiento: {user_data.get('expires')}"
                    )
                )
            except Exception:
                logger.exception("No se pudo avisar al usuario extendido %s", target_user_id)

            await update.message.reply_text(
                f"✅ Usuario {target_user_id} extendido {days} días.\n"
                f"Nuevo fin: {user_data.get('expires')}"
            )
            return

    if context.user_data.get("sport_ia_mode"):
        context.user_data["sport_ia_mode"] = False

        remaining = remaining_queries_today(user_id)
        if remaining <= 0:
            await update.message.reply_text(f"⚠️ Ya usaste tus {SPORT_IA_DAILY_LIMIT} consultas de ProHeat Sport IA hoy.")
            return

        await update.message.reply_text("⏳ Analizando partido con ProHeat Sport IA...")
        picks_text, blog_text = run_sport_ia_analysis(raw_text)

        if picks_text:
            increment_user_usage(user_id)
            restantes = remaining_queries_today(user_id)
            await update.message.reply_text(picks_text)
            await update.message.reply_text(f"{blog_text}\n\nConsultas restantes hoy: {restantes}")
        else:
            restantes = remaining_queries_today(user_id)
            await update.message.reply_text(f"{blog_text}\n\nConsultas restantes hoy: {restantes}")
        return

    if is_admin(user_id) and "panel de administradores" in msg:
        await update.message.reply_text("🛠️ PANEL DE ADMINISTRADORES", reply_markup=admin_panel_menu)
        return

    if "guía" in msg or "guia" in msg:
        await update.message.reply_text(guia_texto(), parse_mode="Markdown")
        return

    if "proheat sport ia" in msg or "sport ia" in msg:
        restantes = remaining_queries_today(user_id)
        await update.message.reply_text(
            "🤖 PROHEAT SPORT IA ACTIVADO\n\n"
            "Escribe el partido en este formato:\n"
            "Equipo Local vs Equipo Visitante\n\n"
            "Ejemplo:\n"
            "Real Madrid vs Atletico de Madrid\n\n"
            f"Consultas disponibles hoy: {restantes}\n"
            f"Límite diario actual: {SPORT_IA_DAILY_LIMIT}"
        )
        context.user_data["sport_ia_mode"] = True
        return

    if "inferno" in msg:
        await send_long_message(update.message, read_sheet("Hoja9"))
        return

    if "partidos" in msg:
        await send_long_message(update.message, read_sheet("Hoja1"))
        return

    if "hot predicciones" in msg or "picks" in msg or "predicciones" in msg:
        await update.message.reply_text("🔥 Selecciona una sección de Hot Predicciones:", reply_markup=hot_predicciones_menu)
        return

async def handle_picks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    await send_long_message(query.message, read_sheet(query.data))

async def handle_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    user_id = query.from_user.id
    if not is_admin(user_id):
        await query.answer("Solo admin", show_alert=True)
        return

    await query.answer()
    data = query.data

    if data == "admin_view_users":
        await query.message.reply_text(build_users_report())
        return

    if data == "admin_view_pending":
        await query.message.reply_text(get_pending_users_report())
        return

    if data == "admin_prompt_approve":
        await query.message.reply_text(
            "✅ Escribe el ID del usuario que deseas aprobar por 30 días.\n\n"
            f"{get_pending_users_report()}"
        )
        context.user_data["admin_action"] = "approve_user_input"
        return

    if data == "admin_prompt_delete":
        await query.message.reply_text("🗑️ Escribe el ID del usuario que deseas eliminar manualmente.")
        context.user_data["admin_action"] = "delete_user_input"
        return

    if data == "admin_prompt_extend_15":
        await query.message.reply_text("⏳ Escribe el ID del usuario al que deseas extender 15 días.")
        context.user_data["admin_action"] = "extend_user_15"
        return

    if data == "admin_prompt_extend_30":
        await query.message.reply_text("⏳ Escribe el ID del usuario al que deseas extender 30 días.")
        context.user_data["admin_action"] = "extend_user_30"
        return

    if data == "admin_prompt_extend_60":
        await query.message.reply_text("📆 Escribe el ID del usuario al que deseas extender 60 días.")
        context.user_data["admin_action"] = "extend_user_60"
        return

    if data == "admin_prompt_extend_90":
        await query.message.reply_text("📅 Escribe el ID del usuario al que deseas extender 90 días.")
        context.user_data["admin_action"] = "extend_user_90"
        return

    if data == "admin_prompt_trial_7":
        await query.message.reply_text("🎁 Escribe el ID del usuario al que deseas activar prueba gratuita de 7 días.")
        context.user_data["admin_action"] = "trial_user_7"
        return

async def approve_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    admin_user_id = query.from_user.id
    if not is_admin(admin_user_id):
        await query.answer("Solo admin", show_alert=True)
        return

    await query.answer()

    try:
        user_id = query.data.split("_", 1)[1]
    except Exception:
        await query.edit_message_text("No pude interpretar el ID del usuario.")
        return

    user_data = approve_user_membership(user_id, days=30)

    try:
        await context.bot.send_message(chat_id=user_id, text="✅ Acceso aprobado")
        await context.bot.send_message(
            chat_id=user_id,
            text="Selecciona opción:",
            reply_markup=build_main_menu_for_user(int(user_id))
        )
    except Exception:
        logger.exception("No se pudo notificar al usuario aprobado")

    await query.edit_message_text(
        f"Usuario {user_id} aprobado.\n"
        f"Inicio: {user_data.get('start_date')}\n"
        f"Fin: {user_data.get('expires')}"
    )

# =========================================================
# VALIDACIONES
# =========================================================

def validate_environment() -> None:
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not OPENAI_API_KEY:
        missing.append("OPENAI_API_KEY")
    if not API_FOOTBALL_KEY:
        missing.append("API_FOOTBALL_KEY")

    if missing:
        raise RuntimeError(f"Faltan variables de entorno obligatorias: {', '.join(missing)}")

    logger.info("Entorno validado correctamente.")
    logger.info("Excel fallback file: %s", EXCEL_FILE)
    logger.info("ProHeat API base: %s", PROHEAT_API_BASE)
    logger.info("Modelo OpenAI: %s", OPENAI_MODEL)
    logger.info("Sport IA daily limit: %s", SPORT_IA_DAILY_LIMIT)
    logger.info("Admins cargados: %s", sorted(list(ADMIN_IDS)))

# =========================================================
# MAIN
# =========================================================

def main():
    validate_environment()

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("myid", my_id))
    app.add_handler(CommandHandler("usuarios", usuarios_cmd))
    app.add_handler(CommandHandler("eliminar_usuario", eliminar_usuario_cmd))
    app.add_handler(MessageHandler(filters.PHOTO, handle_receipt_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_picks, pattern="^Hoja[1-9]$"))
    app.add_handler(CallbackQueryHandler(handle_admin_panel, pattern=r"^admin_"))
    app.add_handler(CallbackQueryHandler(approve_user, pattern=r"^approve_\d+$"))

    # Solo programa trabajos si JobQueue está disponible
    if app.job_queue:
        app.job_queue.run_repeating(check_subscriptions, interval=43200, first=30)
        logger.info("JobQueue activado correctamente.")
    else:
        logger.warning("JobQueue no disponible. Instala python-telegram-bot[job-queue] para activar recordatorios automáticos.")

    logger.info("ProHeat Bot iniciado...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()