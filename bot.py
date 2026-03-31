import os
import re
import json
import math
import time
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
# CONFIG GENERAL
# =========================================================

MX_TZ = ZoneInfo("America/Mexico_City")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "").strip()
API_FOOTBALL_BASE = "https://v3.football.api-sports.io"

GNEWS_API_KEY = os.getenv("GNEWS_API_KEY", "").strip()
GNEWS_BASE = "https://gnews.io/api/v4"

OPEN_METEO_GEOCODING_BASE = "https://geocoding-api.open-meteo.com/v1/search"
OPEN_METEO_FORECAST_BASE = "https://api.open-meteo.com/v1/forecast"

ADMIN_ID = int(os.getenv("ADMIN_ID", "7696799656"))
USERS_FILE = os.getenv("USERS_FILE", "usuarios.json")
SPORT_IA_USAGE_FILE = os.getenv("SPORT_IA_USAGE_FILE", "sport_ia_usage.json")
SPORT_IA_CACHE_FILE = os.getenv("SPORT_IA_CACHE_FILE", "sport_ia_cache.json")
EXCEL_FILE = os.getenv("EXCEL_FILE", "data.xlsx")

SPORT_IA_DAILY_LIMIT = int(os.getenv("SPORT_IA_DAILY_LIMIT", "10"))
NEWS_LOOKBACK_DAYS = int(os.getenv("NEWS_LOOKBACK_DAYS", "5"))
MAX_NEWS_ITEMS = int(os.getenv("MAX_NEWS_ITEMS", "6"))
GNEWS_MAX_ARTICLES = int(os.getenv("GNEWS_MAX_ARTICLES", "6"))
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

menu = [
    ["📘 Guía de Uso"],
    ["🤖 ProHeat Sport IA"],
    ["Partidos del Día"],
    ["📊 Picks"],
    ["🔥 PICKS INFERNO 🔥"]
]

reply_markup = ReplyKeyboardMarkup(menu, resize_keyboard=True)

picks_menu = InlineKeyboardMarkup([
    [InlineKeyboardButton("Pronósticos Premium", callback_data="Hoja2")],
    [InlineKeyboardButton("Manejo de Banca", callback_data="Hoja3")],
    [InlineKeyboardButton("Combinadas Recomendadas", callback_data="Hoja4")],
    [InlineKeyboardButton("Marcadores Probables", callback_data="Hoja5")],
    [InlineKeyboardButton("Picks Top 10", callback_data="Hoja6")]
])

sheet_titles = {
    "Hoja1": "🔥 PARTIDOS DEL DÍA 🔥",
    "Hoja2": "🔥 PRONÓSTICOS PREMIUM 🔥",
    "Hoja3": "🔥 MANEJO DE BANCA 🔥",
    "Hoja4": "🔥 COMBINADAS RECOMENDADAS 🔥",
    "Hoja5": "🔥 MARCADORES MÁS PROBABLES 🔥",
    "Hoja6": "🔥 PICKS TOP 10 🔥",
    "Hoja7": "🔥 PICKS INFERNO 🔥",
}

# =========================================================
# ALIAS / LIMPIEZA DE EQUIPOS
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

# =========================================================
# HELPERS GENERALES
# =========================================================

def now_mx() -> datetime:
    return datetime.now(MX_TZ)

def today_mx() -> str:
    return now_mx().strftime("%Y-%m-%d")

def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"http\S+", "", str(text))
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def limpiar_texto(texto: Any) -> str:
    texto = str(texto).lower().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    texto = re.sub(r"[^a-z0-9\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto

def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()

def cut_text(text: str, max_len: int = 180) -> str:
    text = normalize_spaces(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"

def atomic_write_json(path: str, data: Any) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    os.replace(tmp_path, path)

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

def hash_key(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# =========================================================
# USUARIOS / ACCESOS
# =========================================================

def load_users() -> Dict[str, Any]:
    return load_json_file(USERS_FILE, {})

def save_users(data: Dict[str, Any]) -> None:
    save_json_file(USERS_FILE, data)

def is_user_active(user_data: Dict[str, Any]) -> bool:
    if not user_data:
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
    if user_id == ADMIN_ID:
        return True
    users = load_users()
    user_data = users.get(str(user_id))
    return is_user_active(user_data)

# =========================================================
# USO DIARIO SPORT IA
# =========================================================

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

# =========================================================
# CACHE SPORT IA
# =========================================================

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
        "result": result,
    }
    save_cache(cache)

# =========================================================
# EXCEL
# =========================================================

def format_cell_value(value: Any) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%H:%M")
    return str(value).strip()

def read_sheet(sheet_name: str) -> str:
    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name=sheet_name)
        if df.empty:
            return "⚠️ Sin datos"

        title = sheet_titles.get(sheet_name, sheet_name)
        text_parts = [title, ""]

        for _, row in df.iterrows():
            valores = [format_cell_value(x) for x in row if str(format_cell_value(x)).strip()]

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

                text_parts.append("\n".join(block))
                text_parts.append("━━━━━━━━━━━━━━━")
                text_parts.append("")
            else:
                text_parts.append("• " + " | ".join(valores))
                text_parts.append("")

        return "\n".join(text_parts).strip()

    except Exception as e:
        logger.exception("Error leyendo Excel")
        return f"❌ Error leyendo Excel:\n{str(e)}"

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
        "💎 *Pronósticos Premium*\n"
        "Selección de picks con mayor probabilidad estadística.\n\n"
        "📊 *Manejo de Banca*\n"
        "Porcentaje recomendado a invertir por pick.\n\n"
        "🔗 *Combinadas Recomendadas*\n"
        "Picks agrupados optimizados por IA.\n\n"
        "⚽ *Marcadores más Probables*\n"
        "Proyección de goles por partido.\n\n"
        "🏆 *Picks Top 10*\n"
        "Los picks con mayor valor estadístico del día.\n\n"
        "🔥🟠 *Picks Inferno*\n"
        "Selección premium basada en análisis completo del día.\n\n"
        "🤖 *ProHeat Sport IA*\n"
        "Análisis bajo demanda de partidos usando datos deportivos y el motor ProHeat Sports.\n"
        f"Límite: {SPORT_IA_DAILY_LIMIT} consultas por usuario al día.\n\n"
        "📈 *RECOMENDACIÓN*\n"
        "Gestiona tu banca con disciplina.\n"
        "Evita sobreapostar y sigue la estrategia.\n"
    )

# =========================================================
# PARSEO DE PARTIDO
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
        team_name.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u"),
    ]

    for canonical, aliases in TEAM_ALIASES.items():
        if clean_name == canonical or clean_name in aliases:
            variants.extend([canonical] + aliases)

    dedup = []
    seen = set()
    for item in variants:
        item_clean = normalize_spaces(item)
        if item_clean and item_clean not in seen:
            seen.add(item_clean)
            dedup.append(item_clean)
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
# TEAMS / FIXTURES / H2H / INJURIES
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
        f"{api_name} {api_city}".strip(),
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
    league_country = team.get("country", "")

    return {
        "id": team["id"],
        "name": team.get("name", team_name),
        "country": league_country,
        "code": team.get("code", ""),
        "venue_city": venue.get("city", ""),
        "venue_name": venue.get("name", ""),
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
    injuries: List[str] = []
    season_candidates = [now_mx().year, now_mx().year - 1]

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
            "ga_avg": 0.0,
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
        "ga_avg": round(ga / played, 2) if played else 0.0,
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

def compute_h2h_summary(home_name: str, away_name: str, h2h: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not h2h:
        return {
            "count": 0,
            "home_wins": 0,
            "away_wins": 0,
            "draws": 0,
            "avg_goals": 0.0,
            "text": f"No hay H2H reciente confirmado entre {home_name} y {away_name}.",
        }

    home_wins = away_wins = draws = 0
    total_goals = 0

    for fx in h2h:
        home = fx["teams"]["home"]["name"]
        away = fx["teams"]["away"]["name"]
        hg = fx["goals"]["home"] if fx["goals"]["home"] is not None else 0
        ag = fx["goals"]["away"] if fx["goals"]["away"] is not None else 0
        total_goals += hg + ag

        if hg == ag:
            draws += 1
        else:
            # evaluar relativo al partido actual
            if limpiar_texto(home) == limpiar_texto(home_name):
                if hg > ag:
                    home_wins += 1
                else:
                    away_wins += 1
            elif limpiar_texto(away) == limpiar_texto(home_name):
                if ag > hg:
                    home_wins += 1
                else:
                    away_wins += 1
            else:
                # fallback si el naming no coincide perfecto
                if hg > ag:
                    home_wins += 1
                else:
                    away_wins += 1

    count = len(h2h)
    avg_goals = round(total_goals / count, 2) if count else 0.0
    text = (
        f"En los últimos {count} H2H, {home_name} ganó {home_wins}, "
        f"{away_name} ganó {away_wins} y hubo {draws} empates; "
        f"el promedio conjunto fue de {avg_goals} goles."
    )

    return {
        "count": count,
        "home_wins": home_wins,
        "away_wins": away_wins,
        "draws": draws,
        "avg_goals": avg_goals,
        "text": text,
    }

# =========================================================
# NOTICIAS
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
        f'"{team_name}" lesion suspension lesionado suspendido rotacion alineacion dt entrenador',
        f'"{team_name}" descanso viaje conferencia tecnico convocatoria',
    ]

    all_articles: List[Dict[str, Any]] = []
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
    ]

    all_articles: List[Dict[str, Any]] = []
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
            angles.append(f"En noticias recientes sobre {team_name} destaca que {snippet[0].lower() + snippet[1:] if len(snippet) > 1 else snippet.lower()}.")
    return angles

# =========================================================
# PROMPT PROHEAT
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
) -> str:
    h2h_lines = []
    for fx in h2h[:5]:
        try:
            home = fx["teams"]["home"]["name"]
            away = fx["teams"]["away"]["name"]
            hg = fx["goals"]["home"]
            ag = fx["goals"]["away"]
            h2h_lines.append(f"{home} {hg}-{ag} {away}")
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

Usa estas variables:
- forma reciente
- promedio de goles a favor y en contra
- H2H reciente
- descanso
- lesiones
- noticias recientes
- posibles rotaciones o declaraciones si aparecen
- clima
- prudencia si faltan datos

Datos deportivos:
{home_name}: PJ {home_form['played']}, G {home_form['wins']}, E {home_form['draws']}, P {home_form['losses']}, GF {home_form['gf_avg']}, GC {home_form['ga_avg']}
{away_name}: PJ {away_form['played']}, G {away_form['wins']}, E {away_form['draws']}, P {away_form['losses']}, GF {away_form['gf_avg']}, GC {away_form['ga_avg']}

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
  "goles": {{"local": "texto", "visitante": "texto"}},
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
- No agregues texto fuera del JSON.
""".strip()

# =========================================================
# PARSEO JSON RESPUESTA OPENAI
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
# CONSTRUCCIÓN DE 8 LÍNEAS NO GENÉRICAS
# =========================================================

def line_quality_ok(line: str) -> bool:
    if not line:
        return False
    s = normalize_spaces(line)
    if len(s) < 35:
        return False

    banned_fragments = [
        "debe leerse con cautela",
        "partido interesante",
        "puede pasar cualquier cosa",
        "habrá que ver",
        "todo puede suceder",
        "es un duelo parejo",
        "sin duda",
        "sin lugar a dudas",
        "será clave",
    ]
    s_low = limpiar_texto(s)
    return not any(b in s_low for b in banned_fragments)

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

    lines = []

    if home_form["played"] and away_form["played"]:
        lines.append(
            f"{home} llega con balance de {home_form['wins']}-{home_form['draws']}-{home_form['losses']} y {home_form['gf_avg']} goles a favor por juego, mientras {away} trae {away_form['wins']}-{away_form['draws']}-{away_form['losses']} y {away_form['gf_avg']} de media ofensiva."
        )

        lines.append(
            f"En defensa, {home} ha permitido {home_form['ga_avg']} goles por partido en su muestra reciente y {away} concede {away_form['ga_avg']}, dato que ayuda a medir si el cruce apunta a un marcador corto o abierto."
        )

    if h2h_summary["count"] > 0:
        lines.append(h2h_summary["text"])
    else:
        lines.append(f"No hay una muestra reciente de enfrentamientos directos entre {home} y {away}, así que el peso del análisis recae más en forma, bajas y contexto actual.")

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

    lines.append(
        f"El cruce no se resume a un dato aislado: forma, descanso, bajas y noticias recientes dibujan una previa más precisa para {home} vs {away}."
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

    # Si aun faltan líneas, completar con plantillas específicas del contexto, no genéricas.
    if len(final_lines) < 8:
        home = ctx["home_name"]
        away = ctx["away_name"]
        extra_templates = [
            f"La comparación reciente entre producción ofensiva de {home} y resistencia defensiva de {away} marca uno de los puntos más sensibles de la previa.",
            f"También habrá que medir si {away} sostiene su plan fuera de casa o si {home} consigue inclinar el partido desde la localía y el primer tramo del juego.",
            f"Más que un pronóstico vacío, este partido pide leer detalle por detalle: forma corta, bajas activas, descanso y señales recientes del entorno competitivo.",
        ]
        for line in extra_templates:
            key = limpiar_texto(line)
            if key not in used:
                used.add(key)
                final_lines.append(cut_text(line, 190))
            if len(final_lines) == 8:
                break

    # Última protección: exactas 8
    final_lines = final_lines[:8]

    # Si por algún caso extremo siguieran faltando, replicar con matices específicos y no relleno idéntico
    if len(final_lines) < 8:
        home = ctx["home_name"]
        away = ctx["away_name"]
        while len(final_lines) < 8:
            idx = len(final_lines) + 1
            final_lines.append(
                cut_text(
                    f"Ángulo {idx}: la lectura de {home} vs {away} sigue apoyándose en datos concretos de forma, contexto inmediato y disponibilidad del plantel.",
                    190
                )
            )

    return "\n".join(final_lines)

# =========================================================
# FORMATEO SALIDA
# =========================================================

def format_sport_ia_picks(home_name: str, away_name: str, payload: Dict[str, Any]) -> str:
    doble = payload.get("doble_oportunidad", {}) or {}
    goles = payload.get("goles", {}) or {}
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
        f"⚽ L: {goles.get('local', '-')} | V: {goles.get('visitante', '-')}\n"
        f"🎯 SoT: {sot.get('linea', '-')} ({sot.get('probabilidad', '-')})\n"
        f"📐 Corners: {corners.get('linea', '-')} ({corners.get('probabilidad', '-')})\n"
        f"🟨 Tarjetas: {tarjetas.get('linea', '-')} ({tarjetas.get('probabilidad', '-')})"
    )

def format_sport_ia_blog(payload: Dict[str, Any]) -> str:
    return f"🧠 Nota ProHeat\n\n{payload.get('analisis', 'Sin análisis disponible.')}"

# =========================================================
# ANÁLISIS SPORT IA
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

            payload = cached
            return (
                format_sport_ia_picks(home_name, away_name, payload),
                format_sport_ia_blog(payload)
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

    home_news = collect_team_news(home_team["name"])
    away_news = collect_team_news(away_team["name"])
    match_news = collect_match_news(home_team["name"], away_team["name"])

    home_news_summary = summarize_articles_for_prompt(home_news)
    away_news_summary = summarize_articles_for_prompt(away_news)
    match_news_summary = summarize_articles_for_prompt(match_news)

    weather_summary = get_weather_context(
        home_team.get("venue_city", ""),
        home_team.get("country", "")
    )

    h2h_summary = compute_h2h_summary(home_team["name"], away_team["name"], h2h)

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
        payload.setdefault("goles", {"local": "-", "visitante": "-"})
        payload.setdefault("sot", {"linea": "-", "probabilidad": "-"})
        payload.setdefault("corners", {"linea": "-", "probabilidad": "-"})
        payload.setdefault("tarjetas", {"linea": "-", "probabilidad": "-"})
        payload["analisis"] = ensure_8_lines_blog(payload.get("analisis", ""), ctx_for_lines)

        save_cached_analysis(match_text, payload)

        picks_text = format_sport_ia_picks(home_team["name"], away_team["name"], payload)
        blog_text = format_sport_ia_blog(payload)
        return picks_text, blog_text

    except Exception as e:
        logger.exception("Error en OpenAI")
        return None, f"❌ Error generando el análisis:\n{str(e)}"

# =========================================================
# TELEGRAM HANDLERS
# =========================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user_id = update.message.from_user.id

    if not is_allowed(user_id):
        await update.message.reply_text(
            "💎 PROHEAT SPORTS PREMIUM\n\n"
            "Acceso mensual: $100 MXN\n\n"
            "Envía comprobante\n\n"
            "Usa /myid"
        )
        return

    context.user_data["sport_ia_mode"] = False

    await update.message.reply_text(
        "📊 PROHEAT SPORTS\nSelecciona una opción:",
        reply_markup=reply_markup
    )

async def my_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    await update.message.reply_text(f"🆔 {update.message.from_user.id}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user_id = update.message.from_user.id
    raw_text = update.message.text or ""
    msg = raw_text.lower().strip()

    if not is_allowed(user_id):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Aprobar", callback_data=f"approve_{user_id}")]
        ])

        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"📩 Solicitud de acceso\nID: {user_id}",
                reply_markup=keyboard
            )
        except Exception:
            logger.exception("No se pudo avisar al admin")

        await update.message.reply_text("📩 Espera aprobación.")
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
            f"Consultas disponibles hoy: {restantes}"
        )
        context.user_data["sport_ia_mode"] = True
        return

    if "inferno" in msg:
        await update.message.reply_text(read_sheet("Hoja7"))
        return

    if "partidos" in msg:
        await update.message.reply_text(read_sheet("Hoja1"))
        return

    if "picks" in msg:
        await update.message.reply_text(
            "📊 Selecciona tipo de picks:",
            reply_markup=picks_menu
        )
        return

async def handle_picks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    await query.message.reply_text(read_sheet(query.data))

async def approve_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    await query.answer()

    try:
        user_id = query.data.split("_", 1)[1]
    except Exception:
        await query.edit_message_text("No pude interpretar el ID del usuario.")
        return

    users = load_users()
    expiration = now_mx() + timedelta(days=30)

    users[user_id] = {"expires": expiration.strftime("%Y-%m-%d")}
    save_users(users)

    try:
        await context.bot.send_message(chat_id=user_id, text="✅ Acceso aprobado")
        await context.bot.send_message(chat_id=user_id, text="Selecciona opción:", reply_markup=reply_markup)
    except Exception:
        logger.exception("No se pudo notificar al usuario aprobado")

    await query.edit_message_text(f"Usuario {user_id} aprobado.")

# =========================================================
# VALIDACIONES DE ARRANQUE
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
    logger.info("Excel file: %s", EXCEL_FILE)
    logger.info("Modelo OpenAI: %s", OPENAI_MODEL)
    logger.info("Sport IA daily limit: %s", SPORT_IA_DAILY_LIMIT)

# =========================================================
# MAIN
# =========================================================

def main():
    validate_environment()

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("myid", my_id))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_picks, pattern="^Hoja[1-7]$"))
    app.add_handler(CallbackQueryHandler(approve_user, pattern=r"^approve_\d+$"))

    logger.info("ProHeat Bot iniciado...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()