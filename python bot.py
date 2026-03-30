import pandas as pd
import json
import os
import re
import requests
import unicodedata
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, MessageHandler, filters, CommandHandler, ContextTypes, CallbackQueryHandler
from openai import OpenAI

# =========================
# CONFIG
# =========================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = 7696799656
USERS_FILE = "usuarios.json"

excel_file = "data.xlsx"

# =========================
# SPORT IA CONFIG
# =========================

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = "gpt-4o-mini"

API_FOOTBALL_KEY = "e2b935940a4ec9d65fdc75f57764da03"
API_FOOTBALL_BASE = "https://v3.football.api-sports.io"

GNEWS_API_KEY = "101363726f88bf5ae5bded0275a33db8"
GNEWS_BASE = "https://gnews.io/api/v4"

OPEN_METEO_GEOCODING_BASE = "https://geocoding-api.open-meteo.com/v1/search"
OPEN_METEO_FORECAST_BASE = "https://api.open-meteo.com/v1/forecast"

SPORT_IA_USAGE_FILE = "sport_ia_usage.json"
SPORT_IA_CACHE_FILE = "sport_ia_cache.json"
SPORT_IA_DAILY_LIMIT = 10
MX_TZ = ZoneInfo("America/Mexico_City")

NEWS_LOOKBACK_DAYS = 5
MAX_NEWS_ITEMS = 6
TEAM_MATCH_THRESHOLD = 58

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY and "PON_AQUI" not in OPENAI_API_KEY else None

# =========================
# MENU PRINCIPAL
# =========================

menu = [
    ["📘 Guía de Uso"],
    ["🤖 ProHeat Sport IA"],
    ["Partidos del Día"],
    ["📊 Picks"],
    ["🔥 PICKS INFERNO 🔥"]
]

reply_markup = ReplyKeyboardMarkup(menu, resize_keyboard=True)

# =========================
# SUBMENU PICKS
# =========================

picks_menu = InlineKeyboardMarkup([
    [InlineKeyboardButton("Pronósticos Premium", callback_data="Hoja2")],
    [InlineKeyboardButton("Manejo de Banca", callback_data="Hoja3")],
    [InlineKeyboardButton("Combinadas Recomendadas", callback_data="Hoja4")],
    [InlineKeyboardButton("Marcadores Probables", callback_data="Hoja5")],
    [InlineKeyboardButton("Picks Top 10", callback_data="Hoja6")]
])

# =========================
# TITULOS
# =========================

sheet_titles = {
    "Hoja1": "🔥 PARTIDOS DEL DÍA 🔥",
    "Hoja2": "🔥 PRONÓSTICOS PREMIUM 🔥",
    "Hoja3": "🔥 MANEJO DE BANCA 🔥",
    "Hoja4": "🔥 COMBINADAS RECOMENDADAS 🔥",
    "Hoja5": "🔥 MARCADORES MÁS PROBABLES 🔥",
    "Hoja6": "🔥 PICKS TOP 10 🔥",
    "Hoja7": "🔥 PICKS INFERNO 🔥"
}

# =========================
# JSON HELPERS
# =========================

def load_json_file(path, default_value):
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(default_value, f, indent=4, ensure_ascii=False)
        return default_value
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default_value

def save_json_file(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

# =========================
# USUARIOS
# =========================

def load_users():
    return load_json_file(USERS_FILE, {})

def save_users(data):
    save_json_file(USERS_FILE, data)

def is_allowed(user_id):
    users = load_users()
    return user_id == ADMIN_ID or str(user_id) in users

# =========================
# SPORT IA USAGE
# =========================

def today_mx():
    return datetime.now(MX_TZ).strftime("%Y-%m-%d")

def load_usage():
    return load_json_file(SPORT_IA_USAGE_FILE, {})

def save_usage(data):
    save_json_file(SPORT_IA_USAGE_FILE, data)

def get_user_usage_today(user_id):
    usage = load_usage()
    today = today_mx()
    return usage.get(str(user_id), {}).get(today, 0)

def increment_user_usage(user_id):
    usage = load_usage()
    today = today_mx()
    user_key = str(user_id)

    if user_key not in usage:
        usage[user_key] = {}

    usage[user_key][today] = usage[user_key].get(today, 0) + 1
    save_usage(usage)

def remaining_queries_today(user_id):
    used = get_user_usage_today(user_id)
    return max(SPORT_IA_DAILY_LIMIT - used, 0)

# =========================
# SPORT IA CACHE
# =========================

def load_cache():
    return load_json_file(SPORT_IA_CACHE_FILE, {})

def save_cache(data):
    save_json_file(SPORT_IA_CACHE_FILE, data)

def normalize_cache_key(text):
    return limpiar_texto(text).strip()

def get_cached_analysis(match_text):
    cache = load_cache()
    key = normalize_cache_key(match_text)
    today = today_mx()
    if key in cache and cache[key].get("date") == today:
        return cache[key].get("result")
    return None

def save_cached_analysis(match_text, result):
    cache = load_cache()
    key = normalize_cache_key(match_text)
    cache[key] = {
        "date": today_mx(),
        "result": result
    }
    save_cache(cache)

# =========================
# START
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

# =========================
# MY ID
# =========================

async def my_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🆔 {update.message.from_user.id}")

# =========================
# GUIA DE USO
# =========================

def guia_texto():
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

# =========================
# LECTURA DE EXCEL
# =========================

def read_sheet(sheet_name):
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        if df.empty:
            return "⚠️ Sin datos"

        title = sheet_titles.get(sheet_name, sheet_name)
        text = f"{title}\n\n"

        for _, row in df.iterrows():
            valores = [str(x) for x in row if pd.notna(x)]

            if len(valores) == 0:
                continue

            if sheet_name in ["Hoja1", "Hoja2"]:
                if len(valores) < 3:
                    continue

                hora = valores[0][:5]
                liga = valores[1]
                partido = valores[2]

                text += f"⚽ {liga}\n⏰ {hora}\n🏟️ {partido}\n\n"

                if len(valores) > 3:
                    text += f"📊 {valores[3]}\n"

                if len(valores) > 5:
                    text += f"⚽ L: {valores[4]} | V: {valores[5]}\n"

                if len(valores) > 6:
                    text += f"🎯 SoT: {valores[6]}\n"

                if len(valores) > 7:
                    text += f"📐 Corners: {valores[7]}\n"

                if len(valores) > 8:
                    text += f"🟨 Tarjetas: {valores[8]}\n"

                text += "━━━━━━━━━━━━━━━\n\n"
            else:
                text += "• " + " | ".join(valores) + "\n\n"

        return text

    except Exception as e:
        return f"❌ Error:\n{str(e)}"

# =========================
# SPORT IA HELPERS
# =========================

def limpiar_texto(texto):
    texto = str(texto).lower().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    texto = re.sub(r"[^a-z0-9\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto

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
    "chelsea": ["chelsea"]
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
    "juve": "juventus"
}

TEAM_STOPWORDS = {"de", "del", "la", "el", "los", "las", "fc", "cf", "sc", "club", "ac", "as"}

RUMOR_TERMS = [
    "rumor", "rumour", "gossip", "transfer", "fichaje", "mercado",
    "player ratings", "fantasy", "odds", "betting", "highlights", "recap"
]

def expand_team_variants(team_name):
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
        item_clean = item.strip()
        if item_clean and item_clean not in seen:
            seen.add(item_clean)
            dedup.append(item_clean)
    return dedup

def compact_team_query(name):
    txt = limpiar_texto(name)
    for old, new in TEAM_SEARCH_REPLACEMENTS.items():
        txt = txt.replace(old, new)
    return txt.strip()

def remove_team_stopwords(text):
    tokens = limpiar_texto(text).split()
    tokens = [t for t in tokens if t not in TEAM_STOPWORDS]
    return " ".join(tokens).strip()

def generate_search_queries(team_name):
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

    return queries

def api_football_get(endpoint, params=None):
    if not API_FOOTBALL_KEY or "PON_AQUI" in API_FOOTBALL_KEY:
        return None

    headers = {"x-apisports-key": API_FOOTBALL_KEY}

    try:
        response = requests.get(
            f"{API_FOOTBALL_BASE}/{endpoint}",
            headers=headers,
            params=params or {},
            timeout=20
        )
        response.raise_for_status()
        return response.json()
    except Exception:
        return None

def gnews_get(endpoint, params=None):
    if not GNEWS_API_KEY or "PON_AQUI" in GNEWS_API_KEY:
        return None

    params = params or {}
    params["apikey"] = GNEWS_API_KEY

    try:
        response = requests.get(
            f"{GNEWS_BASE}/{endpoint}",
            params=params,
            timeout=20
        )
        response.raise_for_status()
        return response.json()
    except Exception:
        return None

def parse_match_input(text):
    parts = re.split(r"\s+vs\s+|\s+v\s+|\s*-\s*", text.strip(), maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return None, None
    return parts[0].strip(), parts[1].strip()

def score_team_candidate(team_name, item):
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
            score = 0
            if cand == variant:
                score = 100
            elif cand in variant or variant in cand:
                score = 92
            else:
                common = set(cand.split()) & set(variant.split())
                score = max(score, 40 + 15 * len(common))
            if score > best:
                best = score

    return best

def search_team(team_name):
    queries = generate_search_queries(team_name)
    candidates_by_id = {}

    for query in queries:
        data = api_football_get("teams", {"search": query})
        if data and "response" in data and data["response"]:
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
        "name": team["name"],
        "country": team.get("country", ""),
        "venue_city": venue.get("city", ""),
        "venue_name": venue.get("name", "")
    }

def get_recent_fixtures(team_id, last_n=5):
    data = api_football_get("fixtures", {"team": team_id, "last": last_n})
    if not data or "response" not in data:
        return []
    return data["response"]

def get_h2h(home_id, away_id, last_n=5):
    data = api_football_get("fixtures/headtohead", {"h2h": f"{home_id}-{away_id}", "last": last_n})
    if not data or "response" not in data:
        return []
    return data["response"]

def get_team_injuries(team_id):
    season_candidates = [datetime.now(MX_TZ).year, datetime.now(MX_TZ).year - 1]
    injuries = []

    for season in season_candidates:
        data = api_football_get("injuries", {"team": team_id, "season": season})
        if data and "response" in data and data["response"]:
            for item in data["response"][:8]:
                player = item.get("player", {}).get("name", "")
                reason = item.get("player", {}).get("reason", "") or item.get("player", {}).get("type", "")
                if player:
                    injuries.append(f"{player} ({reason})" if reason else player)
            if injuries:
                break

    clean = []
    seen = set()
    for x in injuries:
        k = limpiar_texto(x)
        if k not in seen:
            seen.add(k)
            clean.append(x)
    return clean[:5]

def summarize_team_form(team_name, fixtures):
    if not fixtures:
        return {
            "played": 0,
            "wins": 0,
            "draws": 0,
            "losses": 0,
            "gf_avg": 0,
            "ga_avg": 0
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
        "gf_avg": round(gf / played, 2) if played else 0,
        "ga_avg": round(ga / played, 2) if played else 0
    }

def get_days_since_last_match(fixtures):
    if not fixtures:
        return None

    try:
        latest = fixtures[0]["fixture"]["date"]
        dt = datetime.fromisoformat(latest.replace("Z", "+00:00"))
        now = datetime.now(dt.tzinfo)
        return (now - dt).days
    except Exception:
        return None

def get_last_fixture_context(team_name, fixtures):
    if not fixtures:
        return "Sin datos recientes"

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

        return f"Último juego como {condicion} ante {rival}, marcador {marcador}, hace {days if days is not None else 'N/D'} días"
    except Exception:
        return "Sin contexto claro del último partido"

def geocode_city(city_query):
    try:
        response = requests.get(
            OPEN_METEO_GEOCODING_BASE,
            params={"name": city_query, "count": 1, "language": "es", "format": "json"},
            timeout=20
        )
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        return results[0] if results else None
    except Exception:
        return None

def get_weather_context(city, country=""):
    if not city:
        return "Sin clima confirmado"

    query = f"{city}, {country}" if country else city
    geo = geocode_city(query)
    if not geo:
        return "Sin clima confirmado"

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
            timeout=20
        )
        response.raise_for_status()
        data = response.json()
        daily = data.get("daily", {})

        tmax = daily.get("temperature_2m_max", ["N/D"])[0]
        tmin = daily.get("temperature_2m_min", ["N/D"])[0]
        rain = daily.get("precipitation_probability_max", ["N/D"])[0]
        wind = daily.get("windspeed_10m_max", ["N/D"])[0]

        return f"{city}: máx {tmax}°C, mín {tmin}°C, lluvia {rain}%, viento {wind} km/h"
    except Exception:
        return "Sin clima confirmado"

def news_is_noise(title, description):
    text = limpiar_texto(f"{title} {description}")
    return any(term in text for term in RUMOR_TERMS)

def dedupe_articles(articles):
    out = []
    seen = set()

    for art in articles:
        key = limpiar_texto(art.get("title", ""))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(art)

    return out

def gnews_search_recent(query, days=NEWS_LOOKBACK_DAYS, max_articles=10, lang="es"):
    from_date = (datetime.now(MX_TZ) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    data = gnews_get(
        "search",
        {
            "q": query,
            "lang": lang,
            "max": max_articles,
            "from": from_date,
            "sortby": "publishedAt"
        }
    )
    if not data or "articles" not in data:
        return []
    return data["articles"]

def collect_team_news(team_name):
    queries = [
        f'"{team_name}" lesion suspension lesionado suspendido rotacion alineacion dt entrenador',
        f'"{team_name}" descanso viaje conferencia tecnico convocatoria'
    ]

    all_articles = []
    for q in queries:
        all_articles.extend(gnews_search_recent(q, lang="es"))
        all_articles.extend(gnews_search_recent(q, lang="en"))

    all_articles = dedupe_articles(all_articles)

    filtered = []
    team_clean = limpiar_texto(team_name)
    for art in all_articles:
        title = art.get("title", "")
        desc = art.get("description", "")

        if news_is_noise(title, desc):
            continue

        combined = limpiar_texto(f"{title} {desc}")
        if team_clean not in combined and not any(alias in combined for alias in expand_team_variants(team_name)):
            continue

        filtered.append(art)

    return filtered[:MAX_NEWS_ITEMS]

def collect_match_news(home_name, away_name):
    queries = [
        f'"{home_name}" "{away_name}" lesion suspension lineup preview coach',
        f'"{home_name}" "{away_name}" rotation travel weather'
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

def summarize_articles_for_prompt(articles):
    if not articles:
        return "Sin noticias relevantes recientes"

    lines = []
    for art in articles[:4]:
        title = art.get("title", "").strip()
        desc = art.get("description", "").strip()
        summary = title
        if desc:
            summary += f" — {desc}"
        lines.append(f"- {summary}")

    return "\n".join(lines)

def build_proheat_prompt(
    home_name, away_name,
    home_form, away_form,
    h2h, home_rest_days, away_rest_days,
    home_last_context, away_last_context,
    home_injuries, away_injuries,
    home_news_summary, away_news_summary, match_news_summary,
    weather_summary
):
    h2h_lines = []
    for fx in h2h:
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

Analiza el partido {home_name} vs {away_name} con enfoque prudente y estilo ProHeat Sports.
Usa:
- forma reciente
- promedio de goles a favor y en contra
- H2H reciente
- días de descanso
- viajes inferidos
- lesiones
- noticias recientes del local y visitante
- declaraciones del DT o señales de rotación si aparecen en noticias
- clima
- criterio conservador si faltan datos

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

H2H reciente:
{h2h_text}

Noticias local:
{home_news_summary}

Noticias visitante:
{away_news_summary}

Noticias del partido:
{match_news_summary}

Clima:
{weather_summary}

Devuelve solo JSON válido con esta estructura exacta:
{{
  "pick_principal": "texto",
  "doble_oportunidad": {{"pick": "texto", "probabilidad": "texto"}},
  "goles": {{"local": "texto", "visitante": "texto"}},
  "sot": {{"linea": "texto", "probabilidad": "texto"}},
  "corners": {{"linea": "texto", "probabilidad": "texto"}},
  "tarjetas": {{"linea": "texto", "probabilidad": "texto"}},
  "analisis": "nota tipo blog de exactamente 8 renglones, natural, clara, fácil de leer, sin viñetas"
}}

Reglas:
- El pick principal debe verse estilo ProHeat.
- Doble oportunidad debe ser una opción realista.
- Los goles local y visitante deben ser estimaciones numéricas breves.
- SoT, corners y tarjetas deben incluir línea o estimación y probabilidad.
- El campo 'analisis' debe tener exactamente 8 renglones.
- Cada renglón debe ser breve, natural y fácil de leer.
- Debe sentirse como una mini previa de blog profesional.
- No uses viñetas ni numeración.
- No dejes el análisis genérico.
- Usa lesiones, forma, H2H, descanso, noticias y clima para redactar.
- Si faltan datos, sé prudente pero sigue escribiendo una previa útil.
- No agregues texto fuera del JSON.
""".strip()

def parse_json_response(text):
    try:
        return json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        raise

def ensure_8_lines_blog(text):
    if not text:
        return "Sin análisis disponible."

    lines = [x.strip() for x in str(text).splitlines() if x.strip()]

    if len(lines) == 0:
        return "Sin análisis disponible."

    if len(lines) > 8:
        lines = lines[:8]

    while len(lines) < 8:
        lines.append("El contexto del partido sugiere un duelo que debe leerse con cautela.")

    return "\n".join(lines)

def format_sport_ia_picks(home_name, away_name, payload):
    return (
        "🤖 PROHEAT SPORT IA\n\n"
        "⚽ Partido\n"
        "⏰ Por confirmar\n"
        f"🏟️ {home_name} vs {away_name}\n\n"
        f"📊 {payload.get('pick_principal', '-')}\n"
        f"🔒 Doble oportunidad: {payload.get('doble_oportunidad', {}).get('pick', '-')} "
        f"({payload.get('doble_oportunidad', {}).get('probabilidad', '-')})\n"
        f"⚽ L: {payload.get('goles', {}).get('local', '-')} | V: {payload.get('goles', {}).get('visitante', '-')}\n"
        f"🎯 SoT: {payload.get('sot', {}).get('linea', '-')} ({payload.get('sot', {}).get('probabilidad', '-')})\n"
        f"📐 Corners: {payload.get('corners', {}).get('linea', '-')} ({payload.get('corners', {}).get('probabilidad', '-')})\n"
        f"🟨 Tarjetas: {payload.get('tarjetas', {}).get('linea', '-')} ({payload.get('tarjetas', {}).get('probabilidad', '-')})"
    )

def format_sport_ia_blog(payload):
    blog = ensure_8_lines_blog(payload.get("analisis", ""))
    return f"🧠 Nota ProHeat\n\n{blog}"

def run_sport_ia_analysis(match_text):
    if not openai_client:
        return None, "❌ Falta configurar OPENAI_API_KEY."
    if not API_FOOTBALL_KEY or "PON_AQUI" in API_FOOTBALL_KEY:
        return None, "❌ Falta configurar API_FOOTBALL_KEY."

    cached = get_cached_analysis(match_text)
    if cached:
        try:
            home_name, away_name = parse_match_input(match_text)
            payload = parse_json_response(cached)
            return format_sport_ia_picks(home_name, away_name, payload), format_sport_ia_blog(payload)
        except Exception:
            pass

    home_name, away_name = parse_match_input(match_text)
    if not home_name or not away_name:
        return None, "⚠️ Escribe el partido así:\nLiverpool vs Galatasaray"

    home_team = search_team(home_name)
    away_team = search_team(away_name)

    if not home_team or not away_team:
        return None, "❌ No pude identificar uno o ambos equipos."

    if home_team["id"] == away_team["id"]:
        return None, "❌ Detecté ambos equipos como el mismo club. Escribe el partido con un nombre más específico."

    home_fixtures = get_recent_fixtures(home_team["id"], 5)
    away_fixtures = get_recent_fixtures(away_team["id"], 5)
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

    prompt = build_proheat_prompt(
        home_team["name"],
        away_team["name"],
        home_form,
        away_form,
        h2h,
        home_rest_days,
        away_rest_days,
        home_last_context,
        away_last_context,
        home_injuries,
        away_injuries,
        home_news_summary,
        away_news_summary,
        match_news_summary,
        weather_summary
    )

    try:
        response = openai_client.responses.create(
            model=OPENAI_MODEL,
            input=prompt
        )
        raw = response.output_text.strip()
        payload = parse_json_response(raw)
        payload["analisis"] = ensure_8_lines_blog(payload.get("analisis", ""))

        save_cached_analysis(match_text, json.dumps(payload, ensure_ascii=False))

        picks_text = format_sport_ia_picks(home_team["name"], away_team["name"], payload)
        blog_text = format_sport_ia_blog(payload)
        return picks_text, blog_text

    except Exception as e:
        return None, f"❌ Error con OpenAI:\n{str(e)}"

# =========================
# MENSAJES
# =========================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    if not is_allowed(user_id):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Aprobar", callback_data=f"approve_{user_id}")]
        ])

        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"📩 Solicitud de acceso\nID: {user_id}",
            reply_markup=keyboard
        )

        await update.message.reply_text("📩 Espera aprobación.")
        return

    msg = update.message.text.lower().strip()

    if context.user_data.get("sport_ia_mode"):
        context.user_data["sport_ia_mode"] = False

        remaining = remaining_queries_today(user_id)
        if remaining <= 0:
            await update.message.reply_text("⚠️ Ya usaste tus 10 consultas de ProHeat Sport IA hoy.")
            return

        await update.message.reply_text("⏳ Analizando partido con ProHeat Sport IA...")
        picks_text, blog_text = run_sport_ia_analysis(update.message.text)

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

    elif "proheat sport ia" in msg or "sport ia" in msg:
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

    elif "inferno" in msg:
        await update.message.reply_text(read_sheet("Hoja7"))

    elif "partidos" in msg:
        await update.message.reply_text(read_sheet("Hoja1"))

    elif "picks" in msg:
        await update.message.reply_text(
            "📊 Selecciona tipo de picks:",
            reply_markup=picks_menu
        )

# =========================
# CALLBACK PICKS
# =========================

async def handle_picks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(read_sheet(query.data))

# =========================
# APROBAR
# =========================

async def approve_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.data.split("_")[1]

    users = load_users()
    expiration = datetime.now() + timedelta(days=30)

    users[user_id] = {"expires": expiration.strftime("%Y-%m-%d")}
    save_users(users)

    await context.bot.send_message(chat_id=user_id, text="✅ Acceso aprobado")
    await context.bot.send_message(chat_id=user_id, text="Selecciona opción:", reply_markup=reply_markup)

    await query.edit_message_text(f"Usuario {user_id} aprobado.")

# =========================
# BOT
# =========================

app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("myid", my_id))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
app.add_handler(CallbackQueryHandler(handle_picks, pattern="Hoja"))
app.add_handler(CallbackQueryHandler(approve_user, pattern="approve_"))

print("ProHeat Bot iniciado...")

app.run_polling()