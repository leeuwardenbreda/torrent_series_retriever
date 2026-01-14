import os
import json
import logging
import requests
import re
import datetime
import time
from urllib.parse import quote
from pathlib import Path
from dotenv import load_dotenv
import mysql.connector

# ================= ENV =================
load_dotenv()

USE_MYSQL = os.getenv("USE_MYSQL", "False").lower() == "true"
QBITTORRENT_URL = os.getenv("QBITTORRENT_URL")
QBITTORRENT_USERNAME = os.getenv("QBITTORRENT_USERNAME")
QBITTORRENT_PASSWORD = os.getenv("QBITTORRENT_PASSWORD")
IMDB_API_BASE = os.getenv("IMDB_API_BASE", "https://api.imdbapi.dev")

CONFIG_FILE = Path(os.getenv("MEDIA_CONFIG_FILE", "./media_config.json"))
LOG_FILE = Path(os.getenv("LOG_FILE", "./logs/media_retriever.log"))

SAVE_PATH_SERIES = os.getenv("SAVE_PATH_SERIES", "./series")
SAVE_PATH_MOVIES = os.getenv("SAVE_PATH_MOVIES", "./movies")

# ================= Logging =================
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)

# ================= Quality Definitions =================
VIDEO_QUALITIES = [
    "2160p", "4K", "UHD",
    "1080p", "FullHD", "FHD",
    "720p", "HD",
    "576p", "480p", "SD",
]

AUDIO_QUALITIES = [
    "Atmos", "Dolby Atmos", "DTS-X",
    "TrueHD", "DTS-HD MA", "DTS-HD",
    "7.1", "5.1",
    "DDP", "DD", "AAC",
    "2.0",
]

CODEC_QUALITIES = [
    "H265", "HEVC", "X265",
    "H264", "X264",
]

SOURCE_QUALITIES = [
    "BluRay", "WEB-DL", "WEBRip", "WEB", "HDTV",
]

def build_media_qualities():
    combos = []
    for v in VIDEO_QUALITIES:
        for a in AUDIO_QUALITIES:
            for c in CODEC_QUALITIES:
                combos.append(f"{v} {a} {c}")
            combos.append(f"{v} {a}")
        for c in CODEC_QUALITIES:
            combos.append(f"{v} {c}")
        combos.append(v)
    combos.extend(SOURCE_QUALITIES)
    combos.append("")
    return combos

MEDIA_QUALITIES = build_media_qualities()

# ================= Helpers =================
def pick_best_by_quality(candidates, qualities):
    for q in qualities:
        q_regex = "".join(f"(?=.*{re.escape(p)})" for p in q.split())
        matches = [
            r for r in candidates
            if re.search(q_regex, r.get("name", ""), re.I)
        ]
        if matches:
            return max(matches, key=lambda r: int(r.get("seeders", 0)))
    return max(candidates, key=lambda r: int(r.get("seeders", 0)))

def season_is_empty_in_kodi(kodi_eps, season):
    return not any(s == season for s, _ in kodi_eps)

def is_valid_season_pack(title, season, torrent_name, num_files, num_episodes):
    name = torrent_name.lower()
    title_re = re.escape(title.lower())

    if not (
        re.search(rf"{title_re}.*season\s*{season}\b", name)
        or re.search(rf"{title_re}.*s{season:02}\b", name)
    ):
        return False

    if re.search(r"s\d{2}e\d{2}", name):
        return False

    if num_files < num_episodes:
        return False

    return True

# ================= Config =================
def read_config_file(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ================= MySQL =================
def get_mysql_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        port=int(os.getenv("MYSQL_PORT", "3306"))
    )

def get_kodi_id_from_imdb(imdb_id):
    if not USE_MYSQL or not imdb_id:
        return None
    try:
        conn = get_mysql_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT idShow, c10 AS episodeguide FROM tvshow")
        for row in cur.fetchall():
            raw = row["episodeguide"]
            if not raw:
                continue
            clean = re.sub(r'^<.*?>|</.*?>$', '', raw).strip()
            try:
                if json.loads(clean).get("imdb") == imdb_id:
                    return row["idShow"]
            except json.JSONDecodeError:
                continue
        cur.close()
        conn.close()
    except Exception as e:
        logging.warning("Kodi ID lookup fout (%s): %s", imdb_id, e)
    return None

def get_existing_episodes(series_id):
    if series_id is None or not USE_MYSQL:
        return set()
    conn = get_mysql_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT c12 AS season, c13 AS episode FROM episode WHERE idShow = %s",
        (series_id,)
    )
    eps = {(int(r["season"]), int(r["episode"])) for r in cur.fetchall()}
    cur.close()
    conn.close()
    return eps

# ================= qBittorrent =================
def qbittorrent_login(session):
    r = session.post(
        f"{QBITTORRENT_URL}/api/v2/auth/login",
        data={"username": QBITTORRENT_USERNAME, "password": QBITTORRENT_PASSWORD}
    )
    if r.text != "Ok.":
        raise RuntimeError("qBittorrent login mislukt")

def add_magnet_to_qbittorrent(magnet, save_path):
    session = requests.Session()
    qbittorrent_login(session)
    r = session.post(
        f"{QBITTORRENT_URL}/api/v2/torrents/add",
        data={"urls": magnet, "paused": "false", "savepath": save_path}
    )
    if r.status_code == 200:
        logging.info("Magnet toegevoegd: %s → %s", magnet, save_path)

# ================= TPB =================
def search_tpb(query):
    try:
        r = requests.get(f"https://apibay.org/q.php?q={quote(query)}", timeout=20)
        time.sleep(0.5)
        if not r.text.startswith("["):
            return []
        return json.loads(r.text)
    except Exception:
        return []

# ================= IMDb =================
def get_series_episodes(imdb_id, seasons_filter=None):
    episodes = set()
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    page_token = None

    while True:
        params = {"pageToken": page_token} if page_token else {}
        r = requests.get(f"{IMDB_API_BASE}/titles/{imdb_id}/episodes", params=params)
        data = r.json()

        for ep in data.get("episodes", []):
            s = ep.get("season")
            e = ep.get("episodeNumber")
            rd = ep.get("releaseDate")

            if not (s and e and rd):
                continue

            s = int(s)
            if seasons_filter and s not in seasons_filter:
                continue

            year = rd.get("year")
            month = rd.get("month", 12)
            day = rd.get("day", 31)

            try:
                d = datetime.date(year, month, day)
            except Exception:
                continue

            if d <= yesterday:
                episodes.add((s, int(e)))

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return episodes

# ================= Processing =================
def process_series(entry):
    title = entry["title"]
    imdb_id = entry.get("imdb_id")

    raw_seasons = entry.get("seasons", [])
    seasons_filter = {int(s) for s in raw_seasons} if raw_seasons else None

    kodi_id = get_kodi_id_from_imdb(imdb_id) if USE_MYSQL else None
    kodi_eps = get_existing_episodes(kodi_id)

    imdb_eps = get_series_episodes(imdb_id, seasons_filter)
    todo = sorted(imdb_eps - kodi_eps)

    logging.info("=== Serie controleren: %s ===", title)
    logging.info(
        "Kodi afleveringen: %d | IMDb afleveringen: %d | Te downloaden: %d",
        len(kodi_eps), len(imdb_eps), len(todo)
    )

    # Season packs
    for season in sorted({s for s, _ in todo}):
        if not season_is_empty_in_kodi(kodi_eps, season):
            continue

        season_candidates = []
        for q in (f"{title} S{season:02}", f"{title} Season {season}"):
            for r in search_tpb(q):
                if is_valid_season_pack(
                    title,
                    season,
                    r.get("name", ""),
                    int(r.get("num_files", 1)),
                    len([e for s, e in imdb_eps if s == season])
                ):
                    season_candidates.append(r)

        if season_candidates:
            best = pick_best_by_quality(season_candidates, MEDIA_QUALITIES)
            magnet = f"magnet:?xt=urn:btih:{best['info_hash']}&dn={quote(best['name'])}"
            add_magnet_to_qbittorrent(magnet, SAVE_PATH_SERIES)
            logging.info(
                "Season-pack toegevoegd: %s Season %d → %s",
                title, season, best["name"]
            )
            todo = [t for t in todo if t[0] != season]

    # Episode fallback
    for season, episode in todo:
        candidates = []
        for r in search_tpb(f"{title} s{season:02}e{episode:02}"):
            if re.search(r"S\d{2}E\d{2}", r.get("name", ""), re.I):
                candidates.append(r)

        if not candidates:
            logging.info(
                "Niet gevonden (geen kandidaten): %s S%02dE%02d",
                title, season, episode
            )
            continue

        best = pick_best_by_quality(candidates, MEDIA_QUALITIES)
        magnet = f"magnet:?xt=urn:btih:{best['info_hash']}&dn={quote(best['name'])}"
        add_magnet_to_qbittorrent(magnet, SAVE_PATH_SERIES)
        logging.info(
            "Toegevoegd: %s S%02dE%02d → %s",
            title, season, episode, best["name"]
        )

def process_film(entry):
    title = entry["title"]
    imdb_id = entry.get("imdb_id")
    year = entry.get("year")

    if USE_MYSQL and get_kodi_id_from_imdb(imdb_id):
        logging.info("Film al aanwezig in Kodi: %s", title)
        return

    query = " ".join(filter(None, [title, str(year) if year else None]))
    candidates = search_tpb(query)

    if not candidates:
        logging.info("Film niet gevonden: %s", title)
        return

    best = pick_best_by_quality(candidates, MEDIA_QUALITIES)
    magnet = f"magnet:?xt=urn:btih:{best['info_hash']}&dn={quote(best['name'])}"
    add_magnet_to_qbittorrent(magnet, SAVE_PATH_MOVIES)
    logging.info("Film toegevoegd: %s → %s", title, best["name"])

# ================= Main =================
def run_all_searches():
    config = read_config_file(CONFIG_FILE)
    for s in config.get("series", []):
        process_series(s)
    for f in config.get("films", []):
        process_film(f)

if __name__ == "__main__":
    logging.info("Media Retriever gestart")
    run_all_searches()
