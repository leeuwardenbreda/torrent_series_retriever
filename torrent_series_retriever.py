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

# Logging
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)

# ================= Helpers =================
def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", name)

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
        # Kodi slaat imdb_id op in episodeguide JSON in tvshow tabel
        cur.execute("SELECT idShow, c10 AS episodeguide, c00 AS title FROM tvshow")
        for row in cur.fetchall():
            ep_guide_raw = row["episodeguide"]
            if not ep_guide_raw:
                continue
            ep_guide_clean = re.sub(r'^<.*?>|</.*?>$', '', ep_guide_raw).strip()
            try:
                ep_guide = json.loads(ep_guide_clean)
                if ep_guide.get("imdb") == imdb_id:
                    return row["idShow"]
            except json.JSONDecodeError:
                continue
        cur.close()
        conn.close()
    except Exception as e:
        logging.warning("Fout bij ophalen Kodi ID voor IMDb %s: %s", imdb_id, e)
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
    episodes = {(int(r["season"]), int(r["episode"])) for r in cur.fetchall()}
    cur.close()
    conn.close()
    return episodes

# ================= qBittorrent =================
def qbittorrent_login(session: requests.Session):
    r = session.post(
        f"{QBITTORRENT_URL}/api/v2/auth/login",
        data={"username": QBITTORRENT_USERNAME, "password": QBITTORRENT_PASSWORD},
        timeout=10
    )
    if r.text != "Ok.":
        raise RuntimeError("qBittorrent login mislukt")

def add_magnet_to_qbittorrent(magnet, category):
    session = requests.Session()
    qbittorrent_login(session)
    r = session.post(
        f"{QBITTORRENT_URL}/api/v2/torrents/add",
        data={"urls": magnet, "paused": "true", "category": category},
        timeout=10
    )
    if r.status_code != 200:
        logging.error("qBittorrent fout: %s %s", r.status_code, r.text)
    else:
        logging.info("Magnet toegevoegd: %s", magnet)

# ================= TPB =================
def search_tpb(query):
    logging.info("TPB zoeken: %s", query)
    try:
        r = requests.get(f"https://apibay.org/q.php?q={quote(query)}&cat=0", timeout=20)
        time.sleep(0.5)
        if r.status_code == 429:
            logging.warning("TPB rate-limit, opnieuw proberen")
            time.sleep(10)
            return search_tpb(query)
        if not r.text.startswith("["):
            return []
        return json.loads(r.text)
    except Exception as e:
        logging.error("TPB zoekfout: %s", e)
        return []

# ================= Media =================
def get_series_episodes(imdb_id, seasons_filter=None):
    if not imdb_id:
        return set()
    logging.info("IMDb afleveringen ophalen: %s", imdb_id)
    episodes = set()
    page_token = None
    headers = {"Accept": "application/json", "User-Agent": "MediaRetriever/1.0"}
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    try:
        while True:
            params = {}
            if page_token:
                params["pageToken"] = page_token
            r = requests.get(f"{IMDB_API_BASE}/titles/{imdb_id}/episodes",
                             headers=headers, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            for ep in data.get("episodes", []):
                season = ep.get("season")
                episode = ep.get("episodeNumber")
                rd = ep.get("releaseDate")
                if season is None or episode is None:
                    continue
                if seasons_filter and int(season) not in seasons_filter:
                    continue
                if rd:
                    try:
                        release_date = datetime.date(rd.get("year"), rd.get("month"), rd.get("day"))
                        if release_date <= yesterday:
                            episodes.add((int(season), int(episode)))
                    except Exception:
                        pass
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        logging.info("IMDb afleveringen gevonden: %d", len(episodes))
        return episodes
    except Exception as e:
        logging.error("IMDb API fout (%s): %s", imdb_id, e)
        return set()

def process_series(entry):
    title = entry["title"]
    imdb_id = entry.get("imdb_id")
    seasons = entry.get("seasons")

    # Haal kodi_id automatisch op indien MySQL aan staat en kodi_id niet in config
    kodi_id = entry.get("kodi_id")
    if USE_MYSQL and not kodi_id:
        kodi_id = get_kodi_id_from_imdb(imdb_id)
        entry["kodi_id"] = kodi_id

    kodi_episodes = get_existing_episodes(kodi_id)
    all_eps = get_series_episodes(imdb_id, seasons)
    to_download = all_eps - kodi_episodes
    qualities = ["2160p HDR","2160p","h265","1080p BluRay","1080p WEB-DL","1080p"]

    logging.info("=== Serie controleren: %s ===", title)
    logging.info("Kodi: %d | IMDb: %d | Te downloaden: %d",
                 len(kodi_episodes), len(all_eps), len(to_download))

    for season, episode in to_download:
        found = False
        for q in qualities:
            query = f"{title} s{season:02}e{episode:02} {q}"
            for r in search_tpb(query):
                name = r.get("name","")
                if name == "No results returned":
                    continue
                if re.search(r"S\d{2}E\d{2}", name, re.I) and re.search(re.escape(title), name, re.I):
                    magnet = f"magnet:?xt=urn:btih:{r['info_hash']}&dn={quote(name)}"
                    add_magnet_to_qbittorrent(magnet, sanitize_filename(title))
                    logging.info("Toegevoegd: %s S%02dE%02d → %s", title, season, episode, name)
                    found = True
                    break
            if found: break
        if not found:
            logging.info("Niet gevonden: %s S%02dE%02d", title, season, episode)

def process_film(entry):
    title = entry["title"]
    year = entry.get("year")
    qualities = ["2160p HDR","2160p","h265","1080p BluRay","1080p WEB-DL","1080p"]
    found = False

    logging.info("=== Film controleren: %s ===", title)

    for q in qualities:
        query_parts = [title]
        if year:
            query_parts.append(str(year))
        query_parts.append(q)
        query = " ".join(query_parts)
        for r in search_tpb(query):
            name = r.get("name","")
            if name == "No results returned":
                continue
            magnet = f"magnet:?xt=urn:btih:{r['info_hash']}&dn={quote(name)}"
            add_magnet_to_qbittorrent(magnet, sanitize_filename(title))
            logging.info("Film toegevoegd: %s → %s", title, name)
            found = True
            break
        if found: break
    if not found:
        logging.info("Film niet gevonden: %s", title)

# ================= Main =================
def run_all_searches():
    if not CONFIG_FILE.exists():
        logging.error("Config bestand niet gevonden: %s", CONFIG_FILE)
        return
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
    for s in config.get("series", []):
        try:
            process_series(s)
        except Exception as e:
            logging.error("Fout bij serie '%s': %s", s.get("title"), e)
    for f in config.get("films", []):
        try:
            process_film(f)
        except Exception as e:
            logging.error("Fout bij film '%s': %s", f.get("title"), e)

if __name__ == "__main__":
    logging.info("Media Retriever gestart")
    run_all_searches()
