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

# Configurabele downloadmappen
SAVE_PATH_SERIES = os.getenv("SAVE_PATH_SERIES", "./series")
SAVE_PATH_MOVIES = os.getenv("SAVE_PATH_MOVIES", "./movies")

# ================= Logging =================
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)

# ================= Helpers =================
def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", name)

# ================= Config loader =================
def read_config_file(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(path)
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
                data = json.loads(clean)
                if data.get("imdb") == imdb_id:
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
    cur.execute("SELECT c12 AS season, c13 AS episode FROM episode WHERE idShow = %s", (series_id,))
    episodes = {(int(r["season"]), int(r["episode"])) for r in cur.fetchall()}
    cur.close()
    conn.close()
    return episodes

# ================= qBittorrent =================
def qbittorrent_login(session: requests.Session):
    r = session.post(f"{QBITTORRENT_URL}/api/v2/auth/login",
                     data={"username": QBITTORRENT_USERNAME, "password": QBITTORRENT_PASSWORD},
                     timeout=10)
    if r.text != "Ok.":
        raise RuntimeError("qBittorrent login mislukt")

def add_magnet_to_qbittorrent(magnet, save_path):
    session = requests.Session()
    qbittorrent_login(session)
    r = session.post(
        f"{QBITTORRENT_URL}/api/v2/torrents/add",
        data={
            "urls": magnet,
            "paused": "false",
            "savepath": save_path
        },
        timeout=10
    )
    if r.status_code == 200:
        logging.info("Magnet toegevoegd: %s → %s", magnet, save_path)
    else:
        logging.error("qBittorrent fout: %s %s", r.status_code, r.text)

# ================= TPB =================
def search_tpb(query):
    try:
        r = requests.get(f"https://apibay.org/q.php?q={quote(query)}&cat=0", timeout=20)
        time.sleep(0.5)
        if r.status_code == 429:
            time.sleep(10)
            return search_tpb(query)
        if not r.text.startswith("["):
            return []
        return json.loads(r.text)
    except Exception as e:
        logging.error("TPB zoekfout: %s", e)
        return []

# ================= IMDb =================
def get_series_episodes(imdb_id, seasons_filter=None):
    if not imdb_id:
        return set()
    episodes = set()
    page_token = None
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    try:
        while True:
            params = {}
            if page_token:
                params["pageToken"] = page_token
            r = requests.get(f"{IMDB_API_BASE}/titles/{imdb_id}/episodes",
                             headers={"Accept": "application/json"},
                             params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            for ep in data.get("episodes", []):
                s = ep.get("season")
                e = ep.get("episodeNumber")
                rd = ep.get("releaseDate")
                if s is None or e is None:
                    continue
                if seasons_filter and int(s) not in seasons_filter:
                    continue
                if rd:
                    try:
                        d = datetime.date(rd["year"], rd["month"], rd["day"])
                        if d <= yesterday:
                            episodes.add((int(s), int(e)))
                    except Exception:
                        pass
            page_token = data.get("nextPageToken")
            if not page_token:
                break
    except Exception as e:
        logging.error("IMDb fout (%s): %s", imdb_id, e)
    return episodes

# ================= Processing =================
def process_series(entry):
    title = entry["title"]
    imdb_id = entry.get("imdb_id")
    seasons = entry.get("seasons")

    kodi_id = entry.get("kodi_id")
    if USE_MYSQL and not kodi_id:
        kodi_id = get_kodi_id_from_imdb(imdb_id)
        entry["kodi_id"] = kodi_id

    kodi_eps = get_existing_episodes(kodi_id)
    imdb_eps = get_series_episodes(imdb_id, seasons)
    todo = imdb_eps - kodi_eps

    logging.info("=== Serie controleren: %s ===", title)
    logging.info("Kodi afleveringen: %d | IMDb afleveringen: %d | Te downloaden: %d",
                 len(kodi_eps), len(imdb_eps), len(todo))

    qualities = [
        "2160p 10bit HDR Atmos DTS-X 7.1 H265",
        "2160p 10bit HDR Atmos DTS-HD 7.1 H265",
        "2160p 10bit HDR Atmos TrueHD 7.1 H265",
        "2160p 10bit HDR Atmos DDP 5.1 H265",
        "2160p 10bit HDR DTS-X 7.1 H265",
        "2160p 10bit HDR DTS-HD 7.1 H265",
        "2160p 10bit HDR DTS 5.1 H265",
        "2160p HDR Atmos DTS-X 7.1 H265",
        "2160p HDR Atmos DTS-HD 7.1 H265",
        "2160p HDR Atmos DTS 5.1 H265",
        "2160p HDR DTS-X 7.1 H265",
        "2160p HDR DTS-HD 7.1 H265",
        "2160p HDR DTS 5.1 H265",
        "2160p 10bit DTS-X 7.1 H265",
        "2160p 10bit DTS-HD 7.1 H265",
        "2160p Atmos DTS 5.1 H265",
        "2160p Atmos 7.1 H265",
        "2160p Atmos 5.1 H265",
        "2160p 7.1 DTS-X H265",
        "2160p 7 1 DTS-HD H265",
        "2160p 5.1 DTS H265",
        "2160p 5 1 DTS H265",
        "2160p H265",
        "2160p HEVC",
        "2160p UHD BluRay",
        "2160p UHD WEB-DL",
        "2160p WEB-DL H265",
        "2160p WEB-DL",
        "2160p BluRay",
        "2160p UHD",
        "2160p",

        "1080p 10bit HDR Atmos DTS-X 7.1 H265",
        "1080p 10bit HDR Atmos DTS-HD 7.1 H265",
        "1080p 10bit HDR DTS-X 7.1 H265",
        "1080p 10bit HDR DTS-HD 7.1 H265",
        "1080p HDR Atmos DTS 5.1 H265",
        "1080p HDR DTS-X 7.1 H265",
        "1080p HDR DTS-HD 7.1 H265",
        "1080p HDR DTS 5.1 H265",
        "1080p Atmos DTS-X 7.1 H265",
        "1080p Atmos DTS-HD 7.1",
        "1080p Atmos DTS 5.1",
        "1080p 7.1 DTS-X",
        "1080p 7 1 DTS-HD",
        "1080p 5.1 DTS",
        "1080p 5 1 DTS",
        "1080p H265",
        "1080p HEVC",
        "1080p BluRay DTS",
        "1080p BluRay",
        "1080p WEB-DL H265",
        "1080p WEB-DL",
        "1080p WEBRip",
        "1080p HDTV",
        "1080p Proper",
        "1080p Repack",

        "720p DTS 5.1",
        "720p 5.1 DTS",
        "720p H265",
        "720p BluRay",
        "720p WEB-DL",
        "720p WEBRip",
        "720p HDTV",
        "720p Proper",
        "720p Repack",
        "720p",

        "576p",
        "480p",
        "SD",

        "BluRay",
        "WEB-DL",
        "WEBRip",
        "HDTV",

        "DTS-X",
        "DTS-HD",
        "DTS",
        "DTX",
        "Atmos",
        "TrueHD",
        "DDP",

        "H265",
        "H264",
        "HEVC",
        "X265",
        "X264",

        "FullHD",
        "FHD",
        "1080p",
        "HD",

        "SDTV",
        "Low Quality",
        "Unknown",
        "Any",
        ""
    ]
    for season, episode in todo:
        found = False

        # === Stap 1: eerst zoeken zonder quality ===
        base_query = f"{title} s{season:02}e{episode:02}"
        base_results = search_tpb(base_query)

        if not (
            base_results
            and re.search(r"S\d{2}E\d{2}", base_results[0].get("name", ""), re.I)
        ):
            logging.info("Niet gevonden (geen geldige basisresultaten): %s S%02dE%02d", title, season, episode)
            continue  # stoppen als er geen geldige SxxExx match is

        # === Stap 2: basisresultaat gevonden → qualities aflopen zoals voorheen ===
        for q in qualities:
            query = f"{title} s{season:02}e{episode:02} {q}"
            for r in search_tpb(query):
                name = r.get("name", "")
                if name and re.search(r"S\d{2}E\d{2}", name, re.I):
                    magnet = f"magnet:?xt=urn:btih:{r['info_hash']}&dn={quote(name)}"
                    add_magnet_to_qbittorrent(magnet, save_path=SAVE_PATH_SERIES)
                    logging.info("Toegevoegd: %s S%02dE%02d → %s", title, season, episode, name)
                    found = True
                    break
            if found:
                break

        if not found:
            logging.info("Niet gevonden met qualities: %s S%02dE%02d", title, season, episode)



def process_film(entry):
    title = entry["title"]
    imdb_id = entry.get("imdb_id")
    year = entry.get("year")

    # Controleer Kodi: als film al bestaat, niet downloaden
    kodi_id = None
    if USE_MYSQL and imdb_id:
        kodi_id = get_kodi_id_from_imdb(imdb_id)
    if kodi_id:
        logging.info("Film al aanwezig in Kodi: %s", title)
        return

    logging.info("=== Film controleren: %s ===", title)

    qualities = ["2160p HDR", "2160p", "h265", "1080p BluRay", "1080p WEB-DL", "1080p"]
    found = False
    for q in qualities:
        parts = [title]
        if year:
            parts.append(str(year))
        parts.append(q)
        query = " ".join(parts)
        for r in search_tpb(query):
            name = r.get("name", "")
            if name:
                magnet = f"magnet:?xt=urn:btih:{r['info_hash']}&dn={quote(name)}"
                add_magnet_to_qbittorrent(magnet, save_path=SAVE_PATH_MOVIES)
                logging.info("Film toegevoegd: %s → %s", title, name)
                found = True
                break
        if found:
            break
    if not found:
        logging.info("Film niet gevonden: %s", title)

# ================= Main =================
def run_all_searches():
    config = read_config_file(CONFIG_FILE)
    for s in config.get("series", []):
        try:
            process_series(s)
        except Exception as e:
            logging.error("Serie fout (%s): %s", s.get("title"), e)
    for f in config.get("films", []):
        try:
            process_film(f)
        except Exception as e:
            logging.error("Film fout (%s): %s", f.get("title"), e)

if __name__ == "__main__":
    logging.info("Media Retriever gestart")
    run_all_searches()
