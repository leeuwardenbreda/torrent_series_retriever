import os
import datetime
from pathlib import Path
import requests
import re
import logging
import json
import time
from urllib.parse import quote

from dotenv import load_dotenv

# ================= ENV =================
load_dotenv()

USE_QBITTORRENT = True

IMDB_API_BASE = os.getenv("IMDB_API_BASE", "https://api.imdbapi.dev")
CONFIG_FILE = Path(os.getenv("SERIES_CONFIG_FILE", "series_config.json"))

# ---- MySQL (optioneel) ----
MYSQL_ENABLED = all([
    os.getenv("MYSQL_HOST"),
    os.getenv("MYSQL_USER"),
    os.getenv("MYSQL_PASSWORD"),
    os.getenv("MYSQL_DATABASE"),
])

if MYSQL_ENABLED:
    import mysql.connector
    MYSQL_CONFIG = {
        "host": os.getenv("MYSQL_HOST"),
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "database": os.getenv("MYSQL_DATABASE"),
        "port": int(os.getenv("MYSQL_PORT", 3306)),
    }

# ---- qBittorrent ----
QBITTORRENT_CONFIG = {
    "url": os.getenv("QBITTORRENT_URL"),
    "username": os.getenv("QBITTORRENT_USERNAME"),
    "password": os.getenv("QBITTORRENT_PASSWORD"),
}


# ================= Helpers =================
def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", name)


# ================= MySQL =================
def get_series_from_kodi_mysql():
    if not MYSQL_ENABLED:
        logging.info("MySQL niet geconfigureerd, Kodi data wordt overgeslagen")
        return []

    logging.info("Series ophalen uit Kodi MySQL")
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor(dictionary=True)

    cur.execute(
        "SELECT idShow, c00 AS title, c05 AS date, c10 AS episodeguide FROM tvshow"
    )

    series = []
    for row in cur.fetchall():
        imdb_id = None
        ep_guide_raw = row.get("episodeguide")

        if ep_guide_raw:
            ep_guide_clean = re.sub(r'^<.*?>|</.*?>$', '', ep_guide_raw).strip()
            try:
                ep_guide = json.loads(ep_guide_clean)
                imdb_id = ep_guide.get("imdb")
            except json.JSONDecodeError:
                logging.warning(
                    "Episodeguide niet te parsen voor '%s'", row["title"]
                )

        series.append({
            "id": row["idShow"],
            "title": row["title"],
            "year": row["date"][:4] if row["date"] else None,
            "imdb_id": imdb_id
        })

    cur.close()
    conn.close()
    logging.info("Kodi series geladen: %d", len(series))
    return series


def get_existing_episodes(series_id):
    if not MYSQL_ENABLED or series_id is None:
        return set()

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor(dictionary=True)

    cur.execute(
        "SELECT c12 AS season, c13 AS episode FROM episode WHERE idShow = %s",
        (series_id,)
    )

    episodes = {(int(r["season"]), int(r["episode"])) for r in cur.fetchall()}

    cur.close()
    conn.close()
    return episodes


# ================= IMDb =================
def get_all_episodes_imdb(imdb_id, seasons_filter=None):
    if not imdb_id:
        return set()

    logging.info("IMDb afleveringen ophalen: %s", imdb_id)

    episodes = set()
    page_token = None
    headers = {
        "Accept": "application/json",
        "User-Agent": "Kodi-IMDb-Sync/1.0"
    }

    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date()

    try:
        while True:
            params = {}
            if page_token:
                params["pageToken"] = page_token

            r = requests.get(
                f"{IMDB_API_BASE}/titles/{imdb_id}/episodes",
                headers=headers,
                params=params,
                timeout=15
            )
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
                        release_date = datetime.date(
                            rd.get("year"),
                            rd.get("month"),
                            rd.get("day")
                        )
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


# ================= TPB =================
def search_tpb(query):
    logging.debug("TPB zoeken: %s", query)
    try:
        r = requests.get(
            f"https://apibay.org/q.php?q={quote(query)}&cat=0",
            timeout=20
        )
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


# ================= qBittorrent =================
def qbittorrent_login(session: requests.Session):
    r = session.post(
        f"{QBITTORRENT_CONFIG['url']}/api/v2/auth/login",
        data={
            "username": QBITTORRENT_CONFIG["username"],
            "password": QBITTORRENT_CONFIG["password"]
        },
        timeout=10
    )
    if r.text != "Ok.":
        raise RuntimeError("qBittorrent login mislukt")


def add_magnet_to_qbittorrent(magnet, category):
    session = requests.Session()
    qbittorrent_login(session)

    logging.info("Magnet toevoegen (gepauzeerd): %s", category)

    r = session.post(
        f"{QBITTORRENT_CONFIG['url']}/api/v2/torrents/add",
        data={
            "urls": magnet,
            "paused": "true",
            "category": category
        },
        timeout=10
    )

    if r.status_code != 200:
        raise RuntimeError(
            f"qBittorrent fout ({r.status_code}): {r.text}"
        )


# ================= Serie =================
class Serie:
    def __init__(self, kodi_id, title, year, imdb_id, seasons_filter=None):
        self.kodi_id = kodi_id
        self.title = title
        self.year = year
        self.imdb_id = imdb_id
        self.seasons_filter = set(seasons_filter) if seasons_filter else None

        self.kodi_episodes = get_existing_episodes(kodi_id)
        self.all_episodes = get_all_episodes_imdb(
            imdb_id, self.seasons_filter
        )

    @property
    def to_download(self):
        return sorted(self.all_episodes - self.kodi_episodes)


# ================= Main =================
def load_series_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("series", [])
    return []


def get_series_to_process():
    kodi_series = get_series_from_kodi_mysql()
    kodi_map = {s["imdb_id"]: s for s in kodi_series if s["imdb_id"]}

    result = []
    for e in load_series_config():
        imdb_id = e.get("imdb_id")
        if not imdb_id:
            continue

        kodi = kodi_map.get(imdb_id) if MYSQL_ENABLED else None

        result.append({
            "id": kodi["id"] if kodi else None,
            "title": e.get("title") or (kodi["title"] if kodi else None),
            "year": e.get("year") or (kodi["year"] if kodi else None),
            "imdb_id": imdb_id,
            "seasons": e.get("seasons")
        })

    logging.info("Series uit config: %d", len(result))
    return result


def process_series(serie: Serie):
    logging.info("=== Serie: %s (%s) ===", serie.title, serie.year)

    logging.info(
        "Kodi: %d | IMDb: %d | Te downloaden: %d",
        len(serie.kodi_episodes),
        len(serie.all_episodes),
        len(serie.to_download)
    )

    qualities = [
        "2160p HDR",
        "2160p",
        "h265",
        "1080p BluRay",
        "1080p WEB-DL",
        "1080p"
    ]

    for season, episode in serie.to_download:
        found = False

        for q in qualities:
            query = f"{serie.title} s{season:02}e{episode:02} {q}"
            for r in search_tpb(query):
                name = r.get("name", "")
                if name == "No results returned":
                    continue

                if re.search(r"S\d{2}E\d{2}", name, re.I) and \
                   re.search(re.escape(serie.title), name, re.I):

                    magnet = (
                        f"magnet:?xt=urn:btih:{r['info_hash']}"
                        f"&dn={quote(name)}"
                    )

                    add_magnet_to_qbittorrent(
                        magnet,
                        sanitize_filename(serie.title)
                    )

                    logging.info(
                        "Toegevoegd: %s S%02dE%02d → %s",
                        serie.title, season, episode, name
                    )
                    found = True
                    break

            if found:
                break

        if not found:
            logging.info(
                "Niet gevonden: %s S%02dE%02d",
                serie.title, season, episode
            )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    for s in get_series_to_process():
        try:
            serie = Serie(
                s["id"],
                s["title"],
                s["year"],
                s["imdb_id"],
                s.get("seasons")
            )
            process_series(serie)
        except Exception as e:
            logging.error("Fout bij serie '%s': %s", s["title"], e)


if __name__ == "__main__":
    main()
