from functools import partial
from urllib.parse import parse_qsl, urljoin, urlsplit

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

from .utils import Cache, Event, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "STRMCNTR"

CACHE_FILE = Cache(TAG, exp=28_800)

BASE_URL = "https://streamecenter.live"

ALT_BASE = "https://streame.center"


def cleanup(s: str) -> str:
    return "".join(i for i in s.split("—")[-1] if i.isascii()).strip()


def fix_sport(s: str) -> str:
    splits = s.split()

    i = splits[0]

    return f"{i.upper() if len(i) < 5 else i.capitalize()} {' '.join(x.capitalize() for x in splits[1:])}".strip()


async def process_event(url: str, url_num: int) -> str | None:
    if not (
        html_data := await network.request(
            url,
            url_num,
            params={"timeout": httpx.Timeout(25.0)},
            log=log,
        )
    ):
        return

    soup = HTMLParser(html_data.content)

    iframe = soup.css_first("iframe")

    if not iframe or not (src := iframe.attributes.get("src")):
        log.warning(f"URL {url_num}) No iframe element found.")
        return

    splits = urlsplit(src)

    params = dict(parse_qsl(splits.query))

    if not (stream_id := params.get("stream")):
        log.warning(f"URL {url_num}) No stream ID found.")
        return

    log.info(f"URL {url_num}) Captured M3U8")

    return (
        f"https://edgestream3.pro/stream/{stream_id}.m3u8"
        if stream_id.isdigit()
        else f"https://edgestream2.pro/hls/{stream_id}.m3u8"
    )


async def get_events() -> list[Event]:
    events: list[Event] = []

    if not (
        html_data := await network.request(
            urljoin(BASE_URL, "game-cards/embed"),
            log=log,
        )
    ):
        return events

    soup = HTMLParser(html_data.content)

    now = Time.rn()

    for card in soup.css(".game-card-group"):
        if not (sport_elem := card.css_first("h2")):
            continue

        sport = fix_sport(cleanup(sport_elem.text(strip=True)))

        for game in card.css(".game-card-row"):
            if not (event_time_elem := game.css_first(".game-card-when > time")):
                continue

            elif not (event_time := event_time_elem.attributes.get("datetime")):
                continue

            event_dt = Time.fromisoformat(event_time).to_tz("EST")

            if event_dt.date() != now.date():
                continue

            if not (name_elem := game.css_first("h3")):
                continue

            elif not (event_name := name_elem.attributes.get("aria-label")):
                continue

            for source in game.css(".game-card-source > a.game-card-open-link"):
                if not (href := source.attributes.get("href")):
                    continue

                name = f"{event_name} | {source.text(strip=True)}"

                events.append(
                    Event(
                        sport=fix_sport(sport),
                        name=name,
                        link=urljoin(BASE_URL, href),
                        timestamp=now.timestamp(),
                    )
                )

    return events


async def scrape() -> None:
    if cached_urls := CACHE_FILE.load():
        urls.update({k: v for k, v in cached_urls.items() if v["source"]})

        log.info(f"Loaded {len(urls)} event(s) from cache")

        return

    log.info(f'Scraping from "{BASE_URL}"')

    if events := await get_events():
        log.info(f"Processing {len(events)} URL(s)")

        for i, ev in enumerate(events, start=1):
            handler = partial(
                process_event,
                url=ev.link,
                url_num=i,
            )

            source = await network.safe_process(
                handler,
                url_num=i,
                semaphore=network.HTTP_S,
                log=log,
            )

            key = f"[{ev.sport}] {ev.name} ({TAG})"

            tvg_id, logo = leagues.get_tvg_info(ev.sport, ev.name)

            entry = {
                "source": source,
                "logo": logo,
                "refer": ALT_BASE,
                "timestamp": ev.timestamp,
                "tvg-id": tvg_id or "Live.Event.us",
                "link": ev.link,
            }

            cached_urls[key] = entry

            if source:
                urls[key] = entry

        log.info(f"Collected and cached {len(urls)} event(s)")

    else:
        log.info("No events found")

    CACHE_FILE.write(cached_urls)
