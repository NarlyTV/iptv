import re
from functools import partial
from urllib.parse import parse_qsl, urljoin, urlsplit

from selectolax.lexbor import LexborHTMLParser as HTMLParser

from .utils import Cache, Event, Time, get_logger, leagues, network
from .xyzstreams import API_FILE, refresh_api_cache

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "XYZ2"

CACHE_FILE = Cache(TAG, exp=28_800)

BASE_DOMAIN = "https://xyzstreams.st/"


async def process_event(url: str, url_num: int) -> str | None:
    if not (html_data := await network.request(url, url_num, log=log)):
        return

    elif not (sports_map := await get_sports_map()):
        return

    soup = HTMLParser(html_data.content)

    iframe = soup.css_first("iframe#stream-player")

    if not iframe or not (iframe_src := iframe.attributes.get("src")):
        log.warning(f"URL {url_num}) No iframe element/src found.")
        return

    splits = urlsplit(iframe_src)

    params = dict(parse_qsl(splits.query))

    if not (channel_id := params.get("src")):
        log.warning(f"URL {url_num}) No source found.")
        return

    elif not (channel_src := sports_map.get("NFL", {}).get(channel_id)):
        log.warning(f"URL {url_num}) No source found.")
        return

    log.info(f"URL {url_num}) Captured M3U8")
    return channel_src


async def get_sports_map() -> dict[str, dict[str, str]]:
    sports_map: dict[str, dict[str, str]] = {}

    if not (
        html_data := await network.request(
            urljoin(BASE_DOMAIN, "ftvembed"),
            log=log,
        )
    ):
        return sports_map

    ptrn = re.compile(r"M3U8_CHANNELS_MAP\s*=\s*\{(.*?)\};", re.S)

    if not (match := ptrn.search(html_data.text)):
        return sports_map

    pairs: list[tuple[str, str]] = re.findall(
        r"'([^']+)'\s*:\s*'([^']+)'",
        match[1],
    )

    sports_map["NFL"] = dict(pairs)

    return sports_map


async def get_events() -> list[Event]:
    now = Time.rn()

    events: list[Event] = []

    if not (
        html_data := await network.request(
            urljoin(BASE_DOMAIN, "nfl.html"),
            log=log,
        )
    ):
        return events

    if not (api_data := API_FILE.load(per_entry=False, ts_index=-1)):
        log.info("Refreshing API cache")

        api_data = await refresh_api_cache(now)

        API_FILE.write(api_data)

    ptrn = re.compile(r"NFL_STREAM_MAP\s*=\s*\{(.*?)\};", re.S)

    if not (match := ptrn.search(html_data.text)):
        return events

    pairs: list[tuple[str, str]] = re.findall(
        r"'([^']+)'\s*:\s*'([^']+)'",
        match[1],
    )

    for game_info in api_data:
        if not all(
            values := [
                game_info.get(x)
                for x in (
                    "league",
                    "name",
                    "shortName",
                )
            ]
        ):
            continue

        sport, name, short_name = values

        if sport != "NFL":
            continue

        for team_id, url in pairs:
            events.extend(
                Event(
                    sport="NFL",
                    name=name,
                    link=url,
                    timestamp=now.timestamp(),
                )
                for abbr in re.sub(r"(@|VS)", "", short_name, flags=re.I).split()
                if team_id == abbr
            )
    return events


async def scrape() -> None:
    cached_urls = CACHE_FILE.load()

    valid_urls = {k: v for k, v in cached_urls.items() if v["source"]}

    valid_count = cached_count = len(valid_urls)

    urls.update(valid_urls)

    log.info(f"Loaded {cached_count} event(s) from cache")

    log.info(f'Scraping from "{BASE_DOMAIN}"')

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
                "refer": BASE_DOMAIN,
                "timestamp": ev.timestamp,
                "tvg-id": tvg_id or "Live.Event.us",
            }

            cached_urls[key] = entry

            if source:
                valid_count += 1

                urls[key] = entry

        log.info(f"Collected and cached {valid_count - cached_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_urls)
