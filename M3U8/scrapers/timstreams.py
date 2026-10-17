from collections.abc import KeysView
from dataclasses import dataclass
from functools import partial
from typing import Any
from urllib.parse import urljoin

from playwright.async_api import Browser

from .utils import Cache, Event, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "TIMSTRM"

CACHE_FILE = Cache(TAG, exp=7_200)

API_FILE = Cache(f"{TAG}-api", exp=19_800)

BASE_URL = "https://timst.top"


@dataclass(kw_only=True, slots=True)
class TIMEvent(Event):
    logo: str | None = None


async def get_events(cached_keys: KeysView[str]) -> list[TIMEvent]:
    now = Time.rn()

    events: list[TIMEvent] = []

    if not (api_data := API_FILE.load(per_entry=False)):
        log.info("Refreshing API cache")

        api_data = {"timestamp": now.timestamp()}

        if r := await network.request(
            urljoin(BASE_URL, "api/live-upcoming"),
            log=log,
        ):
            api_data: dict[str, list[dict[str, Any]]] = r.json()

            api_data["timestamp"] = now.timestamp()

        API_FILE.write(api_data)

    start_dt = now.delta(hours=-3)
    end_dt = now.delta(minutes=30)

    sport_genres = {}

    if genres := api_data.get("genres", []):
        sport_genres = {
            genre["id"]: {
                0: genre["name"],
                **{sub["id"]: sub["name"] for sub in genre.get("sub_categories", [])},
            }
            for genre in genres
        }

    for event in api_data.get("events") or []:
        if not all(
            values := [
                event.get(x)
                for x in (
                    "name",
                    "genre",
                    "sub_genre",
                    "time",
                    "streams",
                )
            ]
        ):
            continue

        name, genre, sub_genre, event_time, streams = values

        if 17 <= genre <= 18:
            continue

        event_dt = Time.from_str(event_time, tz_name="EST")

        sport = sport_genres.get(genre, {}).get(sub_genre, "Live Event")

        if not start_dt <= event_dt <= end_dt:
            continue

        elif not (stream_url := streams[0].get("url")):
            continue

        elif f"[{sport}] {name} ({TAG})" in cached_keys:
            continue

        events.append(
            TIMEvent(
                sport=sport,
                name=name,
                link=stream_url,
                logo=event.get("logo"),
                timestamp=now.timestamp(),
            )
        )

    return events


async def scrape(browser: Browser) -> None:
    cached_sources = CACHE_FILE.load()

    valid_sources = {k: v for k, v in cached_sources.items() if v["source"]}

    valid_count = cached_count = len(valid_sources)

    urls.update(valid_sources)

    log.info(f"Loaded {cached_count} event(s) from cache")

    log.info(f'Scraping from "{BASE_URL}"')

    if events := await get_events(cached_sources.keys()):
        log.info(f"Processing {len(events)} new URL(s)")

        async with network.event_context(browser) as context:
            for i, ev in enumerate(events, start=1):
                async with network.event_page(context) as page:
                    handler = partial(
                        network.process_event,
                        url=ev.link,
                        url_num=i,
                        page=page,
                        log=log,
                    )

                    source = await network.safe_process(
                        handler,
                        url_num=i,
                        semaphore=network.PW_S,
                        log=log,
                    )

                    key = f"[{ev.sport}] {ev.name} ({TAG})"

                    tvg_id, logo = leagues.get_tvg_info(ev.sport, ev.name)

                    entry = {
                        "source": source,
                        "logo": ev.logo or logo,
                        "refer": ev.link,
                        "timestamp": ev.timestamp,
                        "tvg-id": tvg_id or "Live.Event.us",
                    }

                    cached_sources[key] = entry

                    if source:
                        valid_count += 1

                        urls[key] = entry

        log.info(f"Collected and cached {valid_count - cached_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_sources)
