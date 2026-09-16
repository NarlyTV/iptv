import json
from collections.abc import KeysView
from functools import partial
from typing import Any
from urllib.parse import urlencode, urljoin

from playwright.async_api import Browser

from .utils import Cache, Event, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "WATCHFTY"

CACHE_FILE = Cache(TAG, exp=10_800)

BASE_DOMAIN = "watchfooty.st"

API_URL, BASE_URL = f"https://api.{BASE_DOMAIN}", f"https://www.{BASE_DOMAIN}"


def build_wfty_url(live: bool, event_id: str | None = None) -> str:
    url = urljoin(
        API_URL,
        (
            "_internal/trpc/sports.getSportsLiveMatchesCount,sports.getPopularMatches,sports.getPopularLiveMatches"
            if live
            else "_internal/trpc/sports.getSportsLiveMatchesCount,sports.getMatchById"
        ),
    )

    input_data = {
        "0": {
            "json": {
                "start": (now := Time.rn()).isoformat(),
                "end": now.delta(days=1).isoformat(),
            }
        },
    } | (
        {
            "1": {
                "json": None,
                "meta": {"values": ["undefined"]},
            },
            "2": {
                "json": None,
                "meta": {"values": ["undefined"]},
            },
        }
        if live
        else {
            "1": {
                "json": {
                    "id": event_id,
                    "withoutAdditionalInfo": True,
                    "withoutLinks": False,
                }
            }
        }
    )

    params = {
        "batch": "1",
        "input": json.dumps(input_data, separators=(",", ":")),
    }

    return f"{url}?{urlencode(params)}"


async def pre_process(url: str, url_num: int) -> str | None:
    if not (event_data := await network.request(url, url_num, log=log)):
        return

    api_data: dict = (
        (event_data.json() or [{}])[-1].get("result", {}).get("data", {}).get("json")
    )

    if not api_data:
        log.warning(f"URL {url_num}) No API data found.")
        return

    if not (links := api_data.get("fixtureData", {}).get("links")):
        log.warning(f"URL {url_num}) No stream links found.")
        return

    quality_links: list[dict] = sorted(
        [link for link in links if (wld := link.get("wld")) and "e" not in wld],
        key=lambda x: x.get("viewerCount") or -1,
        reverse=True,
    )

    if not quality_links:
        log.warning(f"URL {url_num}) No valid quality link found.")
        return

    link_data = quality_links[0]

    embed_path = (
        link_data["gi"],
        link_data["t"],
        link_data["wld"]["cn"],
        link_data["wld"]["sn"],
    )

    return f"https://sportsembed.su/embed/{'/'.join(embed_path)}?player=clappr&autoplay=true"


async def get_events(cached_keys: KeysView[str]) -> list[Event]:
    events: list[Event] = []

    live_url = build_wfty_url(live=True)

    if not (live_data := await network.request(live_url, log=log)):
        return events

    api_data: list[dict[str, Any]] = (
        (live_data.json() or [{}])[-1].get("result", {}).get("data", {}).get("json")
    )

    if not api_data:
        return events

    for link in api_data:
        if not link.get("viewerCount"):
            continue

        event_league: str = link["league"]

        event_name: str = link["title"]

        if f"[{event_league}] {event_name} ({TAG})" in cached_keys:
            continue

        events.append(
            Event(
                sport=event_league,
                name=event_name,
                link=build_wfty_url(live=False, event_id=link["id"]),
            )
        )

    return events


async def scrape(browser: Browser) -> None:
    cached_urls = CACHE_FILE.load()

    valid_urls = {k: v for k, v in cached_urls.items() if v["source"]}

    valid_count = cached_count = len(valid_urls)

    urls.update(valid_urls)

    log.info(f"Loaded {cached_count} event(s) from cache")

    log.info(f'Scraping from "{BASE_URL}"')

    if events := await get_events(cached_urls.keys()):
        log.info(f"Processing {len(events)} new URL(s)")

        now = Time.rn()

        async with network.event_context(browser, stealth=False) as context:
            for i, ev in enumerate(events, start=1):
                source, event_link = None, None

                async with network.event_page(context) as page:
                    if event_link := await pre_process(ev.link, i):
                        handler = partial(
                            network.process_event,
                            url=event_link,
                            url_num=i,
                            page=page,
                            log=log,
                        )

                        source = await network.safe_process(
                            handler,
                            url_num=i,
                            semaphore=network.PW_S,
                            timeout=20,
                            log=log,
                        )

                    key = f"[{ev.sport}] {ev.name} ({TAG})"

                    tvg_id, logo = leagues.get_tvg_info(ev.sport, ev.name)

                    entry = {
                        "source": source,
                        "logo": logo,
                        "refer": event_link,
                        "timestamp": now.timestamp(),
                        "tvg-id": tvg_id or "Live.Event.us",
                        "link": ev.link,
                    }

                    cached_urls[key] = entry

                    if source:
                        valid_count += 1

                        urls[key] = entry

        log.info(f"Collected and cached {valid_count - cached_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_urls)
