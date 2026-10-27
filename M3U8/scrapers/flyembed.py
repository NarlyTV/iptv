import asyncio
import re
from collections.abc import KeysView
from functools import partial

from playwright.async_api import Browser, Page

from .utils import Cache, Event, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "FLYEMBD"

CACHE_FILE = Cache(TAG, exp=7_200)

API_FILE = Cache(f"{TAG}-api", exp=19_800)

BASE_URL = "https://flyembed.cfd/"

API_URL = "https://ovogoal.pages.dev/api/v2/flyembed2.json"


def cleanup(s: str) -> str:
    return re.sub(r"(\r|\n|\t)", "", s).strip()


async def process_event(
    url: str,
    url_num: int,
    page: Page,
) -> tuple[str | None, str | None, str | None]:

    nones = None, None

    captured: list[str] = []

    got_one = asyncio.Event()

    handler = partial(
        network.capture_req,
        captured=captured,
        got_one=got_one,
    )

    page.on("request", handler)

    try:
        resp = await page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=6_000,
        )

        if not resp or not resp.ok:
            log.error(f"URL {url_num}) Status Code: {resp.status if resp else 'None'}")
            return nones

        try:
            ifr = page.locator("iframe[style*='position:fixed']")

            await ifr.wait_for(state="attached", timeout=1_250)

            ifr_src = await ifr.get_attribute("src")
        except TimeoutError:
            log.warning(f"URL {url_num}) No iframe found.")
            return nones

        wait_task = asyncio.create_task(got_one.wait())

        try:
            await asyncio.wait_for(wait_task, timeout=5)
        except TimeoutError:
            log.warning(f"URL {url_num}) Timed out waiting for M3U8.")
            return nones

        finally:
            if not wait_task.done():
                wait_task.cancel()

                try:
                    await wait_task
                except asyncio.CancelledError:
                    pass

        if captured:
            log.info(f"URL {url_num}) Captured M3U8")

            return captured[0], ifr_src
    except Exception as e:
        log.warning(f"URL {url_num}) {e}")
        return nones

    finally:
        page.remove_listener("request", handler)


async def get_events(cached_keys: KeysView[str]) -> list[Event]:
    now = Time.rn()

    events: list[Event] = []

    if not (api_data := API_FILE.load(per_entry=False, ts_index=-1)):
        log.info("Refreshing API cache")

        api_data = [{"timestamp": now.timestamp()}]

        if r := await network.request(API_URL, log=log):
            api_data: list[dict[str, str]] = r.json()

            api_data[-1]["timestamp"] = now.timestamp()

        API_FILE.write(api_data)

    start_dt = now.delta(hours=-3)
    end_dt = now.delta(minutes=30)

    for event_group in api_data:
        if not all(
            values := [
                event_group.get(x)
                for x in (
                    "League",
                    "Team1",
                    "Team2",
                    "MatchDate",
                    "MatchStartTime",
                    "IframeURL",
                )
            ]
        ):
            continue

        sport, away, home, date, time, link = values

        event_dt = Time.from_str(f"{date} {time}", tz_name="ALMT")

        if not start_dt <= event_dt <= end_dt:
            continue

        name = f"{away.strip()} vs {home.strip()}"

        sport, name = cleanup(sport), cleanup(name)

        if f"[{sport}] {name} ({TAG})" in cached_keys:
            continue

        events.append(
            Event(
                sport=sport,
                name=name,
                link=link,
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
                        process_event,
                        url=ev.link,
                        url_num=i,
                        page=page,
                    )

                    source, iframe = await network.safe_process(
                        handler,
                        url_num=i,
                        timeout_return=(None, None),
                        semaphore=network.PW_S,
                        log=log,
                    )

                    key = f"[{ev.sport}] {ev.name} ({TAG})"

                    tvg_id, logo = leagues.get_tvg_info(ev.sport, ev.name)

                    entry = {
                        "source": source,
                        "logo": logo,
                        "refer": iframe,
                        "timestamp": ev.timestamp,
                        "tvg-id": tvg_id or "Live.Event.us",
                        "link": ev.link,
                    }

                    cached_sources[key] = entry

                    if source:
                        valid_count += 1

                        urls[key] = entry

        log.info(f"Collected and cached {valid_count - cached_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_sources)
