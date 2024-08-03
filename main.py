import asyncio
from collections import defaultdict
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, UTC, timedelta
from enum import StrEnum

datefmt = "%H:%M:%S"
logging.basicConfig(format="%(asctime)s %(message)s", datefmt=datefmt, level=logging.INFO)
logging.Formatter.converter = time.gmtime


@dataclass
class RateLimit:
    limit: int
    remaining: int
    reset: datetime

    def __repr__(self) -> str:
        return f"{type(self).__name__}(limit={self.limit}, remaining={self.remaining}, reset={self.reset.strftime(datefmt)})"


def generate_server_response_headers(requests_count: int = 20, ratelimit_limit: int = 5, ratelimit_reset_duration_s: int = 5) -> list[RateLimit]:
    ratelimit_reset = datetime.now(UTC)
    headers = []
    for i in range(requests_count):
        if i % ratelimit_limit == 0:
            ratelimit_reset += timedelta(seconds=ratelimit_reset_duration_s)
        headers.append(RateLimit(
            limit=ratelimit_limit,
            remaining=(ratelimit_limit - i % ratelimit_limit) - 1,
            reset=ratelimit_reset
        ))
    return headers


class Stage(StrEnum):
    FETCH_RATELIMIT = "fetch_ratelimit"
    FETCHING_RATELIMIT = "fetching_ratelimit"
    SEND_CONCURRENT_REQUESTS = "send_concurrent_requests"
    SENDING_CONCURRENT_REQUESTS = "sending_concurrent_requests"


@dataclass
class RateLimiter:
    requests_sent_in_time_window: int = 0
    ratelimit: RateLimit | None = None
    stage: Stage = Stage.FETCH_RATELIMIT
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)


class HttpClient:
    def __init__(self) -> None:
        self.host_to_ratelimiter: dict[str, RateLimiter] = defaultdict(lambda: RateLimiter())
        self.bg_task = asyncio.create_task(self._notify_when_ratelimit_resets())

    async def _notify_when_ratelimit_resets(self) -> None:
        while True:
            for host, ratelimiter in self.host_to_ratelimiter.items():
                if ratelimiter.stage == Stage.FETCH_RATELIMIT:
                    async with ratelimiter.condition:
                        logging.info(f"{host} | notifying 1 task to fetch ratelimit...")
                        ratelimiter.condition.notify()
                elif ratelimiter.stage == Stage.SEND_CONCURRENT_REQUESTS:
                    async with ratelimiter.condition:
                        logging.info(f"{host} | notifying {ratelimiter.ratelimit.limit - 1} tasks, reset={ratelimiter.ratelimit.reset.strftime(datefmt)}")  # type: ignore
                        ratelimiter.stage = Stage.SENDING_CONCURRENT_REQUESTS
                        ratelimiter.condition.notify(ratelimiter.ratelimit.limit - 1)  # type: ignore

            await asyncio.sleep(0.3)

    async def request(self, url: str, ratelimit: RateLimit) -> None:
        host, id_ = url.split(" ")
        ratelimiter = self.host_to_ratelimiter[host]
        
        async with ratelimiter.condition:
            logging.info(f"Task of requesting {url} is going to wait...")
            await ratelimiter.condition.wait()

        if ratelimiter.stage == Stage.FETCH_RATELIMIT:
            ratelimiter.stage = Stage.FETCHING_RATELIMIT
            await self._send_request(url, ratelimit)
            ratelimiter.ratelimit = ratelimit
            ratelimiter.stage = Stage.SEND_CONCURRENT_REQUESTS if datetime.now(UTC) < ratelimiter.ratelimit.reset else Stage.FETCH_RATELIMIT
            return

        await self._send_request(url, ratelimit)

    async def _send_request(self, url: str, ratelimit: RateLimit) -> None:
        host, _ = url.split(" ")
        ratelimiter = self.host_to_ratelimiter[host]
        logging.info(f"Sending request to {url}, {ratelimit=}...")
        await asyncio.sleep(1)
        ratelimiter.requests_sent_in_time_window += 1
        if ratelimiter.stage == Stage.SENDING_CONCURRENT_REQUESTS and ratelimiter.requests_sent_in_time_window == ratelimiter.ratelimit.limit:  # type: ignore
            ratelimiter.stage = Stage.FETCH_RATELIMIT
            ratelimiter.requests_sent_in_time_window = 0
        logging.info(f"Response received for {url}!")


async def main() -> None:
    client = HttpClient()
    await asyncio.gather(
        *[client.request(f"host-a {i+1}", ratelimit) for i, ratelimit in enumerate(generate_server_response_headers(15))],
        *[client.request(f"host-b {i+1}", ratelimit) for i, ratelimit in enumerate(generate_server_response_headers(10))]
    )
    logging.info(f"{client.host_to_ratelimiter=}")


if __name__ == "__main__":
    asyncio.run(main())

