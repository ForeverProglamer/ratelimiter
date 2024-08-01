import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, UTC, timedelta

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


class HttpClient:
    def __init__(self) -> None:
        self.first_request_sent = False
        self.ratelimit = None
        self.requests_sent_in_time_window = 0  # Misleading name, it is rather `requests_started_count`.
        self.first_request_sent_event = asyncio.Event()
        self.condition = asyncio.Condition()
        self.bg_task = asyncio.create_task(self._notify_when_ratelimit_resets())

    async def _notify_when_ratelimit_resets(self) -> None:
        while True:
            now = datetime.now(UTC)
            if self.ratelimit and now >= self.ratelimit.reset and self.requests_sent_in_time_window == 0:
                async with self.condition:
                    logging.info(f"Reseting sent requests, notifying {self.ratelimit.limit} tasks, reset={self.ratelimit.reset.strftime(datefmt)}")
                    self.requests_sent_in_time_window = 0
                    self.condition.notify(self.ratelimit.limit)
            await asyncio.sleep(0.3)

    async def request(self, url: str, ratelimit: RateLimit) -> None:
        host, id_ = url.split(" ")
        
        if not self.first_request_sent:
            self.first_request_sent = True
            await self._send_request(url, ratelimit)
            self.first_request_sent_event.set()
            return
        
        await self.first_request_sent_event.wait()

        if self.ratelimit and self.requests_sent_in_time_window == self.ratelimit.limit:
            logging.info(f"Task of requesting {url} is going to wait...")
            async with self.condition:
                await self.condition.wait()
        
        await self._send_request(url, ratelimit)

    async def _send_request(self, url: str, ratelimit: RateLimit) -> None:
        logging.info(f"Sending request to {url}, {ratelimit=}...")
        self.requests_sent_in_time_window += 1
        await asyncio.sleep(1)
        self.ratelimit = ratelimit
        self.requests_sent_in_time_window -= 1
        logging.info(f"Response received for {url}!")


async def main() -> None:
    client = HttpClient()
    await asyncio.gather(
        *[client.request(f"host-a {i+1}", ratelimit) for i, ratelimit in enumerate(generate_server_response_headers(15))],
        # *[client.request(f"host-b {i+1}") for i in range(10)]
    )


if __name__ == "__main__":
    asyncio.run(main())

