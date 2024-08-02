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
        self.is_first_request_initiated = False
        self.first_response_received = asyncio.Event()
        self.rest_of_remaining_requests_sent = asyncio.Event()
        self.incoming_requests_count = 0
        self.requests_sent_in_time_window = 0
        self.ratelimit = None
        self.condition = asyncio.Condition()
        self.bg_task = asyncio.create_task(self._notify_when_ratelimit_resets())

    async def _notify_when_ratelimit_resets(self) -> None:
        while True:
            if self.incoming_requests_count == 0:
                await asyncio.sleep(0.3)
                continue

            if not self.is_first_request_initiated:
                async with self.condition:
                    logging.info("Waking the first task up...")
                    self.condition.notify()
                    self.rest_of_remaining_requests_sent.clear()

            await self.first_response_received.wait()

            async with self.condition:
                logging.info(f"Notifying {self.ratelimit.limit - 1} tasks, reset={self.ratelimit.reset.strftime(datefmt)}")
                self.condition.notify(self.ratelimit.limit - 1)

            await self.rest_of_remaining_requests_sent.wait()

            self.is_first_request_initiated = False
            self.first_response_received.clear()

    async def request(self, url: str, ratelimit: RateLimit) -> None:
        host, id_ = url.split(" ")
        
        async with self.condition:
            logging.info(f"Task of requesting {url} is going to wait...")
            self.incoming_requests_count += 1
            await self.condition.wait()

        if not self.is_first_request_initiated:
            self.is_first_request_initiated = True
            await self._send_request(url, ratelimit)
            self.ratelimit = ratelimit
            self.first_response_received.set()
            return

        await self._send_request(url, ratelimit)

    async def _send_request(self, url: str, ratelimit: RateLimit) -> None:
        logging.info(f"Sending request to {url}, {ratelimit=}...")
        await asyncio.sleep(1)
        self.requests_sent_in_time_window += 1
        if self.ratelimit and self.requests_sent_in_time_window == self.ratelimit.limit:
            self.rest_of_remaining_requests_sent.set()
            self.requests_sent_in_time_window = 0
        logging.info(f"Response received for {url}!")


async def main() -> None:
    client = HttpClient()
    await asyncio.gather(
        *[client.request(f"host-a {i+1}", ratelimit) for i, ratelimit in enumerate(generate_server_response_headers(15))],
        # *[client.request(f"host-b {i+1}") for i in range(10)]
    )


if __name__ == "__main__":
    asyncio.run(main())

