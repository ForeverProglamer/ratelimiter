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
class HostRequestsInfo:
    requests_sent_in_time_window: int = 0
    ratelimit: RateLimit | None = None
    stage: Stage = Stage.FETCH_RATELIMIT
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)


class HttpClient:
    def __init__(self) -> None:
        self.host_to_requests_info: dict[str, HostRequestsInfo] = defaultdict(lambda: HostRequestsInfo())
        self.bg_task = asyncio.create_task(self._control_tasks_notification())

    async def _control_tasks_notification(self) -> None:
        while True:
            for host, requests_info in self.host_to_requests_info.items():
                if requests_info.stage == Stage.FETCH_RATELIMIT:
                    async with requests_info.condition:
                        logging.info(f"{host} | notifying 1 task to fetch ratelimit...")
                        requests_info.condition.notify()
                elif requests_info.stage == Stage.SEND_CONCURRENT_REQUESTS:
                    async with requests_info.condition:
                        logging.info(
                            f"{host} | notifying {requests_info.ratelimit.limit - 1} tasks, "  # type: ignore
                            f"reset={requests_info.ratelimit.reset.strftime(datefmt)}"  # type: ignore
                        )
                        requests_info.stage = Stage.SENDING_CONCURRENT_REQUESTS
                        requests_info.condition.notify(requests_info.ratelimit.limit - 1)  # type: ignore

            await asyncio.sleep(0.3)

    async def request(self, url: str, ratelimit: RateLimit) -> None:
        host, id_ = url.split(" ")
        requests_info = self.host_to_requests_info[host]
        
        async with requests_info.condition:
            logging.info(f"Task of requesting {url} is going to wait...")
            await requests_info.condition.wait()

        if requests_info.stage == Stage.FETCH_RATELIMIT:
            requests_info.stage = Stage.FETCHING_RATELIMIT
            await self._send_request(url, ratelimit)
            requests_info.ratelimit = ratelimit
            requests_info.stage = Stage.SEND_CONCURRENT_REQUESTS if datetime.now(UTC) < requests_info.ratelimit.reset else Stage.FETCH_RATELIMIT
            return

        await self._send_request(url, ratelimit)

    async def _send_request(self, url: str, ratelimit: RateLimit) -> None:
        host, _ = url.split(" ")
        requests_info = self.host_to_requests_info[host]
        logging.info(f"Sending request to {url}, {ratelimit=}...")
        await asyncio.sleep(1)
        requests_info.requests_sent_in_time_window += 1
        if (
            requests_info.stage == Stage.SENDING_CONCURRENT_REQUESTS and
            requests_info.requests_sent_in_time_window == requests_info.ratelimit.limit  # type: ignore
        ):
            requests_info.stage = Stage.FETCH_RATELIMIT
            requests_info.requests_sent_in_time_window = 0
        logging.info(f"Response received for {url}!")


async def main() -> None:
    client = HttpClient()
    await asyncio.gather(
        *[client.request(f"host-a {i+1}", ratelimit) for i, ratelimit in enumerate(generate_server_response_headers(15))],
        *[client.request(f"host-b {i+1}", ratelimit) for i, ratelimit in enumerate(generate_server_response_headers(10))]
    )
    logging.info(f"{client.host_to_requests_info=}")


if __name__ == "__main__":
    asyncio.run(main())

