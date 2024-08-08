import asyncio
import logging
import time
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from datetime import datetime, UTC, timedelta
from enum import StrEnum

from tenacity import before_log, retry, stop_after_attempt, wait_fixed, retry_if_exception_type

datefmt = "%H:%M:%S"
logging.basicConfig(format="%(asctime)s %(message)s", datefmt=datefmt, level=logging.INFO)
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)


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
    SEND_ONE_REQUEST = "send_one_request"
    SENDING_ONE_REQUEST = "sending_one_request"
    SEND_CONCURRENT_REQUESTS = "send_concurrent_requests"
    SENDING_CONCURRENT_REQUESTS = "sending_concurrent_requests"
    WAITING_FOR_RESET = "waiting_for_reset"


@dataclass
class HostRequestsInfo:
    requests_sent_in_time_window: int = 0
    incoming_requests: int = 0
    ratelimit: RateLimit | None = None
    stage: Stage = Stage.SEND_ONE_REQUEST
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)


class HttpClient:
    def __init__(self) -> None:
        self.host_to_requests_info: dict[str, HostRequestsInfo] = defaultdict(lambda: HostRequestsInfo())
        self.run_bg_task = True
        self.bg_task = asyncio.create_task(self._control_tasks_notification())
        self.concurrent_requests: dict[str, int] = Counter()

    async def _control_tasks_notification(self) -> None:
        while self.run_bg_task:
            now = datetime.now(UTC)
            for host, requests_info in self.host_to_requests_info.items():
                if requests_info.stage == Stage.SEND_ONE_REQUEST and requests_info.incoming_requests > 0:
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
                elif requests_info.stage == Stage.SENDING_CONCURRENT_REQUESTS and (
                    requests_info.requests_sent_in_time_window + self.concurrent_requests[host] 
                    < requests_info.ratelimit.limit  # type: ignore
                ) and requests_info.incoming_requests > 0:
                    async with requests_info.condition:
                        tasks_to_notify = (
                            requests_info.ratelimit.limit -   # type: ignore
                            (requests_info.requests_sent_in_time_window + self.concurrent_requests[host])
                        )
                        logging.info(
                            f"{host} | notifying additional {tasks_to_notify} tasks, "  # type: ignore
                            f"reset={requests_info.ratelimit.reset.strftime(datefmt)}"  # type: ignore
                        )
                        requests_info.condition.notify(tasks_to_notify)  # type: ignore
                elif (
                    requests_info.stage == Stage.WAITING_FOR_RESET and
                    now >= requests_info.ratelimit.reset  # type: ignore
                ):
                    requests_info.stage = Stage.SEND_ONE_REQUEST
                    requests_info.requests_sent_in_time_window = 0

            await asyncio.sleep(0.3)

    @retry(
        stop=stop_after_attempt(2+1),
        retry=retry_if_exception_type(ZeroDivisionError),
        wait=wait_fixed(0.5),
        before=before_log(logger, logging.DEBUG)
    )
    async def request(self, url: str, ratelimit: RateLimit, raise_error: bool = False, retryable_error: bool = True) -> None:
        host, id_ = url.split(" ")
        requests_info = self.host_to_requests_info[host]
        
        async with requests_info.condition:
            logging.info(f"Task of requesting {url} is going to wait...")
            requests_info.incoming_requests += 1
            await requests_info.condition.wait()
        requests_info.incoming_requests -= 1
        self.concurrent_requests[host] += 1

        if requests_info.stage == Stage.SEND_ONE_REQUEST:
            requests_info.stage = Stage.SENDING_ONE_REQUEST

        try:
            await self._send_request(url, ratelimit, raise_error, retryable_error)
        except Exception as error:
            logging.error(f"{url=}, {error=}")
            raise
        finally:
            self.concurrent_requests[host] -= 1
            requests_info.requests_sent_in_time_window += 1

            now = datetime.now(UTC)
            if requests_info.stage == Stage.SENDING_ONE_REQUEST and requests_info.ratelimit:
                if (
                    now >= requests_info.ratelimit.reset or
                    requests_info.requests_sent_in_time_window == requests_info.ratelimit.limit
                ):
                    requests_info.stage = Stage.WAITING_FOR_RESET
                elif now < requests_info.ratelimit.reset and requests_info.incoming_requests >= 1:
                    requests_info.stage = Stage.SEND_CONCURRENT_REQUESTS
                elif now < requests_info.ratelimit.reset and requests_info.incoming_requests == 0:
                    requests_info.stage = Stage.SEND_ONE_REQUEST
            elif requests_info.stage == Stage.SENDING_CONCURRENT_REQUESTS and requests_info.ratelimit and (
                now >= requests_info.ratelimit.reset or
                requests_info.requests_sent_in_time_window == requests_info.ratelimit.limit
            ):
                requests_info.stage = Stage.WAITING_FOR_RESET

    async def _send_request(self, url: str, ratelimit: RateLimit, raise_error: bool = False, retryable_error: bool = True) -> None:
        host, _ = url.split(" ")
        requests_info = self.host_to_requests_info[host]

        logging.info(f"Sending request to {url}, {ratelimit=}...")
        await asyncio.sleep(1)
        if requests_info.stage == Stage.SENDING_ONE_REQUEST:
            requests_info.ratelimit = ratelimit
        logging.info(f"Response received for {url}!")

        if raise_error:
            if retryable_error:
                raise ZeroDivisionError
            raise RuntimeError

    async def close(self) -> None:
        self.run_bg_task = False
        await self.bg_task


async def concurrent_requests_single_host_run() -> None:
    logging.info("Concurrent Requests Single Host Run")
    client = HttpClient()
    await asyncio.gather(
        *[client.request(f"host-a {i+1}", ratelimit) for i, ratelimit in enumerate(generate_server_response_headers(15))],
    )
    await client.close()
    logging.info(f"{client.host_to_requests_info=}\n")


async def concurrent_requests_multiple_hosts_run() -> None:
    logging.info("Concurrent Requests Multiple Hosts Run")
    client = HttpClient()
    await asyncio.gather(
        *[client.request(f"host-b {i+1}", ratelimit) for i, ratelimit in enumerate(generate_server_response_headers(15))],
        *[client.request(f"host-c {i+1}", ratelimit) for i, ratelimit in enumerate(generate_server_response_headers(10))]
    )
    await client.close()
    logging.info(f"{client.host_to_requests_info=}\n")


async def sequential_requests_single_host_run() -> None:
    logging.info("Sequential Requests Single Host Run")
    client = HttpClient()
    for i, ratelimit in enumerate(generate_server_response_headers(requests_count=6, ratelimit_limit=2)):
        await client.request(f"host-d {i+1}", ratelimit)
    await client.close()
    logging.info(f"{client.host_to_requests_info=}\n")


async def concurrent_requests_single_host_retry_run() -> None:
    logging.info("Concurrent Requests Single Host Run | Retry")
    client = HttpClient()
    data = [(f"host-a {i+1}", ratelimit, i in (1, 2), i != 2) for i, ratelimit in enumerate(generate_server_response_headers(15)[:3])]
    result = await asyncio.gather(
        *[client.request(url, ratelimit, raise_error, retryable_error) for url, ratelimit, raise_error, retryable_error in data],
        return_exceptions=True
    )
    logging.info(f"{result=}")
    await client.close()
    logging.info(f"{client.host_to_requests_info=}\n")


async def main() -> None:
    await concurrent_requests_single_host_run()
    await concurrent_requests_multiple_hosts_run()
    await sequential_requests_single_host_run()

    await concurrent_requests_single_host_retry_run()


if __name__ == "__main__":
    asyncio.run(main())
