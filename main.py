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
    NO_LIMITS = "no_limits"


class HostRequestsLimiter:
    def __init__(self, host: str, stage: Stage = Stage.SEND_ONE_REQUEST) -> None:
        self.host = host
        self.requests_sent_in_time_window = 0
        self._incoming_requests = 0
        self.ratelimit: RateLimit | None = None
        self.stage: Stage = stage
        self._condition = asyncio.Condition()

    async def wait_until_notified(self) -> None:
        async with self._condition:
            self._incoming_requests += 1
            await self._condition.wait()
        self._incoming_requests -= 1

    def next_stage(self) -> None:
        now = datetime.now(UTC)
        if self.stage == Stage.SEND_ONE_REQUEST:
            self.stage = Stage.SENDING_ONE_REQUEST
        elif self.stage == Stage.SENDING_ONE_REQUEST and self.ratelimit:
            if (
                now >= self.ratelimit.reset or
                self.requests_sent_in_time_window == self.ratelimit.limit
            ):
                self.stage = Stage.WAITING_FOR_RESET
            elif now < self.ratelimit.reset and self._incoming_requests >= 1:
                self.stage = Stage.SEND_CONCURRENT_REQUESTS
            elif now < self.ratelimit.reset and self._incoming_requests == 0:
                self.stage = Stage.SEND_ONE_REQUEST
        elif self.stage == Stage.SENDING_ONE_REQUEST:
            self.stage = Stage.NO_LIMITS
        elif self.stage == Stage.SENDING_CONCURRENT_REQUESTS and self.ratelimit and (
            now >= self.ratelimit.reset or
            self.requests_sent_in_time_window == self.ratelimit.limit
        ):
            self.stage = Stage.WAITING_FOR_RESET

    async def notify(self, concurrent_requests: int) -> None:
        if self.stage == Stage.NO_LIMITS and self._incoming_requests > 0:
            async with self._condition:
                logging.info(f"{self.host} | notifying all tasks...")
                self._condition.notify_all()
        elif self.stage == Stage.SEND_ONE_REQUEST and self._incoming_requests > 0:
            async with self._condition:
                logging.info(f"{self.host} | notifying 1 task to fetch ratelimit...")
                self._condition.notify()
        elif self.stage == Stage.SEND_CONCURRENT_REQUESTS and self.ratelimit:
            async with self._condition:
                logging.info(
                    f"{self.host} | notifying {self.ratelimit.limit - 1} tasks, "
                    f"reset={self.ratelimit.reset.strftime(datefmt)}"
                )
                self.stage = Stage.SENDING_CONCURRENT_REQUESTS
                self._condition.notify(self.ratelimit.limit - 1)
        elif self.stage == Stage.SENDING_CONCURRENT_REQUESTS and self.ratelimit and (
            self.requests_sent_in_time_window + concurrent_requests
            < self.ratelimit.limit
        ) and self._incoming_requests > 0:
            async with self._condition:
                tasks_to_notify = (
                    self.ratelimit.limit -
                    (self.requests_sent_in_time_window + concurrent_requests)
                )
                logging.info(
                    f"{self.host} | notifying additional {tasks_to_notify} tasks, "
                    f"reset={self.ratelimit.reset.strftime(datefmt)}"
                )
                self._condition.notify(tasks_to_notify)
        elif (
            self.stage == Stage.WAITING_FOR_RESET and self.ratelimit and
            datetime.now(UTC) >= self.ratelimit.reset
        ):
            self.stage = Stage.SEND_ONE_REQUEST
            self.requests_sent_in_time_window = 0


class HttpClient:
    def __init__(self) -> None:
        self.host_to_requests_limiter: dict[str, HostRequestsLimiter] = {}
        self.run_bg_task = True
        self.bg_task = asyncio.create_task(self._control_tasks_notification())
        self.concurrent_requests: dict[str, int] = Counter()

    def _get_requests_limiter(self, host: str) -> HostRequestsLimiter:
        if not (requests_limiter := self.host_to_requests_limiter.get(host)):
            self.host_to_requests_limiter[host] = HostRequestsLimiter(host)
            requests_limiter = self.host_to_requests_limiter[host]
        return requests_limiter

    async def _control_tasks_notification(self) -> None:
        while self.run_bg_task:
            for host, requests_limiter in self.host_to_requests_limiter.items():
                await requests_limiter.notify(self.concurrent_requests[host])
            await asyncio.sleep(0.3)

    @retry(
        stop=stop_after_attempt(2+1),
        retry=retry_if_exception_type(ZeroDivisionError),
        wait=wait_fixed(0.5),
        before=before_log(logger, logging.DEBUG)
    )
    async def request(self, url: str, ratelimit: RateLimit | None = None, raise_error: bool = False, retryable_error: bool = True) -> None:
        host, id_ = url.split(" ")
        requests_limiter = self._get_requests_limiter(host)
        
        logging.info(f"Task of requesting {url} is going to wait...")
        await requests_limiter.wait_until_notified()
        self.concurrent_requests[host] += 1

        requests_limiter.next_stage()

        try:
            await self._send_request(url, ratelimit, raise_error, retryable_error)
        except Exception as error:
            logging.error(f"{url=}, {error=}")
            raise
        finally:
            self.concurrent_requests[host] -= 1
            requests_limiter.requests_sent_in_time_window += 1
            requests_limiter.next_stage()

    async def _send_request(self, url: str, ratelimit: RateLimit | None = None, raise_error: bool = False, retryable_error: bool = True) -> None:
        host, _ = url.split(" ")
        requests_limiter = self._get_requests_limiter(host)

        logging.info(f"Sending request to {url}, {ratelimit=}...")
        await asyncio.sleep(1)
        if requests_limiter.stage == Stage.SENDING_ONE_REQUEST:
            if ratelimit is None:
                # Headers don't contain rate limit info or its format is 
                # invalid/unexpected.
                requests_limiter.ratelimit = None
            else:
                requests_limiter.ratelimit = ratelimit
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
    logging.info(f"{client.host_to_requests_limiter=}\n")


async def concurrent_requests_multiple_hosts_run() -> None:
    logging.info("Concurrent Requests Multiple Hosts Run")
    client = HttpClient()
    await asyncio.gather(
        *[client.request(f"host-b {i+1}", ratelimit) for i, ratelimit in enumerate(generate_server_response_headers(15))],
        *[client.request(f"host-c {i+1}", ratelimit) for i, ratelimit in enumerate(generate_server_response_headers(10))]
    )
    await client.close()
    logging.info(f"{client.host_to_requests_limiter=}\n")


async def sequential_requests_single_host_run() -> None:
    logging.info("Sequential Requests Single Host Run")
    client = HttpClient()
    for i, ratelimit in enumerate(generate_server_response_headers(requests_count=6, ratelimit_limit=2)):
        await client.request(f"host-d {i+1}", ratelimit)
    await client.close()
    logging.info(f"{client.host_to_requests_limiter=}\n")


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
    logging.info(f"{client.host_to_requests_limiter=}\n")


async def concurrent_requests_without_limits_run() -> None:
    logging.info("Concurrent Requests Without Limits Run")
    client = HttpClient()
    await asyncio.gather(
        *[client.request(f"host-a {i+1}") for i in range(15)],
    )
    await client.close()
    logging.info(f"{client.host_to_requests_limiter=}\n")


async def sequential_requests_without_limits_run() -> None:
    logging.info("Sequential Requests Without Limits Run")
    client = HttpClient()
    for i in range(15):
        await client.request(f"host-a {i+1}")
    await client.close()
    logging.info(f"{client.host_to_requests_limiter=}\n")


async def main() -> None:
    await concurrent_requests_single_host_run()
    await concurrent_requests_multiple_hosts_run()
    await sequential_requests_single_host_run()

    await concurrent_requests_single_host_retry_run()
    await concurrent_requests_without_limits_run()
    await sequential_requests_without_limits_run()


if __name__ == "__main__":
    asyncio.run(main())
