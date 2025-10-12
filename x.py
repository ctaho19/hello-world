import asyncio
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import ssl  # Make sure to import the ssl library
import aiohttp
import pandas as pd

from config_pipeline import ConfigPipeline
from connectors.ca_certs import CL_CERT_FILE
from connectors.exchange.oauth_token import refresh
from settings import Env

_old_record_factory = logging.getLogRecordFactory()


def _record_factory(*args, **kwargs):
    record = _old_record_factory(*args, **kwargs)
    try:
        current_task = asyncio.current_task()
    except Exception:
        current_task = None
    if current_task is not None:
        try:
            record.task_name = current_task.get_name()
        except Exception:
            record.task_name = f"task-{id(current_task)}"
        record.task_id = id(current_task)
    else:
        record.task_name = "sync"
        record.task_id = 0
    return record


logging.setLogRecordFactory(_record_factory)
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(levelname)s][%(asctime)s][%(name)s][task=%(task_name)s][tid=%(task_id)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

logger = logging.getLogger(__name__)


def run(
    env: Env,
    *,
    load_env: Optional[Env] = None,
    load: bool = True,
    dq_actions: bool = True,
) -> None:
    pipeline = PLAutomatedMonitoringCTRL1077188(env, load_env=load_env)
    pipeline.configure.from_filename(str(Path(__file__).parent / "config.yaml"))
    return pipeline.run(load=load, dq_actions=dq_actions)


def backfill(
    env: Env,
    backfill_date: date,
    backfill_args: dict[str, str],
    *,
    load_env: Optional[Env] = None,
    load: bool = True,
    dq_actions: bool = True,
) -> dict[str, pd.DataFrame]:
    pipeline = PLAutomatedMonitoringCTRL1077188(
        env, load_env=load_env, snap_dt=backfill_date
    )
    pipeline.configure.from_filename(str(Path(__file__).parent / "config.yaml"))
    return pipeline.run(load=load, dq_actions=dq_actions)


class PLAutomatedMonitoringCTRL1077188(ConfigPipeline):
    def __init__(self, env: Env, load_env: Optional[Env] = None) -> None:
        super().__init__(env, load_env=load_env)
        self.api_url = "https://self.env.exchange.exchange.url3/internal-operations/cloud-services/"
        self.api_token = refresh(
            client_id=self.env.exchange.client_id,
            client_secret=self.env.exchange.client_secret,
            exchange_url=self.env.exchange.exchange_url,
        )
        self._page_delay_seconds = 0.03
        self.max_retries = 3
        self._enable_trace = True  # toggle for deep aiohttp timings
        self.session: Optional[aiohttp.ClientSession] = None
        # Trace configuration guidance follows aiohttp recommendations [docs.aiohttp.org](https://docs.aiohttp.org/en/stable/tracing_reference.html)
        # and community tracing examples [gist.github.com](https://gist.github.com/anthonynsimon/375fa15b729cd0c2ef6a851ed19c468a).

    def _build_trace_config(self, name: str) -> Optional[aiohttp.TraceConfig]:
        if not self._enable_trace:
            return None
        trace_config = aiohttp.TraceConfig()

        async def on_request_start(session, trace_config_ctx, params):  # type: ignore
            logger.debug(f"[{name}] trace_start url={params.url}")

        trace_config.on_request_start.append(on_request_start)

        async def on_request_end(session, trace_config_ctx, params):
            if getattr(params, "response", None) is not None:
                logger.debug(
                    f"[{name}] trace_end url={params.url} status={params.response.status}"
                )
            else:
                logger.debug(f"[{name}] trace_end url={params.url} status=None")

        trace_config.on_request_end.append(on_request_end)
        return trace_config

    async def _fetch_paginated_resource(
        self, name: str, payload: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        loop = asyncio.get_running_loop()
        task_start_wall = datetime.now(timezone.utc).isoformat()
        logger.debug(
            f"[{name}] fetch_start payload_keys={list(payload.keys())} start_ts={task_start_wall}"
        )
        all_resources_for_payload: List[Dict[str, Any]] = []
        next_record_key: Optional[str] = None
        headers = {
            "Accept": "application/json;v=1.0",
            "Authorization": self.api_token,
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip, deflate",
        }
        page_index = 0
        start_payload_ts = loop.time()
        progress_interval = 10
        while True:
            current_payload = payload.copy()
            if next_record_key:
                current_payload["nextRecordKey"] = next_record_key
            retry_delay = 0.5
            page_number = page_index + 1
            for attempt in range(self.max_retries):
                attempt_number = attempt + 1
                page_start_monotonic = loop.time()
                request_start_wall = datetime.now(timezone.utc).isoformat()
                logger.debug(
                    f"[{name}] page={page_number} attempt={attempt_number} request_start_ts={request_start_wall} "
                    f"next_key_present={bool(next_record_key)} accumulated_resources={len(all_resources_for_payload)}"
                )
                try:
                    async with self.session.post(  # type: ignore[union-attr]
                        self.api_url,
                        headers=headers,
                        json=current_payload,
                    ) as response:
                        status = response.status
                        response_end_wall = datetime.now(timezone.utc).isoformat()
                        if status != 200:
                            response_text = await response.text()
                            logger.debug(
                                f"[{name}] page={page_number} attempt={attempt_number} non_200 status={status} "
                                f"response_len={len(response_text.encode(response.charset or 'utf-8'))} "
                                f"end_ts={response_end_wall}"
                            )
                            if status in (429, 503) and attempt < self.max_retries - 1:
                                logger.warning(
                                    f"[{name}] page={page_number} attempt={attempt_number} status={status} "
                                    f"retry_in={retry_delay:.2f}s reason=throttled_or_service_unavailable"
                                )
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                                continue
                            raise aiohttp.ClientResponseError(
                                response.request_info,
                                response.history,
                                status=status,
                                message=response_text,
                            )
                        body_bytes = await response.read()
                        response_size_bytes = len(body_bytes)
                        encoding = response.charset or "utf-8"
                        if body_bytes:
                            try:
                                decoded_json = json.loads(body_bytes.decode(encoding))
                            except json.JSONDecodeError as decode_error:
                                logger.error(
                                    f"[{name}] page={page_number} attempt={attempt_number} json_decode_error "
                                    f"bytes={response_size_bytes} end_ts={response_end_wall}",
                                    exc_info=(type(decode_error), decode_error, decode_error.__traceback__),
                                )
                                raise
                        else:
                            decoded_json = {}
                        page_elapsed = loop.time() - page_start_monotonic
                        logger.debug(
                            f"[{name}] page={page_number} attempt={attempt_number} request_end_ts={response_end_wall} "
                            f"status={status} elapsed={page_elapsed:.4f}s response_bytes={response_size_bytes}"
                        )
                        data = decoded_json
                        resources = data.get("resourceConfigurations", []) or []
                        for resource in resources:
                            resource["source_payload"] = name
                        all_resources_for_payload.extend(resources)
                        next_record_key = data.get("nextRecordKey")
                        page_index += 1
                        logger.debug(
                            f"[{name}] page={page_index} collected_resources={len(resources)} "
                            f"total_resources={len(all_resources_for_payload)} next_key_present={bool(next_record_key)}"
                        )
                        if page_index % progress_interval == 0:
                            total_elapsed = loop.time() - start_payload_ts
                            avg_page_time = total_elapsed / page_index if page_index else 0.0
                            estimated_remaining = (
                                avg_page_time if next_record_key else 0.0
                            )
                            logger.debug(
                                f"[{name}] progress pages={page_index} total_resources={len(all_resources_for_payload)} "
                                f"elapsed={total_elapsed:.2f}s avg_page={avg_page_time:.2f}s "
                                f"est_time_remaining≈{estimated_remaining:.2f}s"
                            )
                    if not next_record_key:
                        total_elapsed = loop.time() - start_payload_ts
                        avg_page = total_elapsed / page_index if page_index else 0.0
                        rate = (
                            len(all_resources_for_payload) / total_elapsed
                            if total_elapsed
                            else 0.0
                        )
                        logger.info(
                            f"[{name}] fetch_complete pages={page_index} total_resources={len(all_resources_for_payload)} "
                            f"total_elapsed={total_elapsed:.2f}s avg_page={avg_page:.2f}s rate={rate:.2f} res/s"
                        )
                        return all_resources_for_payload
                    logger.debug(
                        f"[{name}] page={page_index} sleeping={self._page_delay_seconds}s before_next_page"
                    )
                    await asyncio.sleep(self._page_delay_seconds)
                    break
                except aiohttp.ClientError as client_error:
                    page_elapsed = loop.time() - page_start_monotonic
                    if attempt < self.max_retries - 1:
                        logger.warning(
                            f"[{name}] page={page_number} attempt={attempt_number} client_error={client_error} "
                            f"elapsed={page_elapsed:.4f}s retry_in={retry_delay:.2f}s"
                        )
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        logger.error(
                            f"[{name}] page={page_number} attempt={attempt_number} client_error_final={client_error} "
                            f"elapsed={page_elapsed:.4f}s"
                        )
                        raise RuntimeError(
                            f"Failed to fetch {name} resources from API after {self.max_retries} attempts"
                        ) from client_error

    async def extract_api_certificates_concurrently(self) -> pd.DataFrame:
        payloads = {
            "Amazon": {
                "searchParameters": [
                    {
                        "ResourceType": "AWS::ACM::Certificate",
                        "configurationItems": [
                            {
                                "configurationName": "Issuer",
                                "configurationValue": "Amazon",
                            },
                            {
                                "configurationName": "renewalSummary.renewalStatus",
                                "configurationValue": "SUCCESS",
                            },
                        ],
                        "supplementaryConfigurationItems": [
                            {
                                "supplementaryConfigurationName": "Tags",
                                "supplementaryConfigurationValue": "I.T...ASVACAUTOMATION....1",
                            },
                        ],
                    },
                ],
            },
            "DigiCert": {
                "searchParameters": [
                    {
                        "ResourceType": "AWS::ACM::Certificate",
                        "configurationItems": [
                            {
                                "configurationName": "Issuer",
                                "configurationValue": "DigiCert Inc",
                            },
                            {"configurationName": "type", "configurationValue": "IMPORTED"},
                        ],
                    },
                ],
            },
            "PCA": {
                "searchParameters": [
                    {
                        "ResourceType": "AWS::ACM::Certificate",
                        "configurationItems": [
                            {
                                "configurationName": "Issuer",
                                "configurationValue": "Capital One AWS",
                            },
                            {"configurationName": "type", "configurationValue": "PRIVATE"},
                        ],
                    },
                ],
            },
        }
        ssl_context = ssl.create_default_context(cafile=CL_CERT_FILE)
        timeout = aiohttp.ClientTimeout(total=40, sock_read=25, connect=10)
        connector = aiohttp.TCPConnector(
            ssl=ssl_context,
            limit=12,
            enable_cleanup_closed=True,
            force_close=False,
        )
        trace_configs = [self._build_trace_config(name) for name in payloads]
        trace_configs = [config for config in trace_configs if config]
        overall_start = asyncio.get_running_loop().time()
        logger.info(
            "[AllPayloads] concurrency_start payload_count=%d",
            len(payloads),
        )
        task_metrics: Dict[str, Dict[str, Any]] = {}
        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector, trace_configs=trace_configs
        ) as session:
            self.session = session

            async def payload_task(task_name: str, task_payload: Dict[str, Any]):
                current_task = asyncio.current_task()
                if current_task and hasattr(current_task, "set_name"):
                    try:
                        current_task.set_name(f"payload-{task_name}")
                    except Exception:
                        pass
                loop = asyncio.get_running_loop()
                task_start = loop.time()
                task_start_wall = datetime.now(timezone.utc).isoformat()
                logger.info(
                    f"[{task_name}] task_start payload_keys={list(task_payload.keys())} start_ts={task_start_wall}"
                )
                try:
                    resources = await self._fetch_paginated_resource(task_name, task_payload)
                    task_elapsed = loop.time() - task_start
                    task_metrics[task_name] = {
                        "elapsed": task_elapsed,
                        "count": len(resources),
                        "success": True,
                    }
                    logger.info(
                        f"[{task_name}] task_complete elapsed={task_elapsed:.2f}s resources={len(resources)} "
                        f"rate={len(resources)/task_elapsed if task_elapsed else 0.0:.2f} res/s"
                    )
                    return resources
                except Exception as task_error:
                    task_elapsed = loop.time() - task_start
                    task_metrics[task_name] = {
                        "elapsed": task_elapsed,
                        "count": 0,
                        "success": False,
                    }
                    logger.exception(
                        f"[{task_name}] task_exception elapsed={task_elapsed:.2f}s",
                        exc_info=(type(task_error), task_error, task_error.__traceback__),
                    )
                    raise

            payload_order = list(payloads.keys())
            tasks = [
                payload_task(payload_name, payload_definition)
                for payload_name, payload_definition in payloads.items()
            ]
            results_list = await asyncio.gather(*tasks, return_exceptions=True)
            all_resources: List[Dict[str, Any]] = []
            for payload_name, result in zip(payload_order, results_list):
                if isinstance(result, Exception):
                    logger.error(
                        f"[{payload_name}] gather_exception",
                        exc_info=(type(result), result, result.__traceback__),
                    )
                    raise result
                all_resources.extend(result)
            overall_elapsed = asyncio.get_running_loop().time() - overall_start
            total_resources = len(all_resources)
            logger.info(
                f"[AllPayloads] concurrency_complete total_resources={total_resources} "
                f"total_elapsed={overall_elapsed:.2f}s overall_rate={total_resources/overall_elapsed if overall_elapsed else 0.0:.2f} res/s"
            )
            if task_metrics:
                total_elapsed_sum = sum(metric["elapsed"] for metric in task_metrics.values())
                avg_task_time = (
                    total_elapsed_sum / len(task_metrics) if task_metrics else 0.0
                )
                for payload_name, metric in task_metrics.items():
                    status = "success" if metric.get("success") else "failure"
                    elapsed = metric.get("elapsed", 0.0)
                    count = metric.get("count", 0)
                    rate = count / elapsed if elapsed else 0.0
                    logger.debug(
                        f"[{payload_name}] task_metrics status={status} elapsed={elapsed:.2f}s "
                        f"resources={count} rate={rate:.2f} res/s"
                    )
                logger.debug(
                    f"[AllPayloads] concurrency_metrics avg_task_time={avg_task_time:.2f}s task_count={len(task_metrics)}"
                )
            return pd.DataFrame(all_resources)

    def extract(self) -> dict[str, pd.DataFrame]:
        logger.info("[AllPayloads] extract_start message=Extracting data from CloudRadar API concurrently")
        api_df = asyncio.run(self.extract_api_certificates_concurrently())
        extracted_dfs = {"api_certificates": api_df}
        logger.info(
            "[AllPayloads] extract_complete dataframe_shapes=%s",
            {key: value.shape for key, value in extracted_dfs.items()},
        )
        return extracted_dfs