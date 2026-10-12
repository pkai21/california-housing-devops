"""Consumer: lấy job từ Redis Stream, gọi API dự đoán cũ, ghi kết quả."""
import asyncio
import json
import logging
import os
import signal
import socket
import tempfile
import time
from pathlib import Path
from typing import Any

import httpx
import redis.asyncio as redis
from redis.exceptions import RedisError, ResponseError

from app.common import GROUP, RESULT_TTL_S, STREAM_DEAD, STREAM_JOBS, create_redis, job_key

# Gọi API cũ bằng tên Service nội bộ của K8s, không đi vòng qua NodePort
API_URL: str = os.getenv("API_URL", "http://california-housing-api:8000")
# Số job xử lý đồng thời mỗi consumer = mức "tải tối đa" đổ vào API cũ. Đây chính là chỗ điều tiết tải
CONCURRENCY: int = int(os.getenv("CONSUMER_CONCURRENCY", "4"))
MAX_DELIVERIES: int = int(os.getenv("MAX_DELIVERIES", "5"))        # quá số lần giao này thì coi là job lỗi
CLAIM_IDLE_MS: int = int(os.getenv("CLAIM_IDLE_MS", "30000"))      # job treo lâu hơn mức này thì giao lại
RECLAIM_INTERVAL_S: float = float(os.getenv("RECLAIM_INTERVAL_S", "15"))
BLOCK_MS: int = 5000

# Tên pod K8s, mỗi consumer có tên riêng để Redis theo dõi ai đang giữ job nào
CONSUMER_NAME: str = socket.gethostname()
# File "nhịp tim": livenessProbe của K8s sẽ kiểm tra file này còn được cập nhật không
HEARTBEAT_FILE: Path = Path(tempfile.gettempdir()) / "consumer_alive"

log = logging.getLogger("consumer")
Message = tuple[str, dict[str, str]]  # (id trong stream, các trường)


async def ensure_group(r: redis.Redis) -> None:
    # id="0": group đọc từ đầu stream nên job gửi trước khi consumer khởi động không bị bỏ sót
    try:
        await r.xgroup_create(STREAM_JOBS, GROUP, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):  # BUSYGROUP = group đã có, bình thường
            raise


async def finish(r: redis.Redis, msg_id: str, job_id: str, mapping: dict[str, str]) -> None:
    # Ghi kết quả + ack + xóa khỏi stream trong 1 giao dịch: không bao giờ có trạng thái
    # "đã ack mà chưa lưu kết quả". Xóa khỏi stream để XLEN luôn bằng số job còn tồn đọng
    async with r.pipeline(transaction=True) as pipe:
        await (
            pipe.hset(job_key(job_id), mapping=mapping)
            .expire(job_key(job_id), RESULT_TTL_S)
            .xack(STREAM_JOBS, GROUP, msg_id)
            .xdel(STREAM_JOBS, msg_id)
            .execute()
        )


async def drop(r: redis.Redis, msg_id: str) -> None:
    """Bỏ message hỏng (thiếu trường), nếu không nó sẽ bị giao lại mãi mãi."""
    async with r.pipeline(transaction=True) as pipe:
        await pipe.xack(STREAM_JOBS, GROUP, msg_id).xdel(STREAM_JOBS, msg_id).execute()


async def dead_letter(r: redis.Redis, msg_id: str, job_id: str, fields: dict[str, str]) -> None:
    """Job lỗi quá số lần: chuyển sang stream 'lỗi' để điều tra, không thử lại nữa."""
    async with r.pipeline(transaction=True) as pipe:
        await (
            pipe.xadd(STREAM_DEAD, fields, maxlen=1000, approximate=True)
            .hset(job_key(job_id), mapping={
                "status": "failed",
                "error": f"Đã thử quá {MAX_DELIVERIES} lần mà vẫn lỗi",
            })
            .expire(job_key(job_id), RESULT_TTL_S)
            .xack(STREAM_JOBS, GROUP, msg_id)
            .xdel(STREAM_JOBS, msg_id)
            .execute()
        )


async def delivery_count(r: redis.Redis, msg_id: str) -> int:
    """Số lần message này đã được giao (Redis tự đếm)."""
    info: list[dict[str, Any]] = await r.xpending_range(
        STREAM_JOBS, GROUP, min=msg_id, max=msg_id, count=1
    )
    return int(info[0]["times_delivered"]) if info else 1


async def handle(
    r: redis.Redis, client: httpx.AsyncClient, msg_id: str, fields: dict[str, str], redelivered: bool
) -> None:
    job_id: str | None = fields.get("job_id")
    raw: str | None = fields.get("payload")
    if not job_id or raw is None:
        log.error("Message %s thiếu trường, bỏ qua: %s", msg_id, fields)
        await drop(r, msg_id)
        return

    if redelivered and await delivery_count(r, msg_id) > MAX_DELIVERIES:
        log.error("Job %s quá %d lần thử, chuyển sang stream lỗi", job_id, MAX_DELIVERIES)
        await dead_letter(r, msg_id, job_id, fields)
        return

    try:
        resp = await client.post("/predict", json=json.loads(raw))
    except json.JSONDecodeError:
        await finish(r, msg_id, job_id, {"status": "failed", "error": "Payload không phải JSON hợp lệ"})
        return
    except httpx.HTTPError as exc:
        # Lỗi TẠM THỜI (API sập, timeout): KHÔNG ack. Job nằm lại ở trạng thái pending,
        # sau CLAIM_IDLE_MS sẽ được giao lại, đây chính là cơ chế thử lại
        log.warning("Job %s: lỗi kết nối API (%s), sẽ thử lại", job_id, type(exc).__name__)
        return

    if resp.status_code == 429 or resp.status_code >= 500:
        log.warning("Job %s: API trả %d, sẽ thử lại", job_id, resp.status_code)
        return

    if resp.status_code >= 400:
        # Lỗi VĨNH VIỄN (ví dụ 422 sai dữ liệu): thử lại bao nhiêu lần cũng vậy, ghi lỗi và ack luôn
        await finish(r, msg_id, job_id, {
            "status": "failed",
            "error": f"API từ chối (HTTP {resp.status_code}): {resp.text[:500]}",
        })
        return

    await finish(r, msg_id, job_id, {"status": "done", "result": json.dumps(resp.json())})


async def reclaim(r: redis.Redis) -> list[Message]:
    """Nhận lại job mà consumer khác đã lấy nhưng bỏ dở (chết, lỗi tạm thời) quá CLAIM_IDLE_MS."""
    res = await r.xautoclaim(
        STREAM_JOBS, GROUP, CONSUMER_NAME,
        min_idle_time=CLAIM_IDLE_MS, start_id="0-0", count=CONCURRENCY,
    )
    return list(res[1])  # res = [id tiếp theo, danh sách message, id đã bị xóa]


async def run() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            # K8s gửi SIGTERM khi tắt pod: dừng nhận job mới, xử lý nốt lô hiện tại rồi thoát
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # Windows không hỗ trợ; ở đó dùng Ctrl+C (KeyboardInterrupt)

    r = create_redis()
    limits = httpx.Limits(max_connections=CONCURRENCY)
    group_ready = False
    last_reclaim = 0.0
    log.info("Consumer %s khởi động, concurrency=%d, API=%s", CONSUMER_NAME, CONCURRENCY, API_URL)

    async with httpx.AsyncClient(base_url=API_URL, timeout=10.0, limits=limits) as client:
        while not stop.is_set():
            HEARTBEAT_FILE.touch()
            try:
                if not group_ready:
                    await ensure_group(r)
                    group_ready = True

                if time.monotonic() - last_reclaim >= RECLAIM_INTERVAL_S:
                    last_reclaim = time.monotonic()
                    claimed = await reclaim(r)
                    if claimed:
                        await asyncio.gather(*(handle(r, client, mid, f, True) for mid, f in claimed))

                # Đọc tối đa CONCURRENCY job rồi xử lý song song. Chờ tối đa BLOCK_MS nếu hàng đợi rỗng
                resp = await r.xreadgroup(
                    GROUP, CONSUMER_NAME, {STREAM_JOBS: ">"}, count=CONCURRENCY, block=BLOCK_MS
                )
                for _stream, messages in resp:
                    await asyncio.gather(*(handle(r, client, mid, f, False) for mid, f in messages))

            except ResponseError as exc:
                if "NOGROUP" in str(exc):
                    group_ready = False  # Redis mất group (ví dụ bị xóa dữ liệu): tạo lại ở vòng sau
                else:
                    log.exception("Lỗi lệnh Redis")
                    await asyncio.sleep(2)
            except (RedisError, OSError):
                # Redis đang restart/mất kết nối: chờ rồi thử lại, redis-py tự nối lại
                log.warning("Mất kết nối Redis, thử lại sau 2 giây")
                await asyncio.sleep(2)
            except Exception:
                # Lỗi không lường trước: ghi log và chờ, tránh vòng lặp quay vô hạn ngốn CPU
                log.exception("Lỗi không xác định")
                await asyncio.sleep(2)

    await r.aclose()
    log.info("Consumer đã dừng gọn gàng")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass