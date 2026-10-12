"""Cổng nhận job: nhận request, đẩy vào hàng đợi, trả kết quả khi client hỏi."""
import json
import os
import uuid
from contextlib import asynccontextmanager
from typing import Annotated, Any, AsyncIterator

import redis.asyncio as redis
from fastapi import Body, FastAPI, HTTPException, Request
from fastapi import Path as PathParam
from pydantic import Field
from redis.exceptions import RedisError

from app.common import PENDING_TTL_S, STREAM_JOBS, create_redis, job_key

# Giới hạn độ dài hàng đợi: đây mới là "van an toàn" thật sự khi quá tải.
# Mỗi job chỉ vài trăm byte nên 10.000 job vẫn chỉ tốn vài MB, rất xa mức 200mb của Redis
MAX_QUEUE_LEN: int = int(os.getenv("MAX_QUEUE_LEN", "10000"))

# Chỉ kiểm tra "là số hợp lệ" ở cổng. Kiểm tra khoảng giá trị do API cũ làm (nguồn sự thật duy nhất),
# lỗi 422 của API sẽ được consumer ghi vào kết quả job. Đổi lại: lỗi nhập liệu biết chậm hơn một nhịp
Payload = Annotated[
    dict[str, Annotated[float, Field(allow_inf_nan=False)]],
    Body(min_length=1, max_length=32),
]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.redis = create_redis()
    yield
    await app.state.redis.aclose()


app = FastAPI(title="Job Gateway", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    # Dùng cho livenessProbe: KHÔNG gọi Redis. Redis chết mà gateway bị restart
    # theo thì chỉ thêm hỗn loạn, không giải quyết được gì
    return {"status": "alive"}


@app.get("/ready")
async def ready(request: Request) -> dict[str, str]:
    # Dùng cho readinessProbe: Redis không trả lời thì tạm rút gateway khỏi Service
    try:
        await request.app.state.redis.ping()
    except RedisError as exc:
        raise HTTPException(status_code=503, detail="Redis chưa sẵn sàng") from exc
    return {"status": "ready"}


@app.post("/jobs", status_code=202)
async def submit_job(payload: Payload, request: Request) -> dict[str, str]:
    r: redis.Redis = request.app.state.redis
    try:
        # Hàng đợi đầy thì từ chối ngay (429) thay vì để Redis đầy RAM.
        # Kiểm tra và ghi không nguyên tử nên có thể vượt vài job, chấp nhận được
        if await r.xlen(STREAM_JOBS) >= MAX_QUEUE_LEN:
            raise HTTPException(
                status_code=429,
                detail="Hàng đợi đang đầy, vui lòng thử lại sau",
                headers={"Retry-After": "5"},
            )

        job_id: str = uuid.uuid4().hex
        # Tạo trạng thái "pending" và đẩy job vào stream trong CÙNG 1 giao dịch.
        # Nếu làm 2 bước rời, consumer có thể xử lý xong trước khi trạng thái "pending" được ghi,
        # và "pending" sẽ ghi đè lên "done"
        async with r.pipeline(transaction=True) as pipe:
            await (
                pipe.hset(job_key(job_id), mapping={"status": "pending"})
                .expire(job_key(job_id), PENDING_TTL_S)
                .xadd(STREAM_JOBS, {"job_id": job_id, "payload": json.dumps(payload)})
                .execute()
            )
    except RedisError as exc:
        # Báo 503 rõ ràng thay vì để client nhận lỗi 500 mơ hồ
        raise HTTPException(status_code=503, detail="Hàng đợi tạm thời không khả dụng") from exc
    return {"job_id": job_id}


@app.get("/jobs/{job_id}")
async def get_job(
    job_id: Annotated[str, PathParam(pattern=r"^[0-9a-f]{32}$")],
    request: Request,
) -> dict[str, Any]:
    # pattern ở trên chặn client dò các key khác trong Redis bằng job_id bậy
    try:
        data: dict[str, str] = await request.app.state.redis.hgetall(job_key(job_id))
    except RedisError as exc:
        raise HTTPException(status_code=503, detail="Hàng đợi tạm thời không khả dụng") from exc

    # Khác Celery: job không tồn tại (hoặc đã hết hạn) được phân biệt rõ bằng 404
    if not data:
        raise HTTPException(status_code=404, detail="Không tìm thấy job (sai mã hoặc đã hết hạn)")

    body: dict[str, Any] = {"job_id": job_id, "status": data["status"]}
    if "result" in data:
        body["result"] = json.loads(data["result"])
    if "error" in data:
        body["error"] = data["error"]
    return body