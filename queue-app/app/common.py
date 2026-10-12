"""Cấu hình và hằng số dùng chung cho gateway và consumer."""
import os

import redis.asyncio as redis

REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
# Mật khẩu lấy từ Secret của K8s qua biến môi trường, không ghi cứng trong code
REDIS_PASSWORD: str | None = os.getenv("REDIS_PASSWORD") or None

STREAM_JOBS: str = "jobs"          # hàng đợi chính
STREAM_DEAD: str = "jobs:dead"     # nơi chứa job lỗi quá số lần thử (để điều tra sau)
GROUP: str = "workers"             # consumer group: các consumer chia nhau job, mỗi job chỉ 1 người nhận

PENDING_TTL_S: int = 86400         # job chờ tối đa 24h trước khi hết hạn
RESULT_TTL_S: int = 3600           # kết quả giữ 1h rồi tự xóa, tránh Redis phình RAM


def job_key(job_id: str) -> str:
    """Key lưu trạng thái và kết quả của 1 job."""
    return f"job:{job_id}"


def create_redis() -> redis.Redis:
    # socket_timeout (15s) PHẢI lớn hơn thời gian chờ BLOCK của consumer (5s),
    # nếu không lệnh chờ job sẽ bị ngắt oan và báo lỗi timeout
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_timeout=15,
        socket_connect_timeout=5,
        health_check_interval=30,
    )