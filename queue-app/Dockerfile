FROM python:3.13-slim

WORKDIR /srv

# PYTHONUNBUFFERED: log hiện ngay trong `kubectl logs`, không bị đệm lại
# PYTHONDONTWRITEBYTECODE: không sinh file .pyc, image nhẹ hơn
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Copy requirements.txt trước code: sửa code không làm cài lại thư viện (tận dụng cache layer)
COPY requirements.txt .
RUN python -m pip install --no-cache-dir --upgrade pip && \
    python -m pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# Chạy bằng user thường (số, để K8s kiểm tra được runAsNonRoot)
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin appuser
USER 10001

EXPOSE 8080

# Mặc định chạy gateway; ở K8s, consumer sẽ ghi đè bằng: python -m app.consumer
CMD ["python", "-m", "uvicorn", "app.gateway:app", "--host", "0.0.0.0", "--port", "8080"]