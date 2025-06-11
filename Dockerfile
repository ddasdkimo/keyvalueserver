FROM python:3.11-slim

WORKDIR /app

# 安裝系統依賴
RUN apt-get update && apt-get install -y \
    gcc \
    libevent-dev \
    && rm -rf /var/lib/apt/lists/*

# 創建非 root 用戶
RUN useradd --create-home --shell /bin/bash app

# 複製 requirements 並安裝 Python 依賴
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 複製應用程式代碼
COPY . .

# 設定權限
RUN chown -R app:app /app
USER app

# 暴露端口
EXPOSE 5000

# 健康檢查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 health_check.py || exit 1

# 啟動命令 - 使用 Gunicorn
CMD ["gunicorn", "--config", "gunicorn.conf.py", "app_optimized:app"]
