# Gunicorn 配置文件
import multiprocessing
import os

# 服務器配置
bind = "0.0.0.0:5000"
backlog = 2048

# Worker 配置
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "gevent"  # 使用 gevent 以支援高併發
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50

# 超時設定
timeout = 30
keepalive = 2

# 日誌配置
accesslog = "-"
errorlog = "-"
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# 進程管理
preload_app = True
daemon = False
pidfile = "/tmp/gunicorn.pid"
user = None
group = None

# 重啟配置
max_requests = 1000
max_requests_jitter = 100

# 性能優化
worker_tmp_dir = "/dev/shm"  # 使用共享記憶體
