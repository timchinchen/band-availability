import os

# Keep memory bounded on Render free instances by recycling workers.
# These settings are conservative and aim for stability over peak throughput.
workers = int(os.getenv("WEB_CONCURRENCY", "1"))
threads = int(os.getenv("GUNICORN_THREADS", "2"))
timeout = int(os.getenv("GUNICORN_TIMEOUT", "120"))
graceful_timeout = int(os.getenv("GUNICORN_GRACEFUL_TIMEOUT", "30"))
keepalive = int(os.getenv("GUNICORN_KEEPALIVE", "5"))

# Restart worker after serving a number of requests to mitigate memory growth.
max_requests = int(os.getenv("GUNICORN_MAX_REQUESTS", "80"))
max_requests_jitter = int(os.getenv("GUNICORN_MAX_REQUESTS_JITTER", "20"))
