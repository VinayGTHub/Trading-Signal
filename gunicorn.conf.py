# gunicorn.conf.py
workers = 1
threads = 8
timeout = 120
loglevel = "info"
accesslog = "-"   # ✅ This is CRITICAL — prints access logs to stdout
errorlog  = "-"
capture_output = True  # ✅ Captures print() and logging() output

def on_starting(server):
    from app import ensure_flusher
    ensure_flusher()