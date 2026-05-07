# gunicorn.conf.py

workers = 1
threads = 8
timeout = 120
loglevel = "info"

accesslog = "-"
errorlog  = "-"

capture_output = True
