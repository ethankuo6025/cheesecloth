from typing import Any
from time import sleep, monotonic
from arelle.utils.PluginHooks import PluginHooks
from urllib.parse import urlparse
import threading

SEC_HOSTS = {"www.sec.gov", "data.sec.gov"}

_lock = threading.Lock()
_NEXT_ALLOWED = 0.0

MIN_INTERVAL = .125 # 8 requests/sec
_COUNT = 0

def call_count():
    with _lock:
        return f"api calls: {_COUNT}"

def _is_sec_http_url(url: str) -> bool:
    if not url:
        return False
    if not (url.startswith("http://") or url.startswith("https://")):
        return False
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return host in SEC_HOSTS

def wait(url: str):
    if _is_sec_http_url(url):
        global _NEXT_ALLOWED
        global _COUNT
        with _lock:
            _COUNT += 1
            now = monotonic()
            if now < _NEXT_ALLOWED:
                sleep(_NEXT_ALLOWED - now)
            _NEXT_ALLOWED = monotonic() + MIN_INTERVAL

    return True

class RateLimiter(PluginHooks):
    @staticmethod
    def TransformURLOptions(
        cntlr, 
        url: str | None, 
        base: str | None, 
        *args: Any, 
        **kwargs: Any
    ) -> tuple[str | None, bool]:
        
        if not url:
            return (None, False)

        wait(url)
        
        return(url, False)

__pluginInfo__ = {
    "name": "Rate Limiter Plugin",
    "version": "1.0.0",
    "author": "Ethan Kuo",
    "WebCache.TransformURL": RateLimiter.TransformURLOptions,
}