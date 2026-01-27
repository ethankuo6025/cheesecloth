from optparse import OptionParser
from typing import Any
from time import sleep, monotonic
from arelle.utils.PluginHooks import PluginHooks
import threading

_lock = threading.Lock()
_NEXT_ALLOWED = 0.0
MIN_INTERVAL = .125 # 8 requests/sec
_COUNT = 0

def call_count():
    return f"api calls: {_COUNT}"

def wait():
    global _NEXT_ALLOWED
    global _COUNT
    _COUNT += 1
    with _lock:
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

        wait()
        
        return(url, False)

__pluginInfo__ = {
    "name": "Rate Limiter Plugin",
    "version": "1.0.0",
    "author": "Ethan Kuo",
    "WebCache.TransformURL": RateLimiter.TransformURLOptions,
}