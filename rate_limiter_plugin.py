from optparse import OptionParser
from typing import Any
from time import sleep, monotonic
from arelle.utils.PluginHooks import PluginHooks

_NEXT_ALLOWED = 0.0

MIN_INTERVAL = .125 # 8 requests/sec

class RateLimiter(PluginHooks):
    @staticmethod
    def TransformURLOptions(
        cntlr, 
        url: str | None, 
        base: str | None, 
        *args: Any, 
        **kwargs: Any
    ) -> tuple[str | None, bool]:
        
        global _NEXT_ALLOWED

        if not url:
            return (None, False)
        
        now = monotonic()
        if now < _NEXT_ALLOWED:
            sleep(_NEXT_ALLOWED-now)
        
        _NEXT_ALLOWED = now + MIN_INTERVAL

        return(url, False)

__pluginInfo__ = {
    'name': 'Rate Limiter Plugin',
    'version': '1.0.0',
    'author': 'Ethan Kuo',
    'WebCache.TransformURL': RateLimiter.TransformURLOptions,
}