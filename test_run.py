"""Headless harness for verification — port 8889 so it doesn't collide."""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import uvicorn
_orig_Config = uvicorn.Config


def _patched_Config(*a, **kw):
    kw['port'] = 8889
    return _orig_Config(*a, **kw)


uvicorn.Config = _patched_Config

import webbrowser
webbrowser.open = lambda *a, **kw: None

import main

if __name__ == '__main__':
    asyncio.run(main.main())
