"""
Convenience entry-point so the service can be started with:

    python main.py

or via uvicorn directly:

    uvicorn src.api.app:app --reload
"""

import uvicorn

from src.api.core.config import get_settings

if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "src.api.app:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level="debug" if settings.DEBUG else "info",
    )
