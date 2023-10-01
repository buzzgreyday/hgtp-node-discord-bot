import asyncio
import logging
import threading
from database import engine
from models import SQLBase
import uvicorn
from database import api as database_api


def run_uvicorn():
    host = "127.0.0.1"
    port = 8000
    log_level = "warning"
    logging.getLogger(__name__).info(f"create_db.py - Uvicorn running on {host}:{port}")
    uvicorn.run(
        database_api,
        host=host,
        port=port,
        log_level=log_level,
        log_config=f"assets/data/logs/bot/uvicorn.ini",
    )


async def create_db():
    async with engine.begin() as conn:
        await conn.run_sync(SQLBase.metadata.drop_all)
        await conn.run_sync(SQLBase.metadata.create_all)
        print("database created!")

    await engine.dispose()


print("starting process...")
uvicorn_thread = threading.Thread(target=run_uvicorn)
uvicorn_thread.start()
asyncio.run(create_db())
