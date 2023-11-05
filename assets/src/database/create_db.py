import asyncio
import logging
import os
import threading
from fastapi import FastAPI
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine
from models import SQLBase
import uvicorn
from dotenv import load_dotenv

load_dotenv()

engine = create_async_engine(
    url=os.getenv("DB_URL"),
    future=True,
    # echo=True,
    poolclass=NullPool,
    connect_args={"server_settings": {"statement_timeout": "9000"}},
)

database_api = FastAPI(
    title="Hypergraph Node Status Bot", description="This is a Node Bot", docs_url="/"
)


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
