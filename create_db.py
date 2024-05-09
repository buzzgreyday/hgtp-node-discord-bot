import asyncio
import os
import signal
import subprocess

from assets.src.database.models import SQLBase
from assets.src.database.crud import engine


async def create_db():
    async with engine.begin() as conn:
        # Reflect the metadata
        metadata = SQLBase.metadata
        # await conn.run_sync(metadata.reflect)

        # Check if the table exists
        # if "reward_stats" in metadata.tables:
        #     # Drop the existing table
        #     await conn.run_sync(metadata.tables["reward_stats"].drop)

        # Create all tables
        await conn.run_sync(metadata.create_all)
        await engine.dispose()

    print("Database tables and columns created or updated!")



print("starting process...")


def main():
    try:
        uvi_process = subprocess.Popen(
            [
                "venv/bin/uvicorn",
                "assets.src.database.database:app",
                "--host",
                "127.0.0.1",
                "--port",
                "8000",
            ]
        )
        pid = uvi_process.pid
        asyncio.run(create_db())
        os.kill(pid, signal.SIGTERM)
    except Exception:
        exit(1)
    else:
        exit(0)


main()
