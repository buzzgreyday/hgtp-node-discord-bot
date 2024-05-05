import asyncio
import subprocess
import threading

from sqlalchemy import inspect

from assets.src.database.models import SQLBase
from assets.src.database.crud import engine


async def create_db():
    async with engine.begin() as conn:
        # Reflect the metadata
        metadata = SQLBase.metadata
        await conn.run_sync(metadata.reflect)

        # Check if the table exists
        if "reward_stats" in metadata.tables:
            # Drop the existing table
            await conn.run_sync(metadata.tables["reward_stats"].drop)

        # Create all tables
        await conn.run_sync(metadata.create_all)

    print("Database tables and columns created or updated!")
    await engine.dispose()


print("starting process...")


def run_server():
    subprocess.run(
        [
            "venv/bin/uvicorn",
            "assets.src.database.database:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8000",
        ]
    )


uvi = threading.Thread(target=run_server, daemon=True)
uvi.start()
asyncio.run(create_db())

exit(0)
