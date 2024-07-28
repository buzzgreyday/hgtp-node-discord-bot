import asyncio
import os
import signal
import subprocess
import traceback
import sqlalchemy

from assets.src.database.models import SQLBase
from assets.src.database.crud import engine


async def create_db():
    async with engine.begin() as conn:
        # Reflect the metadata
        metadata = SQLBase.metadata
        await conn.run_sync(metadata.reflect)

        # Temporary
        await conn.execute(
            sqlalchemy.text(
                """
                ALTER TABLE users
                ADD COLUMN removal_datetime TIMESTAMP;
                """
            )
        )
        # Temporary
        await conn.execute(
            sqlalchemy.text(
                """
                ALTER TABLE users
                ADD COLUMN cluster VARCHAR;
                """
            )
        )

        print("Columns added successfully!")

        # Create all tables
        await conn.run_sync(metadata.create_all)
        await engine.dispose()

        print("Database tables and columns created or updated!")



print("starting process...")


def main():
    pid = None
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
        print(traceback.format_exc())
        if pid:
            os.kill(pid, signal.SIGTERM)
        exit(1)
    else:
        exit(0)


main()
