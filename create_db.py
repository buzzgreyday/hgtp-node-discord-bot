import asyncio
import traceback

from hypercorn import Config
from hypercorn.asyncio import serve

from assets.src.database.models import SQLBase
from assets.src.database.crud import engine
from assets.src.database.database import app


async def create_db():
    async with engine.begin() as conn:
        # Reflect the metadata
        metadata = SQLBase.metadata
        await conn.run_sync(metadata.reflect)

        # Temporary
        # await conn.execute(
        #     sqlalchemy.text(
        #         """
        #         ALTER TABLE users
        #         ADD COLUMN removal_datetime TIMESTAMP;
        #         """
        #     )
        # )
        # Temporary
        # await conn.execute(
        #     sqlalchemy.text(
        #         """
        #         ALTER TABLE users
        #         ADD COLUMN cluster VARCHAR;
        #         """
        #     )
        # )

        print("Tables added successfully!")

        # Create all tables
        await conn.run_sync(metadata.create_all)
        await engine.dispose()

        print("Database tables and columns created or updated!")


print("starting process...")

hypercorn_running = False
async def start(app):
    config = Config()
    config.bind = ["localhost:8000"]
    await serve(app, config)
    await create_db()


def main():
    try:
        asyncio.run(start(app))
    except Exception:
        print(traceback.format_exc())
        exit(1)
    else:
        exit(0)


main()
