import asyncio
import signal
import traceback

import sqlalchemy
from hypercorn import Config
from hypercorn.asyncio import serve

from assets.src.database.models import SQLBase
from assets.src.database.crud import engine
from assets.src.database.database import app


async def creation(conn):
    # Temporary
    await conn.execute(
        sqlalchemy.text(
            """
            ALTER TABLE users
            ADD COLUMN subscription_id VARCHAR;
            """))

    await conn.execute(
        sqlalchemy.text(
            """
            ALTER TABLE users
            ADD COLUMN subscription_created TIMESTAMP;
            """))
    await conn.execute(
        sqlalchemy.text(
            """
            ALTER TABLE users
            ADD COLUMN current_subscription_period_start TIMESTAMP;
            """))
    await conn.execute(
        sqlalchemy.text(
            """
            ALTER TABLE users
            ADD COLUMN current_subscription_period_end TIMESTAMP;
            """))
    await conn.execute(
        sqlalchemy.text(
            """
            ALTER TABLE users
            ADD COLUMN discord_dm_allowed BOOLEAN DEFAULT TRUE;
            """
        )
    )

    # Temporary
    # await conn.execute(
    #     sqlalchemy.text(
    #         """
    #         ALTER TABLE users
    #         ADD COLUMN cluster VARCHAR;
    #         """
    #     )
    # )


async def create_db():
    while True:
        if hypercorn_running:
            async with engine.begin() as conn:
                # Reflect the metadata
                metadata = SQLBase.metadata
                await conn.run_sync(metadata.reflect)

                print("CREATING DATABASE STUFF...")
                try:
                    await creation(conn)
                except Exception as e:
                    print(f"CREATION ERROR: {e}")
                    exit(1)

                # Create all tables
                await conn.run_sync(metadata.create_all)
                print("DATABASE OK!")

                await engine.dispose()
                break
        else:
            await asyncio.sleep(1)
    exit(0)


print("STARTING DATABASE MANIPULATION...")

hypercorn_running = False


async def run_hypercorn_process(app):
    global hypercorn_running
    hypercorn_running = True
    config = Config()
    config.bind = ["localhost:8000"]
    try:
        await serve(app, config)
    finally:
        hypercorn_running = False


async def start(app):
    # Run both Hypercorn and create_db() concurrently
    await asyncio.gather(
        run_hypercorn_process(app),
        create_db()
    )


def main():
    loop = asyncio.get_event_loop()

    # Add signal handlers for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, loop.stop)

    # Run the app (starts Hypercorn and database updates)
    loop.run_until_complete(start(app))
    # Ensure proper cleanup
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()


if __name__ == "__main__":
    main()