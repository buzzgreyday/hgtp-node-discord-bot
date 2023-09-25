from models import User, NodeData
from database import engine
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from sqlalchemy import select

session = async_sessionmaker(bind=engine, expire_on_commit=False)


class CRUD:
    async def get_next_index(self, async_session: async_sessionmaker[AsyncSession]) -> int:
        """Fetch the last assigned index from the separate table"""
        async with async_session() as session:
            statement = select()
        result = await db.execute(select(Model.index).order_by(Model.index.desc()).limit(1))
        await db.close()
        last_index = result.scalar_one_or_none()
        return 0 if last_index is None else last_index + 1