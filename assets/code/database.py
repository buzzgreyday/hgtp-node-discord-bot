import datetime

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select
from fastapi import FastAPI, Depends
from assets.code.schemas import User as UserModel

engine = create_async_engine("sqlite+aiosqlite:///assets/data/users/db/user.db", connect_args={"check_same_thread": False})

SessionLocal = async_sessionmaker(engine)

api = FastAPI()


class SQLBase(DeclarativeBase):
    pass


class User(SQLBase):
    __tablename__ = "users"

    name: Mapped[str]
    id: Mapped[str]
    ip: Mapped[str]
    public_port: Mapped[int]
    layer: Mapped[int]
    contact: Mapped[str]
    date: Mapped[datetime.datetime] = mapped_column(primary_key=True)
    type: Mapped[str]


async def get_db():
    async with engine.begin() as conn:
        await conn.run_sync(SQLBase.metadata.create_all)
    db = SessionLocal()
    try:
        yield db
    finally:
        await db.close()


@api.post("/user")
async def index(data: UserModel, db: AsyncSession = Depends(get_db)):
    db_user = User(
                   name=data.name,
                   id=data.id,
                   ip=data.ip,
                   public_port=data.public_port,
                   layer=data.layer,
                   contact=data.contact,
                   date=data.date,
                   type=data.type
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user


@api.get("/user")
async def get_users(db: AsyncSession = Depends(get_db)):
    results = await db.execute(select(User))
    users = results.scalars().all()
    return {"users": users}
