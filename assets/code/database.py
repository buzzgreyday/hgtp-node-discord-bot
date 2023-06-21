from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, DateTime
from fastapi import FastAPI, Depends
from assets.code.schemas import User as UserModel
# from assets.code.schemas import SQLUserData

engine = create_async_engine("sqlite+aiosqlite:///assets/data/users/db/user.db", connect_args={"check_same_thread": False})
SQLBase = declarative_base()

SessionLocal = async_sessionmaker(engine)

api = FastAPI()


class User(SQLBase):
    __tablename__ = "users"

    name = Column(String)
    id = Column(String)
    ip = Column(String)
    public_port = Column(Integer)
    layer = Column(Integer)
    contact = Column(String)
    date = Column(DateTime)
    type = Column(String)


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
    users = db.query(User).all()
    return {"users": users}
