from sqlalchemy import Column, String, Integer, Float, DateTime, MetaData
from sqlalchemy.engine import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from fastapi import FastAPI, Depends
from assets.code.schemas import User
# from assets.code.schemas import SQLUserData

engine = create_engine("sqlite:///assets/data/users/db/user.db", echo=True)
SQLBase = declarative_base()

SQLBase.metadata.create_all(engine)
SessionLocal = sessionmaker(engine)

api = FastAPI()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@api.post("/user")
def index(user: User, db: Session = Depends(get_db)):
    db_user = User.Create(user)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


@api.get("/user")
def get_users(db: Session = Depends(get_db)):
    users = db.query(User).all()
    return {"users": users}
