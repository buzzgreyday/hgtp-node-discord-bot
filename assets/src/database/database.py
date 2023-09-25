import os

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine
from fastapi import FastAPI

load_dotenv()

engine = create_async_engine(
    url=os.getenv("DB_URL"),
    echo=True
)

api = FastAPI(title="HGTP Node Status Bot", description="This is a Node Bot", docs_url="/")



'''async def get_next_index(Model) -> int:
    """Fetch the last assigned index from the separate table"""
    # async with db_lock:
    result = await db.execute(select(Model.index).order_by(Model.index.desc()).limit(1))
    await db.close()
    last_index = result.scalar_one_or_none()
    return 0 if last_index is None else last_index + 1


@api.post("/user/create")
async def post_user(data: UserModel):
    """Creates a new user subscription"""
    next_index = await get_next_index(User)
    data.index = next_index
    data.date = datetime.datetime.utcnow()
    data_dict = data.dict()
    user = User(**data_dict)
    # async with db_lock:
    result = await db.execute(select(User).where((User.ip == data.ip) & (User.public_port == data.public_port)))
    # You only need one result that matches
    result = result.fetchone()
    if result:
        logging.getLogger(__name__).info(f"database.py - The user {data.name} already exists for {data.ip}:{data.public_port}")
    else:
        db.add(user)
        while True:
            try:
                await db.commit()
            except (sqlalchemy.exc.OperationalError, sqlite3.OperationalError):
                logging.getLogger(__name__).error(
                    f"database.py - Operational error while posting user data {data.name} ({data.ip}:{data.public_port})")
                await asyncio.sleep(1)
            else:
                break
        await db.refresh(user)
        await db.close()
    return jsonable_encoder(data_dict)


@api.post("/data/create")
async def post_data(data: NodeModel, db: AsyncSession = Depends(get_db)):
    """Inserts node data from automatic check into database file"""
    next_index = await get_next_index(NodeData, db)
    data.index = next_index
    data_dict = data.dict()
    node_data = NodeData(**data_dict)
    # async with db_lock:
    db.add(node_data)
    while True:
        try:
            await db.commit()
        except (sqlalchemy.exc.OperationalError, sqlite3.OperationalError):
            logging.getLogger(__name__).info(f"database.py - Operational error while posting node data  {data.name} ({data.ip}:{data.public_port}, {data.last_known_cluster_name})")
            await asyncio.sleep(1)
        else:
            break
    await db.refresh(node_data)
    await db.close()
    return jsonable_encoder(data_dict)



@api.get("/user")
async def get_users(db: AsyncSession = Depends(get_db)):
    """Returns a list of all user data"""
    # async with db_lock:
    results = await db.execute(select(User))
    await db.close()
    users = results.scalars().all()
    return {"users": users}


@api.get("/user/ids/layer/{layer}")
async def get_user_ids(layer: int, db: AsyncSession = Depends(get_db)):
    """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Returns a list of all user IDs currently subscribed"""
    list_of_tuples = []
    # async with db_lock:
    results = await db.execute(select(User).where(User.layer == layer))
    await db.close()
    ids = results.scalars().all()
    for values in ids:
        list_of_tuples.append((values.id, values.ip, values.public_port))
    # return list(set(ids))
    return list_of_tuples


@api.get("/user/ids/{id_}/{ip}/{port}")
async def get_nodes(id_: str, ip: str, port: int, db: AsyncSession = Depends(get_db)):
    """Return user by ID"""
    # async with db_lock:
    results = await db.execute(select(User).where((User.id == id_) & (User.ip == ip) & (User.public_port == port)))
    await db.close()
    nodes = results.scalars().all()
    return nodes


@api.get("/user/node/{ip}/{public_port}")
async def get_node(ip: str, public_port: int, db: AsyncSession = Depends(get_db)):
    """Return user by IP and port"""
    # async with db_lock:
    results = await db.execute(select(User).where((User.ip == ip) & (User.public_port == public_port)))
    await db.close()
    node = results.scalars().all()
    return {"node": node}


@api.get("/user/ids/contact/{contact}/layer/{layer}")
async def get_contact_node_id(contact, layer, db: AsyncSession = Depends(get_db)):
    """INSTEAD RETURN A TUPLE CONTAINING ID, IP, PORT!!!! Return user by contact"""
    list_of_tuples = []
    # async with db_lock:
    results = await db.execute(select(User).where((User.contact == contact) & (User.layer == layer)))
    await db.close()
    ids = results.scalars().all()
    for values in ids:
        list_of_tuples.append((values.id, values.ip, values.public_port, values.layer))
    # return list(set(ids))
    return list_of_tuples


@api.get("/data/node/{ip}/{public_port}")
async def get_node_data(ip, public_port, db: AsyncSession = Depends(get_db)):
    """Return latest node data fetched via automatic check by IP and port"""
    # async with db_lock:
    results = await db.execute(
                    select(NodeData)
                    .where((NodeData.ip == ip) & (NodeData.public_port == public_port))
                    .order_by(NodeData.timestamp_index.desc())
                    .limit(1))
    await db.close()
    node = results.scalar_one_or_none()
    return node
'''