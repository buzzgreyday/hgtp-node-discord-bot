import asyncio
from typing import List

import pandas as pd

from assets.src import schemas, database, api, check
from assets.src.database import database, models

IP_REGEX = r'^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$'


async def process_node_data_per_user(
        session, name, ids, cluster_data, version_manager, _configuration
) -> List[schemas.Node]:
    futures = []
    data = []

    if ids is not None:
        for lst in ids:
            id_, ip, port = lst
            while True:
                subscriber = await api.locate_node(
                    session=session, node_id=id_, ip=ip, port=port
                )
                if subscriber:
                    break
            subscriber = pd.DataFrame(subscriber)
            futures.append(
                asyncio.create_task(
                    check.node_status(
                        session,
                        subscriber,
                        cluster_data,
                        version_manager,
                        _configuration,
                    )
                )
            )

        for async_process in futures:
            # If user is offline move back in queue
            d = await async_process

            if d.last_known_cluster_name == name:
                data.append(d)

        return data


async def write_db(data: List[schemas.User]):
    for d in data:
        await database.post_user(data=d)


async def delete_db(data: List[models.UserModel]):
    for d in data:
        await database.delete_user_entry(d)
