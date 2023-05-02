import pandas as pd
import dask.dataframe as dd
async def history(dask_client, node_data, configuration):
    history_dataframe = dd.from_pandas(pd.DataFrame(node_data), npartitions=1)
    history_dataframe["publicPort"] = history_dataframe["publicPort"].astype(float)
    history_dataframe["formerTimestampIndex"] = history_dataframe["formerTimestampIndex"].astype(str)
    fut = history_dataframe.to_parquet(configuration["file settings"]["locations"]["history_new"], overwrite=False, compute=False, write_index=False)
    await dask_client.compute(fut)
