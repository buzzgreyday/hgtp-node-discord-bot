import os
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

"""GET VERSION"""
version_file = open(f"{__location__}/version", "r")
version = version_file.read()
version_file.close()

"""REQUEST TIMEOUT"""
aiohttp_timeout = 6

"""FILE LOCATIONS"""
subscriber_data = f'{__location__}/data/subscribers.csv'
archived_node_data = f'{__location__}/data/old/node data'
latest_node_data = f'{__location__}/data/node data'
load_balancers_data = f'{__location__}/data/load_balancers.csv'
logging_dir = f"{__location__}/data/logs"

"""URL ENDINGS"""
# NODE DATA URLs
cluster = f'cluster/info'
session = f'cluster/session'
# GET TESS VERSION (YOU CAN ALSO GET CLUSTER SESSION, NODE SESSION AND ID):
node = f'node/info'


