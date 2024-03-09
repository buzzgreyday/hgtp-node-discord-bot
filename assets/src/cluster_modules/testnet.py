#######################################################################################################################
#                       |    -**  MAINNET HGTP NODE SPIDER BOT MODULE, VERSION 1.0  **-    |
# --------------------------------------------------------------------------------------------------------------------
#  + DESCRIPTION
#   THIS MODULE CONTAINS PROJECT- OR BUSINESS-SPECIFIC CODE WHICH ENABLES SUPPORT FOR THIS PARTICULAR CLUSTER'S API.
# --------------------------------------------------------------------------------------------------------------------
#######################################################################################################################
# * IMPORTS: MODULES, CONSTANTS AND VARIABLES
# ---------------------------------------------------------------------------------------------------------------------


from assets.src import schemas
from assets.src.cluster_modules import constellation

"""
    SECTION 1: GENERAL PRELIMINARIES
"""
# ---------------------------------------------------------------------------------------------------------------------
# + CLUSTER SPECIFIC FUNCTIONS AND CLASSES GOES HERE
# ---------------------------------------------------------------------------------------------------------------------
#   THE FUNCTION BELOW IS ONE OF THE FIRST INITIATIONS. THIS FUNCTION REQUESTS DATA FROM THE MAINNET/TESTNET CLUSTER.
#   IN THIS MODULE WE REQUEST THINGS LIKE STATE, LOAD BALANCER ID, PEERS AND THE LATEST CLUSTER SESSION TOKEN.
#   WE THEN AGGREAGATE ALL THIS DATA IN A "CLUSTER DICTIONARY" AND ADDS IT TO A LIST OF ALL THE SUPPORTED CLUSTERS.
#   WE ALSO CHECK FOR REWARDS.
# ---------------------------------------------------------------------------------------------------------------------


async def request_cluster_data(session, url, layer, name, configuration) -> schemas.Cluster:
    cluster_data = await constellation.request_cluster_data(
        session, url, layer, name, configuration
    )
    return cluster_data


# THE ABOVE FUNCTION ALSO REQUEST THE MOST RECENT REWARDED ADDRESSES. THIS FUNCTION LOCATES THESE ADDRESSES BY
# REQUESTING THE RELEVANT API'S.

# (!) YOU COULD MAKE 50 (MAGIC NUMBER) VARIABLE IN THE CONFIG YAML.
#     YOU MIGHT ALSO BE ABLE TO IMPROVE ON THE TRY/EXCEPT BLOCK LENGTH.


"""
    SECTION 2: NODE DATA
"""
# ---------------------------------------------------------------------------------------------------------------------
# + NODE SPECIFIC FUNCTIONS AND CLASSES GOES HERE
# ---------------------------------------------------------------------------------------------------------------------


async def node_cluster_data(
    session, node_data: schemas.Node, module_name, configuration: dict
) -> schemas.Node:
    """Get node data. IMPORTANT: Create Pydantic Schema for node data"""
    node_data = await constellation.node_cluster_data(
        session, node_data, module_name, configuration
    )
    return node_data


def check_rewards(node_data: schemas.Node, cluster_data):
    node_data = constellation.check_rewards(node_data, cluster_data)
    return node_data


"""
    SECTION 3: CREATE REPORT
"""


def build_embed(node_data: schemas.Node, module_name):
    embed = constellation.build_embed(node_data, module_name)
    return embed


"""
    SECTION 4: NOTIFICATION CONDITIONS
"""


def mark_notify(d: schemas.Node, configuration):
    d = constellation.mark_notify(d, configuration)
    return d
