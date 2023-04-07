import re
from hashlib import sha256
import base58
import nextcord


def id_to_dag_address(node_id: str):
    # pkcs prefix + 04 required bytes
    pkcs_prefix = "3056301006072a8648ce3d020106052b8104000a034200" + "04"

    output_nodeid = f"{node_id[0:8]}...{node_id[-8:]}"

    if len(node_id) == 128:
        node_id = f"{pkcs_prefix}{node_id}"
    else:
        node_id = None

    node_id = sha256(bytes.fromhex(node_id)).hexdigest()
    node_id = base58.b58encode(bytes.fromhex(node_id)).decode()
    node_id = node_id[len(node_id)-36:]

    check_digits = re.sub('[^0-9]+', '', node_id)
    check_digit = 0
    for n in check_digits:
        check_digit += int(n)
        if check_digit > 9:
            check_digit = check_digit % 9

    wallet_address = f"DAG{check_digit}{node_id}"
    print(wallet_address, output_nodeid)
    return wallet_address

async def embed(contact_data):
    title_layer = None
    title_state = None
    for node_data in contact_data:
        # TITLE CLUSTER
        if node_data["clusterNames"] is not None:
            title_cluster = node_data["clusterNames"]
        elif node_data["clusterNames"] is None and node_data["formerClusterNames"] is not None:
            title_cluster = node_data["formerClusterNames"]
        else:
            title_cluster = None
        # TITLE LAYER
        if node_data["layer"] == 0:
            if title_cluster is not None:
                title_layer = f"{title_cluster} validator layer"
            else:
                title_layer = f"Validator layer"
        elif node_data["layer"] == 1:
            if title_cluster is not None:
                title_layer = f"{title_cluster} metagraph layer"
            else:
                title_layer = f"Metagraph layer"
        # TITLE STATE
        if node_data["clusterConnectivity"] in ("new association", "associated"):
            title_state = "up"
        elif node_data["clusterConnectivity"] in ("new dissociation", "dissociated"):
            title_state = "down"
        elif node_data["clusterConnectivity"] is None:
            title_state = node_data["state"]
        # TITLE
        title = f"HGTP NODE SPIDR\n" \
                f"{title_layer} is {title_state}"
        print(title)
        # report = nextcord.Embed(title=title)
