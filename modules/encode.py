import re
from hashlib import sha256
import base58

def id_to_dag_address(node_data):
    # pkcs prefix + 04 required bytes
    pkcs_prefix = "3056301006072a8648ce3d020106052b8104000a034200" + "04"

    output_nodeid = f"{node_data['id'][0:8]}...{node_data['id'][-8:]}"

    if len(node_data["id"]) == 128:
        node_id = f"{pkcs_prefix}{node_data['id']}"
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
